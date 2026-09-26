package main

// A harness for driving one run step by step against Postgres, playing the
// drivers' side of both commands and results.

import (
	"context"
	"database/sql"
	"os"
	"slices"
	"sync"
	"testing"
)

// pcrTemplate is the default workflow's shape, fixed here so these tests do not
// move when workflows.yaml is edited.
var pcrTemplate = []stepTemplate{
	{Name: "fill_sample_plate", DeviceID: "liquid-handler-1"},
	{Name: "incubate_samples", DeviceID: "incubator-1", DependsOn: []string{"fill_sample_plate"}},
	{Name: "fill_reagent_plate", DeviceID: "liquid-handler-1", DependsOn: []string{"fill_sample_plate"}},
	{Name: "fill_buffer_plate", DeviceID: "liquid-handler-1", DependsOn: []string{"fill_sample_plate"}},
	{Name: "warm_reagent_plate", DeviceID: "incubator-1", DependsOn: []string{"fill_reagent_plate"}},
	{Name: "combine", DeviceID: "liquid-handler-1",
		DependsOn: []string{"incubate_samples", "warm_reagent_plate", "fill_buffer_plate"}},
	{Name: "read_plate", DeviceID: "plate-reader-1", DependsOn: []string{"combine"}},
}

// runHarness is one run of pcrTemplate. The test plays the drivers: bus
// answers commands (accepting everything unless scripted otherwise), and
// finish reports results.
type runHarness struct {
	t     *testing.T
	ctx   context.Context
	store *Store
	bus   *scriptedBus
	sched *Scheduler
	runID string
}

func newRunHarness(t *testing.T, script map[string][]reply) *runHarness {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL not set -- run with: docker compose run --rm tests")
	}
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()
	store := NewStore(db)
	run, err := store.CreateRun(ctx, t.Name(), pcrTemplate)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	bus := &scriptedBus{script: script}
	h := &runHarness{t: t, ctx: ctx, store: store, bus: bus,
		sched: NewScheduler(store, bus), runID: run.ID}
	if err := h.sched.Start(ctx, run.ID); err != nil {
		t.Fatalf("start run: %v", err)
	}
	return h
}

// finish reports the named step finished, with errMsg set if it failed. A
// failure is reported as not retryable, like the liquid handler's. The step
// must have been accepted by a driver.
func (h *runHarness) finish(name, errMsg string) {
	h.t.Helper()
	h.report(name, errMsg, false)
}

// finishRetryable reports the named step failed, and that the driver considers
// it safe to run again, like the incubator's and plate reader's failures.
func (h *runHarness) finishRetryable(name, errMsg string) {
	h.t.Helper()
	h.report(name, errMsg, true)
}

func (h *runHarness) report(name, errMsg string, retryable bool) {
	h.t.Helper()
	for _, c := range h.bus.sent() {
		if c.StepName == name {
			h.sched.HandleResult(h.ctx, StepResult{RunID: c.RunID, StepID: c.StepID,
				StepName: c.StepName, DeviceID: c.DeviceID, Error: errMsg, Retryable: retryable})
			return
		}
	}
	h.t.Fatalf("report %s: it was never dispatched (sent: %v)", name, h.sentNames())
}

func (h *runHarness) sentNames() []string {
	var names []string
	for _, c := range h.bus.sent() {
		names = append(names, c.StepName)
	}
	return names
}

func (h *runHarness) expectSent(want ...string) {
	h.t.Helper()
	if got := h.sentNames(); !slices.Equal(got, want) {
		h.t.Fatalf("dispatched %v, want %v", got, want)
	}
}

func (h *runHarness) run() *Run {
	h.t.Helper()
	r, err := h.store.GetRun(h.ctx, h.runID)
	if err != nil {
		h.t.Fatalf("get run: %v", err)
	}
	return r
}

func (h *runHarness) expectRun(status, failedStep, errMsg string) {
	h.t.Helper()
	r := h.run()
	if r.Status != status || deref(r.FailedStep) != failedStep || deref(r.Error) != errMsg {
		h.t.Fatalf("run is %s (failed_step=%q error=%q), want %s (failed_step=%q error=%q)",
			r.Status, deref(r.FailedStep), deref(r.Error), status, failedStep, errMsg)
	}
	if ended := r.FinishedAt != nil; ended != (status == RunFailed) {
		h.t.Fatalf("run %s has finished_at=%v", status, r.FinishedAt)
	}
}

func (h *runHarness) step(name string) Step {
	h.t.Helper()
	steps, err := h.store.ListSteps(h.ctx, h.runID)
	if err != nil {
		h.t.Fatalf("list steps: %v", err)
	}
	for _, st := range steps {
		if st.Name == name {
			return st
		}
	}
	h.t.Fatalf("no step %s", name)
	return Step{}
}

func (h *runHarness) expectStep(name, status, errMsg string) {
	h.t.Helper()
	st := h.step(name)
	if st.Status != status || deref(st.Error) != errMsg {
		h.t.Fatalf("step %s is %s (error=%q), want %s (error=%q)",
			name, st.Status, deref(st.Error), status, errMsg)
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// reply is one scripted answer to a command.
type reply struct {
	ack CommandAck
	err error
}

var (
	refuse = reply{ack: CommandAck{Accepted: false, Reason: "busy with something else"}}
	accept = reply{ack: CommandAck{Accepted: true}}
)

// scriptedBus answers commands like a driver would. It accepts everything,
// unless script holds answers queued for a step's next attempts, by name.
type scriptedBus struct {
	mu       sync.Mutex
	script   map[string][]reply
	attempts []string      // every command offered, by step name
	accepted []StepCommand // commands a driver took
}

func (b *scriptedBus) SendCommand(ctx context.Context, cmd StepCommand) (CommandAck, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.attempts = append(b.attempts, cmd.StepName)
	r := accept
	if q := b.script[cmd.StepName]; len(q) > 0 {
		r, b.script[cmd.StepName] = q[0], q[1:]
	}
	if r.err == nil && r.ack.Accepted {
		b.accepted = append(b.accepted, cmd)
	}
	return r.ack, r.err
}

func (b *scriptedBus) OnStepResult(func(context.Context, StepResult)) error { return nil }

func (b *scriptedBus) DriverState(ctx context.Context, deviceID string) (DriverState, error) {
	return DriverState{DeviceID: deviceID}, nil
}

func (b *scriptedBus) Close() {}

func (b *scriptedBus) sent() []StepCommand {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.accepted)
}

func (b *scriptedBus) tried() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.attempts)
}
