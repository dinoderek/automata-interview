package main

// A harness for driving one run step by step against Postgres, playing the
// drivers' side of both commands and results.

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
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
	clock time.Time // the scheduler's now
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
	runID := createTestRun(t, store)
	bus := &scriptedBus{script: script}
	h := &runHarness{t: t, ctx: ctx, store: store, bus: bus,
		sched: NewScheduler(store, bus), runID: runID,
		clock: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	h.sched.now = func() time.Time { return h.clock }
	if err := h.sched.Start(ctx, runID); err != nil {
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
	states   map[string]driverAnswer
}

// driverAnswer is what DriverState returns for a device.
type driverAnswer struct {
	state DriverState
	err   error
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

// DriverState answers as scripted by setState. An unscripted device is idle
// with nothing executed.
func (b *scriptedBus) DriverState(ctx context.Context, deviceID string) (DriverState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if a, ok := b.states[deviceID]; ok {
		return a.state, a.err
	}
	return DriverState{DeviceID: deviceID}, nil
}

func (b *scriptedBus) setState(deviceID string, a driverAnswer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.states == nil {
		b.states = make(map[string]driverAnswer)
	}
	b.states[deviceID] = a
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

// executedOn is what a real driver reports as Executed: the ID of every
// command it accepted.
func (h *runHarness) executedOn(device string) []string {
	var ids []string
	for _, c := range h.bus.sent() {
		if c.DeviceID == device {
			ids = append(ids, c.StepID)
		}
	}
	return ids
}

// driverBusy makes device report it is working on stepName.
func (h *runHarness) driverBusy(device, stepName string) {
	h.bus.setState(device, driverAnswer{state: DriverState{DeviceID: device,
		Busy: true, CurrentStep: stepName, Executed: h.executedOn(device)}})
}

// driverIdle makes device report it is idle, having executed everything it
// accepted -- what a driver looks like after dropping a result.
func (h *runHarness) driverIdle(device string) {
	h.bus.setState(device, driverAnswer{state: DriverState{DeviceID: device,
		Executed: h.executedOn(device)}})
}

func (h *runHarness) tick(d time.Duration) { h.clock = h.clock.Add(d) }

// reconcile runs one reconcile pass over this run's in-flight steps only: the
// database is shared with other tests' runs, and with a live executor.
func (h *runHarness) reconcile() {
	h.t.Helper()
	h.sched.reconcileSteps(h.ctx, h.inFlight())
}

// inFlight is this run's share of what the reconcile loop would read.
func (h *runHarness) inFlight() []Step {
	h.t.Helper()
	all, err := h.store.ListInFlightSteps(h.ctx)
	if err != nil {
		h.t.Fatalf("list in-flight steps: %v", err)
	}
	var mine []Step
	for _, st := range all {
		if st.RunID == h.runID {
			mine = append(mine, st)
		}
	}
	return mine
}

var errUnreachable = errors.New("state of device: nats: timeout")

// createTestRun creates a pending run of pcrTemplate. Tests leave runs active
// on purpose (a step left running, a run left draining); with one run at a
// time that would block the next test, so the run is aborted at cleanup.
func createTestRun(t *testing.T, store *Store) string {
	t.Helper()
	ctx := context.Background()
	run, err := store.CreateRun(ctx, t.Name(), pcrTemplate)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	t.Cleanup(func() {
		if _, err := store.db.ExecContext(ctx,
			`UPDATE runs SET status = $1, finished_at = now() WHERE id = $2 AND status IN ($3, $4)`,
			RunAborted, run.ID, RunRunning, RunFailedDraining); err != nil {
			t.Errorf("abort run %s: %v", run.ID, err)
		}
	})
	return run.ID
}
