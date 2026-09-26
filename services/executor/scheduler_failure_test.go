package main

// Failure handling, driven step by step: a step fails while another is still
// on an instrument, and the run must drain before it ends.
//
// Runs against Postgres like the concurrency test:  docker compose run --rm tests

import (
	"context"
	"database/sql"
	"os"
	"slices"
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

// failureHarness is one run of pcrTemplate with a bus that accepts everything.
// The test plays the drivers by calling finish.
type failureHarness struct {
	t     *testing.T
	ctx   context.Context
	store *Store
	bus   *recordingBus
	sched *Scheduler
	runID string
}

func newFailureHarness(t *testing.T) *failureHarness {
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
	run, err := store.CreateRun(ctx, "failure test", pcrTemplate)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	bus := &recordingBus{}
	h := &failureHarness{t: t, ctx: ctx, store: store, bus: bus,
		sched: NewScheduler(store, bus), runID: run.ID}
	if err := h.sched.Start(ctx, run.ID); err != nil {
		t.Fatalf("start run: %v", err)
	}
	return h
}

// finish reports the named step finished, with errMsg set if it failed. The
// step must have been dispatched.
func (h *failureHarness) finish(name, errMsg string) {
	h.t.Helper()
	for _, c := range h.bus.sent() {
		if c.StepName == name {
			h.sched.HandleResult(h.ctx, StepResult{RunID: c.RunID, StepID: c.StepID,
				StepName: c.StepName, DeviceID: c.DeviceID, Error: errMsg, Retryable: errMsg != ""})
			return
		}
	}
	h.t.Fatalf("finish %s: it was never dispatched (sent: %v)", name, h.sentNames())
}

func (h *failureHarness) sentNames() []string {
	var names []string
	for _, c := range h.bus.sent() {
		names = append(names, c.StepName)
	}
	return names
}

func (h *failureHarness) expectSent(want ...string) {
	h.t.Helper()
	if got := h.sentNames(); !slices.Equal(got, want) {
		h.t.Fatalf("dispatched %v, want %v", got, want)
	}
}

func (h *failureHarness) run() *Run {
	h.t.Helper()
	r, err := h.store.GetRun(h.ctx, h.runID)
	if err != nil {
		h.t.Fatalf("get run: %v", err)
	}
	return r
}

func (h *failureHarness) expectRun(status, failedStep, errMsg string) {
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

func (h *failureHarness) expectStep(name, status, errMsg string) {
	h.t.Helper()
	steps, err := h.store.ListSteps(h.ctx, h.runID)
	if err != nil {
		h.t.Fatalf("list steps: %v", err)
	}
	for _, st := range steps {
		if st.Name == name {
			if st.Status != status || deref(st.Error) != errMsg {
				h.t.Fatalf("step %s is %s (error=%q), want %s (error=%q)",
					name, st.Status, deref(st.Error), status, errMsg)
			}
			return
		}
	}
	h.t.Fatalf("no step %s", name)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// driveToIncubatorFailure plays the run up to the moment the incubator fails
// while the liquid handler is busy with fill_buffer_plate.
func driveToIncubatorFailure(h *failureHarness) {
	h.t.Helper()
	h.expectSent("fill_sample_plate")
	h.finish("fill_sample_plate", "")
	h.expectSent("fill_sample_plate", "fill_reagent_plate", "incubate_samples")
	h.finish("fill_reagent_plate", "")
	// The liquid handler is free again; the incubator is still busy, so
	// warm_reagent_plate waits.
	h.expectSent("fill_sample_plate", "fill_reagent_plate", "incubate_samples", "fill_buffer_plate")

	h.finish("incubate_samples", "incubator on fire")
	h.expectRun(RunFailedDraining, "incubate_samples", "incubator on fire")
	h.expectStep("incubate_samples", StepFailed, "incubator on fire")
	h.expectStep("fill_buffer_plate", StepRunning, "")
}

// A step fails while another is on an instrument: the run drains, dispatches
// nothing new, and ends failed once the in-flight step reports.
func TestFailureDrainsInFlightSteps(t *testing.T) {
	h := newFailureHarness(t)
	driveToIncubatorFailure(h)
	sentAtFailure := h.sentNames()

	h.finish("fill_buffer_plate", "")
	h.expectStep("fill_buffer_plate", StepCompleted, "")
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")

	// warm_reagent_plate became runnable (its dependency completed and the
	// incubator is free) but the run had failed.
	h.expectSent(sentAtFailure...)
	h.expectStep("warm_reagent_plate", StepPending, "")
}

// A second failure while draining is recorded on its step, but the run keeps
// the first failure as the reason it stopped.
func TestFailureWhileDrainingKeepsFirstFailure(t *testing.T) {
	h := newFailureHarness(t)
	driveToIncubatorFailure(h)

	h.finish("fill_buffer_plate", "tip crash")
	h.expectStep("fill_buffer_plate", StepFailed, "tip crash")
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")
}

// With nothing else on an instrument, a failure ends the run immediately.
func TestFailureWithNothingInFlightFailsImmediately(t *testing.T) {
	h := newFailureHarness(t)
	h.finish("fill_sample_plate", "no plate loaded")
	h.expectRun(RunFailed, "fill_sample_plate", "no plate loaded")
	h.expectSent("fill_sample_plate")
}

// A result delivered again after the run has ended changes nothing: the step
// is no longer running, so the result is ignored.
func TestDuplicateResultAfterRunEndsIsIgnored(t *testing.T) {
	h := newFailureHarness(t)
	driveToIncubatorFailure(h)
	h.finish("fill_buffer_plate", "")
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")

	h.finish("fill_buffer_plate", "")
	h.expectStep("fill_buffer_plate", StepCompleted, "")
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")
}
