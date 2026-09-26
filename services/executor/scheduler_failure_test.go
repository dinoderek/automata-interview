package main

// Failure handling, driven step by step: a step fails while another is still
// on an instrument, and the run must drain before it ends.
//
// Runs against Postgres like the concurrency test:  docker compose run --rm tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

// driveToIncubatorFailure plays the run up to the moment the incubator fails
// while the liquid handler is busy with fill_buffer_plate.
func driveToIncubatorFailure(h *runHarness) {
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
	h.expectStep("warm_reagent_plate", StepSkipped, "") // at once, not after draining
	h.expectStep("incubate_samples", StepFailed, "incubator on fire")
	h.expectStep("fill_buffer_plate", StepRunning, "")
}

// A step fails while another is on an instrument: the run drains, dispatches
// nothing new, and ends failed once the in-flight step reports.
func TestFailureDrainsInFlightSteps(t *testing.T) {
	h := newRunHarness(t, nil)
	driveToIncubatorFailure(h)
	sentAtFailure := h.sentNames()

	h.finish("fill_buffer_plate", "")
	h.expectStep("fill_buffer_plate", StepCompleted, "")
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")

	// warm_reagent_plate became runnable (its dependency completed and the
	// incubator is free) but the run had failed: skipped, like the rest.
	h.expectSent(sentAtFailure...)
	for _, name := range []string{"warm_reagent_plate", "combine", "read_plate"} {
		h.expectStep(name, StepSkipped, "")
	}
}

// A second failure while draining is recorded on its step, but the run keeps
// the first failure as the reason it stopped.
func TestFailureWhileDrainingKeepsFirstFailure(t *testing.T) {
	h := newRunHarness(t, nil)
	driveToIncubatorFailure(h)

	h.finish("fill_buffer_plate", "tip crash")
	h.expectStep("fill_buffer_plate", StepFailed, "tip crash")
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")
}

// With nothing else on an instrument, a failure ends the run immediately.
func TestFailureWithNothingInFlightFailsImmediately(t *testing.T) {
	h := newRunHarness(t, nil)
	h.finish("fill_sample_plate", "no plate loaded")
	h.expectRun(RunFailed, "fill_sample_plate", "no plate loaded")
	h.expectSent("fill_sample_plate")
}

// A result delivered again after the run has ended changes nothing: the step
// is no longer running, so the result is ignored.
func TestDuplicateResultAfterRunEndsIsIgnored(t *testing.T) {
	h := newRunHarness(t, nil)
	driveToIncubatorFailure(h)
	h.finish("fill_buffer_plate", "")
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")

	h.finish("fill_buffer_plate", "")
	h.expectStep("fill_buffer_plate", StepCompleted, "")
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")
}

// When the scheduler's own storage fails, the run is abandoned: failed, no
// step to blame, and the error names its type. A cancelled context makes the
// store fail for real; abandon still records the failure.
func TestDatabaseErrorAbandonsRunWithTypedReason(t *testing.T) {
	h := newRunHarness(t, nil)
	c := h.bus.sent()[0]

	cancelled, cancel := context.WithCancel(h.ctx)
	cancel()
	h.sched.HandleResult(cancelled, StepResult{RunID: c.RunID, StepID: c.StepID,
		StepName: c.StepName, DeviceID: c.DeviceID})

	h.expectRun(RunFailed, "",
		"database error: recording result for fill_sample_plate: recording step completed: context canceled")
	h.expectStep("fill_sample_plate", StepRunning, "") // the result never made it in
}

// If the failure cannot be recorded either, the executor stops: its table may
// no longer match the instruments, and scheduling from it could send a step
// twice. Here the scheduler's database is closed, so every write fails.
func TestAbandonExitsWhenFailureCannotBeRecorded(t *testing.T) {
	h := newRunHarness(t, nil)
	c := h.bus.sent()[0]

	closed, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	closed.Close()
	sched := NewScheduler(NewStore(closed), h.bus)
	var exited string
	sched.fatal = func(format string, args ...any) { exited = fmt.Sprintf(format, args...) }

	sched.HandleResult(h.ctx, StepResult{RunID: c.RunID, StepID: c.StepID,
		StepName: c.StepName, DeviceID: c.DeviceID})

	if !strings.Contains(exited, "cannot record failure of run "+h.runID) ||
		!strings.Contains(exited, "database error: recording result for fill_sample_plate") {
		t.Fatalf("did not exit as expected; fatal message: %q", exited)
	}
	h.expectRun(RunRunning, "", "") // nothing could be written
}

// A scheduler error while a run drains still ends it. Before, abandon only
// failed a running run: on a draining run it changed nothing, reported no
// error, and the run stayed failed_draining -- blocking every later start.
func TestAbandonEndsDrainingRunKeepingFirstFailure(t *testing.T) {
	h := newRunHarness(t, nil)
	driveToIncubatorFailure(h)

	h.sched.abandon(h.ctx, h.runID, errTypeDatabase, errors.New("connection reset"))
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")
}

// At startup, a run left active by a previous executor is failed: outcome
// unknown. What is still on an instrument drains as usual.
func TestFailActiveRunsAtStartup(t *testing.T) {
	h := newRunHarness(t, nil) // fill_sample_plate on the liquid handler
	fresh := NewScheduler(h.store, h.bus)
	if err := fresh.failActiveRuns(h.ctx, []string{h.runID}); err != nil {
		t.Fatalf("failActiveRuns: %v", err)
	}

	reason := "executor restarted: run was active when the executor started; outcome unknown"
	h.expectRun(RunFailedDraining, "", reason)
	h.expectStep("incubate_samples", StepSkipped, "")

	h.finish("fill_sample_plate", "")
	h.expectRun(RunFailed, "", reason)
}

// A run stuck draining with nothing left on an instrument -- nothing would
// ever finish it -- is finished at startup, keeping its first failure.
func TestFailActiveRunsFinishesStuckDrainingRun(t *testing.T) {
	h := newRunHarness(t, nil)
	driveToIncubatorFailure(h)
	// fill_buffer_plate's result recorded, but the run never moved on.
	c := h.bus.sent()[3]
	if _, err := h.store.RecordStepFinished(h.ctx, h.runID, c.StepID, StepCompleted, ""); err != nil {
		t.Fatalf("record: %v", err)
	}
	h.expectRun(RunFailedDraining, "incubate_samples", "incubator on fire")

	if err := NewScheduler(h.store, h.bus).failActiveRuns(h.ctx, []string{h.runID}); err != nil {
		t.Fatalf("failActiveRuns: %v", err)
	}
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")
}
