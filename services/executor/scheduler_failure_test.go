package main

// Failure handling, driven step by step: a step fails while another is still
// on an instrument, and the run must drain before it ends.
//
// Runs against Postgres like the concurrency test:  docker compose run --rm tests

import "testing"

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
	// incubator is free) but the run had failed.
	h.expectSent(sentAtFailure...)
	h.expectStep("warm_reagent_plate", StepPending, "")
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
