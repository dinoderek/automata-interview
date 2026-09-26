package main

// Retrying failures the driver reports as retryable, bounded by maxAttempts.
//
// Runs against Postgres like the concurrency test:  docker compose run --rm tests

import "testing"

func (h *runHarness) timesSent(name string) int {
	n := 0
	for _, s := range h.sentNames() {
		if s == name {
			n++
		}
	}
	return n
}

// A retryable failure puts the step straight back on its device -- the driver
// freed itself before reporting -- and the run carries on as if nothing
// happened. The attempt count shows it took two.
func TestRetryableFailureIsRetried(t *testing.T) {
	h := newRunHarness(t, nil)
	h.finish("fill_sample_plate", "")
	h.finishRetryable("incubate_samples", "lid jammed")

	h.expectStep("incubate_samples", StepRunning, "lid jammed (attempt 1 of 3, retrying)")
	h.expectRun(RunRunning, "", "")
	if n := h.timesSent("incubate_samples"); n != 2 {
		t.Fatalf("incubate_samples sent %d times, want 2", n)
	}

	h.finish("incubate_samples", "")
	h.expectStep("incubate_samples", StepCompleted, "")
	if st := h.step("incubate_samples"); st.DispatchCount != 2 {
		t.Fatalf("dispatch_count = %d, want 2", st.DispatchCount)
	}
	h.expectRun(RunRunning, "", "")
}

// A retried step keeps its place in line: fill_reagent_plate fails and wins
// the liquid handler back from fill_buffer_plate, which has a shorter chain
// behind it.
func TestRetryKeepsItsPriority(t *testing.T) {
	h := newRunHarness(t, nil)
	h.finish("fill_sample_plate", "")
	h.finishRetryable("fill_reagent_plate", "dropped a tip")

	h.expectSent("fill_sample_plate", "fill_reagent_plate", "incubate_samples", "fill_reagent_plate")
	h.expectStep("fill_buffer_plate", StepPending, "")
}

// Retries are bounded: after maxAttempts the step fails, saying so, and the
// run fails with it -- draining whatever else is on an instrument.
func TestRetryGivesUpAfterMaxAttempts(t *testing.T) {
	h := newRunHarness(t, nil)
	h.finish("fill_sample_plate", "")
	h.finishRetryable("incubate_samples", "lid jammed")
	h.finishRetryable("incubate_samples", "lid jammed")
	h.finishRetryable("incubate_samples", "lid jammed")

	reason := "lid jammed (attempt 3 of 3, giving up)"
	h.expectStep("incubate_samples", StepFailed, reason)
	h.expectRun(RunFailedDraining, "incubate_samples", reason)
	if n := h.timesSent("incubate_samples"); n != maxAttempts {
		t.Fatalf("incubate_samples sent %d times, want %d", n, maxAttempts)
	}

	h.finish("fill_reagent_plate", "")
	h.expectRun(RunFailed, "incubate_samples", reason)
}

// Once the run is failing, a retryable failure is not retried: nothing new
// goes out on a run that has already failed.
func TestNoRetryWhileDraining(t *testing.T) {
	h := newRunHarness(t, nil)
	driveToIncubatorFailure(h)

	h.finishRetryable("fill_buffer_plate", "tip crash")
	h.expectStep("fill_buffer_plate", StepFailed,
		"tip crash (retryable, not retried: run is failed_draining)")
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")
	if n := h.timesSent("fill_buffer_plate"); n != 1 {
		t.Fatalf("fill_buffer_plate sent %d times, want 1", n)
	}
}

// A retry that is refused fails like any refused step.
func TestRetryRefusedFailsRun(t *testing.T) {
	h := newRunHarness(t, map[string][]reply{"fill_sample_plate": {accept, refuse}})
	h.finishRetryable("fill_sample_plate", "pipette clogged")

	h.expectStep("fill_sample_plate", StepFailed, refusedLH)
	h.expectRun(RunFailed, "fill_sample_plate", refusedLH)
}
