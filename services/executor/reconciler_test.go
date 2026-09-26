package main

// Lost results: the reconcile loop, driven by hand with a fake clock and
// scripted driver states.
//
// Runs against Postgres like the concurrency test:  docker compose run --rm tests

import "testing"

const lostIncubation = "result lost: incubator-1 finished incubate_samples (attempt 1) but never reported; outcome unknown"

// The incubator finishes incubate_samples and the result is dropped. The first
// pass only suspects it; once lostGrace has passed, the step fails as outcome
// unknown and the run fails -- draining the liquid handler's step first.
func TestDroppedResultFailsStepAfterGrace(t *testing.T) {
	h := newRunHarness(t, nil)
	h.finish("fill_sample_plate", "")
	h.driverBusy("liquid-handler-1", "fill_reagent_plate")
	h.driverIdle("incubator-1") // did the work, never said so

	h.reconcile()
	h.tick(lostGrace / 2)
	h.reconcile()
	h.expectStep("incubate_samples", StepRunning, "")

	h.tick(lostGrace / 2)
	h.reconcile()
	h.expectStep("incubate_samples", StepFailed, lostIncubation)
	h.expectRun(RunFailedDraining, "incubate_samples", lostIncubation)

	h.finish("fill_reagent_plate", "")
	h.expectRun(RunFailed, "incubate_samples", lostIncubation)
}

// The driver goes idle just before its result is published. A result that
// arrives within lostGrace wins; the suspicion is forgotten.
func TestLateResultWithinGraceIsNotLost(t *testing.T) {
	h := newRunHarness(t, nil)
	h.finish("fill_sample_plate", "")
	h.driverBusy("liquid-handler-1", "fill_reagent_plate")
	h.driverIdle("incubator-1")
	h.reconcile() // suspected

	h.finish("incubate_samples", "")
	h.tick(lostGrace)
	h.reconcile()
	h.expectStep("incubate_samples", StepCompleted, "")
	h.expectRun(RunRunning, "", "")
	if len(h.sched.suspected) != 0 {
		t.Fatalf("suspicions left behind: %v", h.sched.suspected)
	}
}

// A driver that says it is working on the step is believed, however long it
// takes.
func TestBusyStepIsNeverLost(t *testing.T) {
	h := newRunHarness(t, nil)
	h.driverBusy("liquid-handler-1", "fill_sample_plate")
	for range 5 {
		h.reconcile()
		h.tick(10 * lostGrace)
	}
	h.expectStep("fill_sample_plate", StepRunning, "")
	h.expectRun(RunRunning, "", "")
}

// Suspicion belongs to an attempt. Attempt 1 is suspected, then its (late)
// retryable failure arrives and attempt 2 goes out; if attempt 2's result is
// dropped too, it gets its own full grace period.
func TestRetryStartsAFreshSuspicion(t *testing.T) {
	h := newRunHarness(t, nil)
	h.finish("fill_sample_plate", "")
	h.driverIdle("incubator-1")
	h.reconcile() // attempt 1 suspected

	h.finishRetryable("incubate_samples", "lid jammed") // attempt 2 goes out
	h.driverIdle("incubator-1")                         // and is dropped
	h.tick(lostGrace)
	h.reconcile()
	h.expectStep("incubate_samples", StepRunning, "lid jammed (attempt 1 of 3, retrying)")

	h.tick(lostGrace)
	h.reconcile()
	lost := "result lost: incubator-1 finished incubate_samples (attempt 2) but never reported; outcome unknown"
	h.expectStep("incubate_samples", StepFailed, lost)
}

// A dropped result on a draining run used to leave it failed_draining for
// ever. Now the lost step fails and the run finishes failing, keeping the
// failure that started it.
func TestDroppedResultWhileDrainingEndsRun(t *testing.T) {
	h := newRunHarness(t, nil)
	driveToIncubatorFailure(h) // fill_buffer_plate still on the liquid handler
	h.driverIdle("liquid-handler-1")

	h.reconcile()
	h.tick(lostGrace)
	h.reconcile()
	h.expectStep("fill_buffer_plate", StepFailed,
		"result lost: liquid-handler-1 finished fill_buffer_plate (attempt 1) but never reported; outcome unknown")
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")
}

// A driver that cannot be asked might still be working, or might not: we
// cannot tell, so after the same grace its step fails as outcome unknown --
// as a command to that driver would have.
func TestUnreachableDriverFailsStepAfterGrace(t *testing.T) {
	h := newRunHarness(t, nil)
	h.bus.setState("liquid-handler-1", driverAnswer{err: errUnreachable})
	h.reconcile()
	h.expectStep("fill_sample_plate", StepRunning, "")

	h.tick(lostGrace)
	h.reconcile()
	reason := "driver unreachable: liquid-handler-1 did not answer while running fill_sample_plate (attempt 1): " +
		errUnreachable.Error() + "; outcome unknown"
	h.expectStep("fill_sample_plate", StepFailed, reason)
	h.expectRun(RunFailed, "fill_sample_plate", reason)
	h.expectStep("read_plate", StepSkipped, "")
}

// A driver with no record of the step (it restarted, and forgot) is also an
// unknown outcome; the message says which kind.
func TestDriverWithNoRecordOfStep(t *testing.T) {
	h := newRunHarness(t, nil)
	h.bus.setState("liquid-handler-1", driverAnswer{state: DriverState{DeviceID: "liquid-handler-1"}})
	h.reconcile()
	h.tick(lostGrace)
	h.reconcile()
	h.expectStep("fill_sample_plate", StepFailed,
		"result lost: liquid-handler-1 has no record of running fill_sample_plate (attempt 1), perhaps restarted; outcome unknown")
}

// The loop reads steps before taking the lock. If a retry goes out in between,
// the stale snapshot still shows attempt 1 -- long suspected -- but failing it
// must not touch attempt 2, which is running.
func TestStaleSnapshotCannotFailNewAttempt(t *testing.T) {
	h := newRunHarness(t, nil)
	h.finish("fill_sample_plate", "")
	h.driverBusy("liquid-handler-1", "fill_reagent_plate")
	h.driverIdle("incubator-1")
	h.reconcile() // attempt 1 suspected
	stale := h.inFlight()

	h.finishRetryable("incubate_samples", "lid jammed") // attempt 2 goes out
	h.tick(lostGrace)
	h.sched.reconcileSteps(h.ctx, stale)

	h.expectStep("incubate_samples", StepRunning, "lid jammed (attempt 1 of 3, retrying)")
	h.expectRun(RunRunning, "", "")
}
