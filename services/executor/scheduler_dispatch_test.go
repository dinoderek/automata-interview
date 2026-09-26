package main

// What happens when a command does not simply succeed -- the driver refuses
// it, or the command itself errors -- and what may happen while a command is
// in flight, now that it is sent without the scheduler's lock.
//
// Runs against Postgres like the concurrency test:  docker compose run --rm tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const refusedLH = "refused by liquid-handler-1 (busy with something else): device not in the expected state"

// Nothing of ours is on a device we send to, so a refusal means the device is
// not in the state we believe: the step fails, and the run with it. A claim
// made in the same scan (incubate_samples) still goes out, and drains.
func TestRefusalFailsStepAndRun(t *testing.T) {
	h := newRunHarness(t, map[string][]reply{"fill_reagent_plate": {refuse}})
	h.finish("fill_sample_plate", "")

	h.expectStep("fill_reagent_plate", StepFailed, refusedLH)
	h.expectRun(RunFailedDraining, "fill_reagent_plate", refusedLH)
	h.expectStep("incubate_samples", StepRunning, "")
	h.expectStep("fill_buffer_plate", StepSkipped, "")

	h.finish("incubate_samples", "")
	h.expectRun(RunFailed, "fill_reagent_plate", refusedLH)
}

// A command that errors might or might not have reached the driver. The step
// fails as "outcome unknown", and the run fails -- draining what is in flight.
func TestSendErrorFailsRunAsOutcomeUnknown(t *testing.T) {
	for name, sendErr := range map[string]error{
		"timeout":      fmt.Errorf("command incubate_samples to incubator-1: %w", context.DeadlineExceeded),
		"no responder": fmt.Errorf("command incubate_samples to incubator-1: %w", nats.ErrNoResponders),
	} {
		t.Run(name, func(t *testing.T) {
			h := newRunHarness(t, map[string][]reply{"incubate_samples": {{err: sendErr}}})
			h.finish("fill_sample_plate", "")

			// fill_reagent_plate went out before incubate_samples errored.
			reason := "dispatch failed, outcome unknown: " + sendErr.Error()
			h.expectStep("incubate_samples", StepFailed, reason)
			h.expectRun(RunFailedDraining, "incubate_samples", reason)

			h.finish("fill_reagent_plate", "")
			h.expectRun(RunFailed, "incubate_samples", reason)
			h.expectSent("fill_sample_plate", "fill_reagent_plate")
		})
	}
}

// Two claims from one scan both fail. The run keeps the first failure; the
// second is recorded on its step; with nothing left on an instrument the run
// ends failed rather than draining for ever.
func TestEveryClaimOfAScanFails(t *testing.T) {
	sendErr := fmt.Errorf("command incubate_samples to incubator-1: %w", context.DeadlineExceeded)
	h := newRunHarness(t, map[string][]reply{
		"fill_reagent_plate": {refuse},
		"incubate_samples":   {{err: sendErr}},
	})
	h.finish("fill_sample_plate", "")

	h.expectStep("fill_reagent_plate", StepFailed, refusedLH)
	h.expectStep("incubate_samples", StepFailed, "dispatch failed, outcome unknown: "+sendErr.Error())
	h.expectRun(RunFailed, "fill_reagent_plate", refusedLH)
	h.expectStep("fill_buffer_plate", StepSkipped, "")
}

// The only ready step is refused at start: the step and the run fail.
func TestOnlyReadyStepRefusedFailsRun(t *testing.T) {
	h := newRunHarness(t, map[string][]reply{"fill_sample_plate": {refuse}})

	h.expectStep("fill_sample_plate", StepFailed, refusedLH)
	h.expectRun(RunFailed, "fill_sample_plate", refusedLH)
	h.expectSent()
}

// Mid-run, every ready step is refused, on different devices. Each refused
// step records why; the run records the first. Steps never offered are skipped.
func TestAllReadyStepsRefusedFailsRun(t *testing.T) {
	h := newRunHarness(t, map[string][]reply{
		"fill_reagent_plate": {refuse},
		"incubate_samples":   {refuse},
	})
	h.finish("fill_sample_plate", "")

	h.expectStep("fill_reagent_plate", StepFailed, refusedLH)
	h.expectStep("incubate_samples", StepFailed,
		"refused by incubator-1 (busy with something else): device not in the expected state")
	h.expectStep("fill_buffer_plate", StepSkipped, "")
	h.expectRun(RunFailed, "fill_reagent_plate", refusedLH)
	h.expectSent("fill_sample_plate")
}

// The scheduler's lock is not held while a command is in flight: a result
// handled during the send -- beating the recorded ack -- is recorded, and the
// late ack then changes nothing. If the lock were held, this would deadlock.
func TestResultBeforeAckIsRecorded(t *testing.T) {
	h := newRunHarness(t, nil)
	h.bus.onSend = func(c StepCommand) {
		if c.StepName == "incubate_samples" {
			h.sched.HandleResult(h.ctx, StepResult{RunID: c.RunID, StepID: c.StepID,
				StepName: c.StepName, DeviceID: c.DeviceID})
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.finish("fill_sample_plate", "")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: the scheduler's lock is held across SendCommand")
	}

	h.expectStep("incubate_samples", StepCompleted, "")
	if st := h.step("incubate_samples"); st.DispatchCount != 1 || st.DispatchedAt == nil {
		t.Fatalf("incubate_samples: dispatch_count=%d dispatched_at=%v, want 1 and set",
			st.DispatchCount, st.DispatchedAt)
	}
	h.expectRun(RunRunning, "", "")
}

// While a command is in flight the driver may not have it yet: idle, no record.
// The reconcile loop must not take that for a lost result, however long the
// send takes.
func TestReconcileLeavesStepBeingSentAlone(t *testing.T) {
	h := newRunHarness(t, nil)
	h.driverBusy("liquid-handler-1", "fill_reagent_plate")
	h.bus.onSend = func(c StepCommand) {
		if c.StepName == "incubate_samples" {
			h.bus.onSend = nil
			h.driverIdle("incubator-1") // has not taken it yet
			h.reconcile()
			h.tick(10 * lostGrace)
			h.reconcile()
		}
	}
	h.finish("fill_sample_plate", "")
	h.expectStep("incubate_samples", StepRunning, "")
	h.expectRun(RunRunning, "", "")
}

// An ack is recorded only for its own attempt. If attempt 1's result (a
// retryable failure) beats its ack, and attempt 2 is already claimed, attempt
// 1's late ack must not mark attempt 2 running before its driver answers.
func TestLateAckDoesNotApplyToALaterAttempt(t *testing.T) {
	h := newRunHarness(t, nil)
	st := h.step("fill_sample_plate") // attempt 1, running
	if ok, err := h.store.RecordStepRetrying(h.ctx, h.runID, st.ID, "clogged (attempt 1 of 3, retrying)"); err != nil || !ok {
		t.Fatalf("retrying: ok=%v err=%v", ok, err)
	}
	if n, err := h.store.ClaimStep(h.ctx, st.ID); err != nil || n != 2 {
		t.Fatalf("claim: attempt=%d err=%v, want 2", n, err)
	}

	if ok, err := h.store.RecordStepRunning(h.ctx, st.ID, 1); err != nil || ok {
		t.Fatalf("attempt 1's ack applied (ok=%v err=%v) to attempt 2", ok, err)
	}
	h.expectStep("fill_sample_plate", StepDispatched, "clogged (attempt 1 of 3, retrying)")
}
