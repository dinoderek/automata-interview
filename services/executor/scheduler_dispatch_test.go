package main

// What happens when a command does not simply succeed: the driver refuses it,
// or the command itself errors.
//
// Runs against Postgres like the concurrency test:  docker compose run --rm tests

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/nats-io/nats.go"
)

// A refused step stays pending, untouched, and is offered again on the next
// result -- here there is one coming, because incubate_samples is in flight.
// The device that refused is offered nothing else in the same scan.
func TestRefusedStepWaitsForNextResult(t *testing.T) {
	h := newRunHarness(t, map[string][]reply{"fill_reagent_plate": {refuse}})
	h.finish("fill_sample_plate", "")

	// fill_reagent_plate was refused, so fill_buffer_plate -- same device --
	// was not tried. incubate_samples, on another device, went ahead.
	want := []string{"fill_sample_plate", "fill_reagent_plate", "incubate_samples"}
	if got := h.bus.tried(); !slices.Equal(got, want) {
		t.Fatalf("offered %v, want %v", got, want)
	}
	st := h.step("fill_reagent_plate")
	if st.Status != StepPending || st.DispatchCount != 0 || st.DispatchedAt != nil {
		t.Fatalf("refused step: status=%s dispatch_count=%d dispatched_at=%v, want pending, 0, nil",
			st.Status, st.DispatchCount, st.DispatchedAt)
	}
	h.expectRun(RunRunning, "", "")

	// The next result triggers a rescan, and the refused step goes out.
	h.finish("incubate_samples", "")
	h.expectSent("fill_sample_plate", "incubate_samples", "fill_reagent_plate")
	if st := h.step("fill_reagent_plate"); st.Status != StepRunning || st.DispatchCount != 1 {
		t.Fatalf("after retry: status=%s dispatch_count=%d, want running, 1", st.Status, st.DispatchCount)
	}
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

// A device that refused holds none of our work, so it must not count as
// in flight: with nothing else running, a send error in the same scan fails
// the run outright rather than leaving it draining, waiting for a result
// that will never come.
func TestRefusalIsNotCountedAsInFlight(t *testing.T) {
	sendErr := fmt.Errorf("command incubate_samples to incubator-1: %w", context.DeadlineExceeded)
	h := newRunHarness(t, map[string][]reply{
		"fill_reagent_plate": {refuse},
		"incubate_samples":   {{err: sendErr}},
	})
	h.finish("fill_sample_plate", "")

	h.expectRun(RunFailed, "incubate_samples", "dispatch failed, outcome unknown: "+sendErr.Error())
	h.expectStep("fill_reagent_plate", StepSkipped, "") // refused, never ran
}

// Liveness: the only ready step is refused and nothing else is in flight, so
// no result will ever come to trigger another scan. Without a timer to try
// again, the run fails -- with the refusal as its reason -- instead of hanging.
// A device held by something physical, or a driver bug, looks exactly like this.
func TestOnlyReadyStepRefusedFailsRun(t *testing.T) {
	h := newRunHarness(t, map[string][]reply{"fill_sample_plate": {refuse}})

	reason := "refused by liquid-handler-1 (busy with something else) with nothing in flight to wait for"
	h.expectStep("fill_sample_plate", StepFailed, reason)
	h.expectRun(RunFailed, "fill_sample_plate", reason)
	h.expectSent()
}

// Mid-run, every ready step is refused, on different devices. Each refused
// step records why; the run records the most urgent one. Steps that were never
// offered are skipped.
func TestAllReadyStepsRefusedFailsRun(t *testing.T) {
	h := newRunHarness(t, map[string][]reply{
		"fill_reagent_plate": {refuse},
		"incubate_samples":   {refuse},
	})
	h.finish("fill_sample_plate", "")

	reagent := "refused by liquid-handler-1 (busy with something else) with nothing in flight to wait for"
	h.expectStep("fill_reagent_plate", StepFailed, reagent)
	h.expectStep("incubate_samples", StepFailed,
		"refused by incubator-1 (busy with something else) with nothing in flight to wait for")
	h.expectStep("fill_buffer_plate", StepSkipped, "")
	h.expectRun(RunFailed, "fill_reagent_plate", reagent)
	h.expectSent("fill_sample_plate")
}
