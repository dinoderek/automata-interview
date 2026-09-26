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

const refusedLH = "refused by liquid-handler-1 (busy with something else): device not in the expected state"

// Nothing of ours is on a device we send to, so a refusal means the device is
// not in the state we believe: the step fails, and the run with it. Nothing
// else is sent -- incubate_samples, ready in the same scan, is skipped.
func TestRefusalFailsStepAndRun(t *testing.T) {
	h := newRunHarness(t, map[string][]reply{"fill_reagent_plate": {refuse}})
	h.finish("fill_sample_plate", "")

	if got, want := h.bus.tried(), []string{"fill_sample_plate", "fill_reagent_plate"}; !slices.Equal(got, want) {
		t.Fatalf("offered %v, want %v", got, want)
	}
	h.expectStep("fill_reagent_plate", StepFailed, refusedLH)
	h.expectRun(RunFailed, "fill_reagent_plate", refusedLH)
	h.expectStep("incubate_samples", StepSkipped, "")
	h.expectStep("fill_buffer_plate", StepSkipped, "")
}

// A refusal while another step is on an instrument fails the run too; the
// step in flight drains.
func TestRefusalWithWorkInFlightDrains(t *testing.T) {
	h := newRunHarness(t, map[string][]reply{"fill_buffer_plate": {refuse}})
	h.finish("fill_sample_plate", "")  // fill_reagent_plate and incubate_samples go out
	h.finish("fill_reagent_plate", "") // fill_buffer_plate is next on the liquid handler

	h.expectStep("fill_buffer_plate", StepFailed, refusedLH)
	h.expectRun(RunFailedDraining, "fill_buffer_plate", refusedLH)
	h.expectStep("warm_reagent_plate", StepSkipped, "")

	h.finish("incubate_samples", "")
	h.expectRun(RunFailed, "fill_buffer_plate", refusedLH)
}

// The only ready step is refused at start: the step and the run fail.
func TestOnlyReadyStepRefusedFailsRun(t *testing.T) {
	h := newRunHarness(t, map[string][]reply{"fill_sample_plate": {refuse}})

	h.expectStep("fill_sample_plate", StepFailed, refusedLH)
	h.expectRun(RunFailed, "fill_sample_plate", refusedLH)
	h.expectSent()
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
