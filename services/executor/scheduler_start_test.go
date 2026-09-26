package main

// One run at a time: Start refuses while another run is active.
//
// Runs against Postgres like the concurrency test:  docker compose run --rm tests

import (
	"errors"
	"strings"
	"testing"
)

// While run A is running, run B cannot start: it stays pending and nothing is
// sent for it. The error names the run in the way.
func TestStartRefusedWhileAnotherRunIsRunning(t *testing.T) {
	h := newRunHarness(t, nil)
	other := createTestRun(t, h.store)

	err := h.sched.Start(h.ctx, other)
	if !errors.Is(err, ErrAnotherRunActive) || !strings.Contains(err.Error(), h.runID) {
		t.Fatalf("Start = %v, want ErrAnotherRunActive naming %s", err, h.runID)
	}
	if r, _ := h.store.GetRun(h.ctx, other); r.Status != RunPending {
		t.Fatalf("refused run is %s, want pending", r.Status)
	}
	h.expectSent("fill_sample_plate") // run A's only
}

// A draining run still has steps on instruments, so it still counts. Once it
// has drained, the next run starts.
func TestStartRefusedUntilDrainingRunEnds(t *testing.T) {
	h := newRunHarness(t, nil)
	driveToIncubatorFailure(h)
	other := createTestRun(t, h.store)

	if err := h.sched.Start(h.ctx, other); !errors.Is(err, ErrAnotherRunActive) {
		t.Fatalf("Start while draining = %v, want ErrAnotherRunActive", err)
	}

	h.finish("fill_buffer_plate", "")
	h.expectRun(RunFailed, "incubate_samples", "incubator on fire")
	if err := h.sched.Start(h.ctx, other); err != nil {
		t.Fatalf("Start after drain = %v, want nil", err)
	}
	last := h.bus.sent()[len(h.bus.sent())-1]
	if last.RunID != other || last.StepName != "fill_sample_plate" {
		t.Fatalf("last command %s/%s, want %s/fill_sample_plate", last.RunID, last.StepName, other)
	}
}

// Starting the same run twice is refused as not pending, not as a clash with
// itself.
func TestStartTwiceIsNotPending(t *testing.T) {
	h := newRunHarness(t, nil)
	if err := h.sched.Start(h.ctx, h.runID); !errors.Is(err, ErrRunNotPending) {
		t.Fatalf("second Start = %v, want ErrRunNotPending", err)
	}
}
