package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// Lost results (*_DROP_PCT, see docs/drops.md).
//
// The scheduler moves a run on only when a result arrives, so it cannot notice
// a result that never does. The reconcile loop is the other source of
// progress: every reconcileEvery it compares each running step with what its
// driver says. A step whose driver is no longer working on it, and stays that
// way for lostGrace, lost its result. Nothing says whether it succeeded, so it
// fails as "outcome unknown", like a command that timed out.

const (
	reconcileEvery = 1 * time.Second

	// lostGrace is how long a result may still be in transit after its driver
	// has gone idle: the driver frees itself before publishing, and our handler
	// may be queued on the mutex. Detection lands 1-3s after the driver finishes.
	lostGrace = 1 * time.Second
)

// attempt identifies one dispatch of one step. Suspicion attaches to an
// attempt, so a retry (a new attempt) starts with a clean slate.
type attempt struct {
	stepID string
	n      int // dispatch_count
}

// RunReconciler runs the reconcile loop until ctx is done. main starts it;
// NewScheduler does not, so tests stay deterministic and call reconcile.
func (s *Scheduler) RunReconciler(ctx context.Context) {
	t := time.NewTicker(reconcileEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reconcile(ctx)
		}
	}
}

func (s *Scheduler) reconcile(ctx context.Context) {
	inFlight, err := s.store.ListInFlightSteps(ctx)
	if err != nil {
		log.Printf("reconcile: list in-flight steps: %v", err)
		return
	}
	s.reconcileSteps(ctx, inFlight)
}

// reconcileSteps checks the given in-flight steps against their drivers.
// Tests call it with one run's steps, since the test database is shared.
func (s *Scheduler) reconcileSteps(ctx context.Context, inFlight []Step) {
	// Outside the lock: DriverState can take seconds, and results must not
	// queue behind it. Our steps were read first, then the drivers: a step we
	// read as running was already accepted, so no driver snapshot predates it.
	states := make(map[string]DriverState)
	for _, st := range inFlight {
		if _, done := states[st.DeviceID]; done {
			continue
		}
		ds, err := s.bus.DriverState(ctx, st.DeviceID)
		if err != nil {
			// No verdict on an unreachable driver; its steps wait.
			log.Printf("reconcile: state of %s: %v", st.DeviceID, err)
			continue
		}
		states[st.DeviceID] = ds
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	stillSuspected := make(map[attempt]bool)
	for _, st := range inFlight {
		ds, ok := states[st.DeviceID]
		if !ok || (ds.Busy && ds.CurrentStep == st.Name) {
			continue // still working, or we cannot tell
		}

		a := attempt{st.ID, st.DispatchCount}
		stillSuspected[a] = true
		first, seen := s.suspected[a]
		if !seen {
			s.suspected[a] = now
			continue
		}
		if now.Sub(first) < lostGrace {
			continue
		}

		// Conditional on the attempt: a result recorded, or a retry sent,
		// since the snapshot makes this a no-op.
		msg := lostMessage(st, ds)
		lost, err := s.store.RecordStepLost(ctx, st.RunID, st.ID, st.DispatchCount, msg)
		if err != nil {
			s.abandon(ctx, st.RunID, errTypeDatabase,
				fmt.Errorf("recording lost result for %s: %w", st.Name, err))
			continue
		}
		if !lost {
			continue
		}
		log.Printf("reconcile: run %s: %s: %s", st.RunID, st.Name, msg)
		if err := s.advance(ctx, st.RunID); err != nil {
			s.abandon(ctx, st.RunID, errTypeDatabase,
				fmt.Errorf("scheduling after lost result for %s: %w", st.Name, err))
		}
	}

	// Forget suspicions that resolved themselves: the result arrived, the step
	// was retried, or the run ended. (Suspicions about steps outside inFlight
	// are dropped too; in production inFlight is every in-flight step.)
	for a := range s.suspected {
		if !stillSuspected[a] {
			delete(s.suspected, a)
		}
	}
}

// lostMessage says what the driver knows about a step we lost. Either way the
// outcome is unknown; the difference helps whoever reads it.
func lostMessage(st Step, ds DriverState) string {
	runs := 0
	for _, id := range ds.Executed {
		if id == st.ID {
			runs++
		}
	}
	if runs >= st.DispatchCount {
		return fmt.Sprintf("result lost: %s finished %s (attempt %d) but never reported; outcome unknown",
			st.DeviceID, st.Name, st.DispatchCount)
	}
	return fmt.Sprintf("result lost: %s has no record of running %s (attempt %d), perhaps restarted; outcome unknown",
		st.DeviceID, st.Name, st.DispatchCount)
}
