package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
)

// Scheduler owns the question "what should be running right now, and on what?".
//
// Design (see docs/driver-interaction.md and DIARY.md):
//
//   - One run at a time is assumed.
//   - Every decision happens under one mutex, held across decide -> send ->
//     record outcome. That is what stops two simultaneous results both deciding
//     the same step is runnable, and it is why steps need no "dispatched" state:
//     nothing can observe a step between being chosen and its outcome recorded.
//   - Each Start and each result triggers a full rescan of the run's steps.
//   - A device is busy if one of our steps is running on it. We do not send it
//     more work until that step's result arrives.
//   - Any driver error fails the run: a result with an error, or a command that
//     could not be delivered or answered. The run records the first failed step
//     and its reason. If steps are still on instruments, which cannot be
//     cancelled, the run is failed_draining until they report, then failed.
type Scheduler struct {
	store *Store
	bus   Bus

	mu sync.Mutex
}

func NewScheduler(store *Store, bus Bus) *Scheduler {
	return &Scheduler{store: store, bus: bus}
}

// Start begins executing a run.
func (s *Scheduler) Start(ctx context.Context, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.store.StartRun(ctx, runID); err != nil {
		return err
	}
	if err := s.advance(ctx, runID); err != nil {
		s.abandon(ctx, runID, err)
		return err
	}
	return nil
}

// HandleResult records that a driver finished a step and moves the run on.
// May be called concurrently.
func (s *Scheduler) HandleResult(ctx context.Context, res StepResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	status := StepCompleted
	if res.Error != "" {
		status = StepFailed
	}
	recorded, err := s.store.RecordStepFinished(ctx, res.RunID, res.StepID, status, res.Error)
	if err != nil {
		s.abandon(ctx, res.RunID, fmt.Errorf("record result for %s: %w", res.StepName, err))
		return
	}
	if !recorded {
		log.Printf("scheduler: ignoring result for %s (%s): not a running step of run %s",
			res.StepName, res.StepID, res.RunID)
		return
	}
	log.Printf("scheduler: %s on %s finished as %s", res.StepName, res.DeviceID, status)

	if err := s.advance(ctx, res.RunID); err != nil {
		s.abandon(ctx, res.RunID, err)
	}
}

// advance looks at the whole run and does whatever is due: finish the run if
// it is done, otherwise dispatch every step that is ready and whose device is
// free. Must be called with s.mu held.
func (s *Scheduler) advance(ctx context.Context, runID string) error {
	run, err := s.store.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status != RunRunning && run.Status != RunFailedDraining {
		return nil // over; a late result is recorded but moves nothing
	}

	steps, err := s.store.ListSteps(ctx, runID)
	if err != nil {
		return err
	}

	status := make(map[string]string, len(steps))
	busy := make(map[string]bool) // devices with one of our steps on them
	completed := 0
	var failed *Step
	for i, st := range steps {
		status[st.Name] = st.Status
		switch st.Status {
		case StepRunning:
			busy[st.DeviceID] = true
		case StepCompleted:
			completed++
		case StepFailed:
			if failed == nil {
				failed = &steps[i]
			}
		}
	}

	if run.Status == RunFailedDraining {
		if len(busy) == 0 {
			log.Printf("scheduler: run %s drained, now failed", runID)
			return s.store.FinishRun(ctx, runID, RunFailedDraining, RunFailed)
		}
		return nil
	}
	if failed != nil {
		reason := ""
		if failed.Error != nil {
			reason = *failed.Error
		}
		return s.fail(ctx, runID, failed.Name, reason, len(busy))
	}
	if completed == len(steps) {
		log.Printf("scheduler: run %s completed", runID)
		return s.store.FinishRun(ctx, runID, RunRunning, RunCompleted)
	}

	refused := make(map[string]bool) // devices that refused during this scan
	var refusals []Step              // steps refused, with the reason in Error
	for _, st := range readyByPriority(steps, status) {
		if busy[st.DeviceID] || refused[st.DeviceID] {
			continue
		}
		ack, err := s.bus.SendCommand(ctx, StepCommand{
			RunID: runID, StepID: st.ID, StepName: st.Name, DeviceID: st.DeviceID,
		})
		if err != nil {
			// The driver may or may not have taken it. Fail rather than risk
			// running it twice.
			msg := fmt.Sprintf("dispatch failed, outcome unknown: %v", err)
			if rerr := s.store.RecordDispatchFailed(ctx, st.ID, msg); rerr != nil {
				log.Printf("scheduler: record dispatch failure for %s: %v", st.Name, rerr)
			}
			return s.fail(ctx, runID, st.Name, msg, len(busy))
		}
		if !ack.Accepted {
			// Our own records say the device is free, so something we do not
			// know about is using it. The step stays pending and is looked at
			// again on the next result -- if one is coming (see below). Offer
			// the device nothing else this scan; it is not ours to count as in
			// flight, so it stays out of busy.
			log.Printf("scheduler: %s refused %s: %s", st.DeviceID, st.Name, ack.Reason)
			refused[st.DeviceID] = true
			msg := fmt.Sprintf("refused by %s (%s) with nothing in flight to wait for",
				st.DeviceID, ack.Reason)
			st.Error = &msg
			refusals = append(refusals, st)
			continue
		}
		if err := s.store.RecordStepRunning(ctx, st.ID); err != nil {
			return fmt.Errorf("record %s running: %w", st.Name, err)
		}
		busy[st.DeviceID] = true
		log.Printf("scheduler: dispatched %s to %s", st.Name, st.DeviceID)
	}

	// Liveness (docs/liveness.md): a scan must not end with the run unfinished
	// and nothing in flight, because only a result triggers the next scan.
	// Every run that gets here had its ready steps refused. With no timer to
	// try again later, fail the run rather than let it hang.
	if len(busy) == 0 {
		if len(refusals) == 0 {
			return s.fail(ctx, runID, "", "no step could be dispatched and none is in flight", 0)
		}
		for _, st := range refusals {
			if err := s.store.RecordDispatchFailed(ctx, st.ID, *st.Error); err != nil {
				return fmt.Errorf("record refusal of %s: %w", st.Name, err)
			}
		}
		return s.fail(ctx, runID, refusals[0].Name, *refusals[0].Error, 0)
	}
	return nil
}

// fail stops a running run because stepName failed. Nothing new is dispatched
// from here on. With steps still on instruments the run drains first.
func (s *Scheduler) fail(ctx context.Context, runID, stepName, reason string, inFlight int) error {
	status := RunFailed
	if inFlight > 0 {
		status = RunFailedDraining
	}
	log.Printf("scheduler: run %s %s: %s: %s (%d step(s) still on instruments)",
		runID, status, stepName, reason, inFlight)
	return s.store.FailRun(ctx, runID, status, stepName, reason)
}

// abandon is the last resort when the scheduler itself cannot make progress,
// e.g. the database is unreachable: log, and try to fail the run rather than
// leave it running with nothing driving it. No step is to blame, and whether
// anything is still on an instrument is unknown, so it goes straight to failed.
func (s *Scheduler) abandon(ctx context.Context, runID string, cause error) {
	log.Printf("scheduler: run %s: %v", runID, cause)
	if err := s.store.FailRun(ctx, runID, RunFailed, "", "scheduler error: "+cause.Error()); err != nil {
		log.Printf("scheduler: could not fail run %s: %v", runID, err)
	}
}

// readyByPriority returns the pending steps whose dependencies have all
// completed, most urgent first.
//
// Urgency is the length of the longest chain of steps still to come after this
// one. When two ready steps want the same device, the one on the longer chain
// goes first: in the default workflow, filling the reagent plate before the
// buffer plate lets the warm-up overlap with the buffer fill, which is the
// difference between a 10s run and a 12s one.
func readyByPriority(steps []Step, status map[string]string) []Step {
	var ready []Step
	for _, st := range steps {
		if st.Status == StepPending && depsCompleted(st, status) {
			ready = append(ready, st)
		}
	}
	if len(ready) < 2 {
		return ready
	}

	chain := chainLengths(steps)
	sort.SliceStable(ready, func(i, j int) bool {
		return chain[ready[i].Name] > chain[ready[j].Name]
	})
	return ready
}

func depsCompleted(st Step, status map[string]string) bool {
	for _, dep := range st.DependsOn {
		if status[dep] != StepCompleted {
			return false
		}
	}
	return true
}

// chainLengths maps each step to the number of steps on the longest path from
// it to the end of the workflow, itself included. Steps are assumed to take
// equal time. The workflow loader has already rejected cycles.
func chainLengths(steps []Step) map[string]int {
	dependents := make(map[string][]string, len(steps))
	for _, st := range steps {
		for _, dep := range st.DependsOn {
			dependents[dep] = append(dependents[dep], st.Name)
		}
	}

	length := make(map[string]int, len(steps))
	var walk func(name string) int
	walk = func(name string) int {
		if n, ok := length[name]; ok {
			return n
		}
		longest := 0
		for _, next := range dependents[name] {
			longest = max(longest, walk(next))
		}
		length[name] = longest + 1
		return length[name]
	}
	for _, st := range steps {
		walk(st.Name)
	}
	return length
}
