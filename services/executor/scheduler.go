package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

// maxAttempts bounds how many times a step is run when its driver keeps
// reporting retryable failures. A retry that never gives up is a hang.
const maxAttempts = 3

// Scheduler owns the question "what should be running right now, and on what?".
//
// Design (see docs/driver-interaction.md and DIARY.md):
//
//   - One run at a time, enforced at Start (Store.StartRun).
//   - Every decision happens under one mutex: that is what stops two
//     simultaneous results both deciding the same step is runnable. Commands
//     are not sent under it. A scan claims each step it chooses (pending ->
//     dispatched) and the claims are sent after the lock is released; the
//     outcome is recorded under the lock again. A driver slow to answer holds
//     up nothing but its own command.
//   - Each Start and each result triggers a full rescan of the run's steps.
//   - A device is busy if one of our steps is dispatched or running on it. We
//     do not send it more work until that step's result arrives.
//   - So a refusal cannot be caused by our own work: the device is not in the
//     state we believe. Like any driver error, it fails the step.
//   - Any driver error fails the run: a result with an error, or a command that
//     was refused, or could not be delivered or answered. The run records the first failed step
//     and its reason. If steps are still on instruments, which cannot be
//     cancelled, the run is failed_draining until they report, then failed.
//     Steps that never ran are marked skipped.
//   - Except: a failed step the driver calls retryable is sent again, up to
//     maxAttempts in all, while the run is still running.
//   - Results can be lost. A reconcile loop (reconciler.go) compares running
//     steps with what their drivers report, and fails a step whose driver has
//     finished with it but never reported.
type Scheduler struct {
	store *Store
	bus   Bus
	now   func() time.Time
	fatal func(format string, args ...any) // log.Fatalf; see abandon

	mu        sync.Mutex
	suspected map[attempt]time.Time // guarded by mu; see reconciler.go
	sending   map[attempt]bool      // guarded by mu; claims whose command is in flight
}

func NewScheduler(store *Store, bus Bus) *Scheduler {
	return &Scheduler{store: store, bus: bus, now: time.Now, fatal: log.Fatalf,
		suspected: make(map[attempt]time.Time), sending: make(map[attempt]bool)}
}

// claim is a step chosen and marked dispatched by a scan, to be sent once the
// lock is released.
type claim struct {
	cmd     StepCommand
	attempt int
}

// Start begins executing a run.
func (s *Scheduler) Start(ctx context.Context, runID string) error {
	// ctx is the HTTP request's. A client hanging up must not abandon the run
	// half-dispatched (and be misreported as a database error).
	ctx = context.WithoutCancel(ctx)

	claims, err := s.start(ctx, runID)
	if err != nil {
		return err
	}
	s.send(ctx, claims)
	return nil
}

func (s *Scheduler) start(ctx context.Context, runID string) ([]claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.store.StartRun(ctx, runID); err != nil {
		return nil, err
	}
	claims, err := s.advance(ctx, runID)
	if err != nil {
		err = fmt.Errorf("scheduling run after start: %w", err)
		s.abandon(ctx, runID, errTypeDatabase, err)
		return nil, err
	}
	return claims, nil
}

// HandleResult records that a driver finished a step and moves the run on.
// May be called concurrently.
func (s *Scheduler) HandleResult(ctx context.Context, res StepResult) {
	s.send(ctx, s.handleResult(ctx, res))
}

func (s *Scheduler) handleResult(ctx context.Context, res StepResult) []claim {
	s.mu.Lock()
	defer s.mu.Unlock()

	recorded, status, err := s.record(ctx, res)
	if err != nil {
		s.abandon(ctx, res.RunID, errTypeDatabase,
			fmt.Errorf("recording result for %s: %w", res.StepName, err))
		return nil
	}
	if !recorded {
		log.Printf("scheduler: ignoring result for %s (%s): not a step on an instrument in run %s",
			res.StepName, res.StepID, res.RunID)
		return nil
	}
	log.Printf("scheduler: %s on %s finished as %s", res.StepName, res.DeviceID, status)

	claims, err := s.advance(ctx, res.RunID)
	if err != nil {
		s.abandon(ctx, res.RunID, errTypeDatabase,
			fmt.Errorf("scheduling after result for %s: %w", res.StepName, err))
		return nil
	}
	return claims
}

// record applies a result to its step, and reports whether it applied (the
// step was running in that run) and what the step became.
//
// A failed attempt goes back to pending, to be dispatched again by the next
// scan, if the driver says the step can be repeated, the run is still running,
// and the step has attempts left. Otherwise the step fails, and its error says
// why it was not retried.
func (s *Scheduler) record(ctx context.Context, res StepResult) (bool, string, error) {
	if res.Error == "" {
		ok, err := s.store.RecordStepFinished(ctx, res.RunID, res.StepID, StepCompleted, "")
		if err != nil {
			return false, "", fmt.Errorf("recording step completed: %w", err)
		}
		return ok, StepCompleted, nil
	}

	msg := res.Error
	if res.Retryable {
		step, err := s.store.GetStep(ctx, res.RunID, res.StepID)
		if errors.Is(err, sql.ErrNoRows) {
			return false, "", nil // not a step of this run
		}
		if err != nil {
			return false, "", fmt.Errorf("reading step: %w", err)
		}
		run, err := s.store.GetRun(ctx, res.RunID)
		if err != nil {
			return false, "", fmt.Errorf("reading run: %w", err)
		}
		switch {
		case run.Status != RunRunning:
			msg = fmt.Sprintf("%s (retryable, not retried: run is %s)", res.Error, run.Status)
		case step.DispatchCount >= maxAttempts:
			msg = fmt.Sprintf("%s (attempt %d of %d, giving up)", res.Error, step.DispatchCount, maxAttempts)
		default:
			msg = fmt.Sprintf("%s (attempt %d of %d, retrying)", res.Error, step.DispatchCount, maxAttempts)
			ok, err := s.store.RecordStepRetrying(ctx, res.RunID, res.StepID, msg)
			if err != nil {
				return false, "", fmt.Errorf("recording step retrying: %w", err)
			}
			return ok, "retrying", nil
		}
	}
	ok, err := s.store.RecordStepFinished(ctx, res.RunID, res.StepID, StepFailed, msg)
	if err != nil {
		return false, "", fmt.Errorf("recording step failed: %w", err)
	}
	return ok, StepFailed, nil
}

// advance looks at the whole run and does whatever is due: finish the run if
// it is done, otherwise claim every step that is ready and whose device is
// free, and return the claims for send. Must be called with s.mu held.
func (s *Scheduler) advance(ctx context.Context, runID string) ([]claim, error) {
	run, err := s.store.GetRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("reading run: %w", err)
	}
	if run.Status != RunRunning && run.Status != RunFailedDraining {
		return nil, nil // over; a late result is recorded but moves nothing
	}

	steps, err := s.store.ListSteps(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("listing steps: %w", err)
	}

	status := make(map[string]string, len(steps))
	busy := make(map[string]bool) // devices with one of our steps on them
	completed := 0
	var failed *Step
	for i, st := range steps {
		status[st.Name] = st.Status
		switch st.Status {
		case StepDispatched, StepRunning:
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
			if err := s.store.FinishRun(ctx, runID, RunFailedDraining, RunFailed); err != nil {
				return nil, fmt.Errorf("finishing drained run: %w", err)
			}
		}
		return nil, nil
	}
	if failed != nil {
		reason := ""
		if failed.Error != nil {
			reason = *failed.Error
		}
		return nil, s.fail(ctx, runID, failed.Name, reason, len(busy))
	}
	if completed == len(steps) {
		log.Printf("scheduler: run %s completed", runID)
		if err := s.store.FinishRun(ctx, runID, RunRunning, RunCompleted); err != nil {
			return nil, fmt.Errorf("completing run: %w", err)
		}
		return nil, nil
	}

	var claims []claim
	for _, st := range readyByPriority(steps, status) {
		if busy[st.DeviceID] {
			continue
		}
		n, err := s.store.ClaimStep(ctx, st.ID)
		if err != nil {
			return nil, fmt.Errorf("claiming %s: %w", st.Name, err)
		}
		busy[st.DeviceID] = true
		s.sending[attempt{st.ID, n}] = true
		claims = append(claims, claim{
			cmd:     StepCommand{RunID: runID, StepID: st.ID, StepName: st.Name, DeviceID: st.DeviceID},
			attempt: n,
		})
	}

	// Liveness (docs/liveness.md): a scan must not end with the run unfinished
	// and nothing on an instrument, because only a result triggers the next
	// scan. For an acyclic workflow some step is always ready and, with nothing
	// busy, its device free -- so this is a guard, not a path.
	if len(busy) == 0 {
		return nil, s.fail(ctx, runID, "", "no step could be dispatched and none is in flight", 0)
	}
	return claims, nil
}

// send sends each claim, without the lock, then records what the driver said.
// Claims made by one scan go out one after another, all of them: they were
// chosen before any of them could fail. A failure fails the run, and whatever
// was accepted drains as usual.
func (s *Scheduler) send(ctx context.Context, claims []claim) {
	for len(claims) > 0 {
		c := claims[0]
		claims = claims[1:]
		ack, err := s.bus.SendCommand(ctx, c.cmd)
		claims = append(claims, s.recordSend(ctx, c, ack, err)...)
	}
}

// recordSend records a claim's outcome and returns any claims that follow.
func (s *Scheduler) recordSend(ctx context.Context, c claim, ack CommandAck, sendErr error) []claim {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sending, attempt{c.cmd.StepID, c.attempt})

	if sendErr == nil && ack.Accepted {
		running, err := s.store.RecordStepRunning(ctx, c.cmd.StepID, c.attempt)
		if err != nil {
			s.abandon(ctx, c.cmd.RunID, errTypeDatabase,
				fmt.Errorf("recording %s running: %w", c.cmd.StepName, err))
			return nil
		}
		if running {
			log.Printf("scheduler: dispatched %s to %s", c.cmd.StepName, c.cmd.DeviceID)
		} else {
			log.Printf("scheduler: %s's result arrived before its ack", c.cmd.StepName)
		}
		return nil
	}

	var msg string
	if sendErr != nil {
		// The driver may or may not have taken it. Fail rather than risk
		// running it twice.
		msg = fmt.Sprintf("dispatch failed, outcome unknown: %v", sendErr)
	} else {
		// Nothing of ours is on the device, so something we do not know about
		// is: the instruments are not in the state we believe.
		msg = fmt.Sprintf("refused by %s (%s): device not in the expected state", c.cmd.DeviceID, ack.Reason)
	}
	log.Printf("scheduler: %s: %s", c.cmd.StepName, msg)
	if _, err := s.store.RecordDispatchFailed(ctx, c.cmd.StepID, c.attempt, msg); err != nil {
		s.abandon(ctx, c.cmd.RunID, errTypeDatabase,
			fmt.Errorf("recording dispatch failure of %s: %w", c.cmd.StepName, err))
		return nil
	}
	claims, err := s.advance(ctx, c.cmd.RunID) // fails the run
	if err != nil {
		s.abandon(ctx, c.cmd.RunID, errTypeDatabase,
			fmt.Errorf("scheduling after dispatch failure of %s: %w", c.cmd.StepName, err))
		return nil
	}
	return claims
}

// fail stops a running run because stepName failed. Nothing new is dispatched
// from here on. With steps still on instruments the run drains first.
func (s *Scheduler) fail(ctx context.Context, runID, stepName, reason string, inFlight int) error {
	status := RunFailed
	if inFlight > 0 {
		status = RunFailedDraining
	}
	cause := reason
	if stepName != "" {
		cause = stepName + ": " + reason
	}
	log.Printf("scheduler: run %s %s: %s (%d step(s) still on instruments)",
		runID, status, cause, inFlight)
	if err := s.store.FailRun(ctx, runID, status, stepName, reason); err != nil {
		return fmt.Errorf("failing run: %w", err)
	}
	if err := s.store.SkipPendingSteps(ctx, runID); err != nil {
		return fmt.Errorf("skipping steps that never ran: %w", err)
	}
	return nil
}

// errTypeDatabase is the error type of every abandon today: advance, record
// and the reconciler only fail when a store call does. Driver errors never get
// here -- they are step failures.
const errTypeDatabase = "database error"

// errTypeRestart is the error type of runs failed by FailActiveRuns.
const errTypeRestart = "executor restarted"

// abandon is the last resort when the scheduler itself cannot make progress:
// fail the run -- running or draining -- rather than leave it active with
// nothing driving it. Whether anything is still on an instrument is unknown,
// so it goes straight to failed. The run keeps its first failure if it has
// one; otherwise its error reads "<errType>: <cause>".
//
// If that cannot be recorded, the executor exits. Its table no longer matches
// the instruments, and carrying on would be scheduling from state we cannot
// trust. Stopping is the safe failure;
// nothing restarts the executor automatically.
func (s *Scheduler) abandon(ctx context.Context, runID, errType string, cause error) {
	reason := errType + ": " + cause.Error()
	log.Printf("scheduler: run %s failed: %s", runID, reason)
	// The failure is recorded even if the caller's context is what failed.
	ctx = context.WithoutCancel(ctx)
	abandoned, err := s.store.AbandonRun(ctx, runID, reason)
	if err == nil && abandoned {
		err = s.store.SkipPendingSteps(ctx, runID)
	}
	if err != nil {
		s.fatal("scheduler: cannot record failure of run %s (%s): %v -- stopping rather than schedule from state that may not match the instruments",
			runID, reason, err)
		return
	}
	if !abandoned {
		log.Printf("scheduler: run %s had already ended", runID)
	}
}

// FailActiveRuns is called once at startup, before results are handled. A run
// still active was being driven by an executor that has gone, and results
// published meanwhile are lost, so its outcome is unknown: fail it. Steps
// still on instruments drain as usual -- their results, or the reconcile loop,
// finish the run.
func (s *Scheduler) FailActiveRuns(ctx context.Context) error {
	ids, err := s.store.ListActiveRunIDs(ctx)
	if err != nil {
		return fmt.Errorf("listing active runs: %w", err)
	}
	return s.failActiveRuns(ctx, ids)
}

// failActiveRuns fails the given runs as FailActiveRuns does. Tests call it
// with their own runs, since the test database is shared.
func (s *Scheduler) failActiveRuns(ctx context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		steps, err := s.store.ListSteps(ctx, id)
		if err != nil {
			return fmt.Errorf("listing steps of %s: %w", id, err)
		}
		inFlight := 0
		for _, st := range steps {
			if st.Status == StepRunning || st.Status == StepDispatched {
				inFlight++
			}
		}
		// Only a running run changes here; a draining run keeps its first
		// failure. advance then finishes any run with nothing in flight.
		if err := s.fail(ctx, id, "", errTypeRestart+": run was active when the executor started; outcome unknown", inFlight); err != nil {
			return err
		}
		if _, err := s.advance(ctx, id); err != nil { // the run is failing: no claims
			return fmt.Errorf("finishing %s: %w", id, err)
		}
	}
	return nil
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
