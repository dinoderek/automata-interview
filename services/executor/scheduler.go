package main

import (
	"context"
	"log"
)

// Scheduler is the part you need to build.
//
// It owns the question "what should be running right now, and on what?". The
// executor gives it two entry points:
//
//   - Start is called when someone starts a run.
//   - HandleResult is called every time a driver reports a step finished.
//     These may be called concurrently.
//
// What it has to do:
//
//   - Run every step of the DAG exactly once, respecting depends_on.
//   - Run independent branches at the same time. A run that could take 10
//     seconds should not take 14.
//   - Respect the drivers. A driver does one thing at a time and will refuse a
//     command while it is busy (CommandAck.Accepted == false). Refusals are not
//     fatal, but a step that gets refused and forgotten stalls the run.
//   - Move the run to completed, or failed if a step fails.
//
// Check your work with ./scripts/acceptance.sh.
//
// The Store and Bus are yours to extend -- add methods, change the schema in
// db/init.sql, whatever you need. Nothing outside this file has to stay as it is.
type Scheduler struct {
	store *Store
	bus   Bus
}

func NewScheduler(store *Store, bus Bus) *Scheduler {
	return &Scheduler{store: store, bus: bus}
}

// Start begins executing a run.
func (s *Scheduler) Start(ctx context.Context, runID string) error {
	if err := s.store.StartRun(ctx, runID); err != nil {
		return err
	}

	// TODO: work out which steps can run now, and get them going.
	log.Printf("scheduler: run %s started, but nothing is scheduled yet", runID)

	return nil
}

// HandleResult records that a driver finished a step and moves the run on.
// May be called concurrently.
func (s *Scheduler) HandleResult(ctx context.Context, res StepResult) {
	// TODO: record the result, then work out what can run now.
	log.Printf("scheduler: driver %s reported %s finished, and nothing happened",
		res.DeviceID, res.StepName)
}
