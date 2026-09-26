package main

// This test forces the situation the drivers cannot reliably produce: two steps
// finishing at the exact same instant. It exists because real instruments finish
// milliseconds apart, which is long enough to hide a scheduling race.
//
// It only uses NewStore, NewScheduler, Start and HandleResult. Everything inside
// those is yours.
//
// Run it with:  docker compose run --rm tests

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// recordingBus accepts every command and remembers what it was asked to send.
type recordingBus struct {
	mu       sync.Mutex
	commands []StepCommand
}

func (b *recordingBus) SendCommand(ctx context.Context, cmd StepCommand) (CommandAck, error) {
	b.mu.Lock()
	b.commands = append(b.commands, cmd)
	b.mu.Unlock()
	return CommandAck{Accepted: true}, nil
}

func (b *recordingBus) OnStepResult(handler func(context.Context, StepResult)) error { return nil }

func (b *recordingBus) DriverState(ctx context.Context, deviceID string) (DriverState, error) {
	return DriverState{DeviceID: deviceID}, nil
}

func (b *recordingBus) Close() {}

func (b *recordingBus) sent() []StepCommand {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]StepCommand{}, b.commands...)
}

// The scenario is repeated because the outcome depends on how two goroutines
// interleave across a couple of database round trips. One attempt is not enough
// to be sure.
const concurrencyAttempts = 10

func TestSimultaneousResultsDispatchEachStepOnce(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL not set -- run with: docker compose run --rm tests")
	}

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	path := os.Getenv("WORKFLOWS_FILE")
	if path == "" {
		path = "/src/config/workflows.yaml"
	}
	set, err := LoadWorkflows(path)
	if err != nil {
		t.Fatalf("load workflows: %v", err)
	}
	name := set.Default()
	tmpl, err := set.Get(name)
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}

	ctx := context.Background()
	store := NewStore(db)

	for attempt := 1; attempt <= concurrencyAttempts; attempt++ {
		runOneAttempt(t, ctx, store, name, tmpl, attempt)
	}
}

func runOneAttempt(t *testing.T, ctx context.Context, store *Store,
	name string, tmpl []stepTemplate, attempt int) {

	run, err := store.CreateRun(ctx, name, tmpl)
	if err != nil {
		t.Fatalf("attempt %d: create run: %v", attempt, err)
	}

	bus := &recordingBus{}
	sched := NewScheduler(store, bus)

	if err := sched.Start(ctx, run.ID); err != nil {
		t.Fatalf("attempt %d: start run: %v", attempt, err)
	}

	// Stand in for the drivers. Every time more than one step is in flight,
	// report them all finished at the same moment.
	completed := map[string]bool{}
	for round := 0; round < 20; round++ {
		var batch []StepCommand
		for _, cmd := range bus.sent() {
			if !completed[cmd.StepID] {
				completed[cmd.StepID] = true
				batch = append(batch, cmd)
			}
		}
		if len(batch) == 0 {
			break
		}

		var release sync.WaitGroup
		release.Add(1)
		var finished sync.WaitGroup
		for _, cmd := range batch {
			finished.Add(1)
			go func(c StepCommand) {
				defer finished.Done()
				release.Wait() // everyone starts together
				sched.HandleResult(ctx, StepResult{
					RunID:    c.RunID,
					StepID:   c.StepID,
					StepName: c.StepName,
					DeviceID: c.DeviceID,
				})
			}(cmd)
		}
		release.Done()
		finished.Wait()

		time.Sleep(20 * time.Millisecond) // let any trailing work settle
	}

	// Every step should have been commanded exactly once.
	counts := map[string]int{}
	names := map[string]string{}
	for _, cmd := range bus.sent() {
		counts[cmd.StepID]++
		names[cmd.StepID] = cmd.StepName
	}
	for id, n := range counts {
		if n > 1 {
			t.Errorf("attempt %d: step %s was dispatched %d times -- two results both decided it was runnable and both sent it",
				attempt, names[id], n)
		}
	}

	steps, err := store.ListSteps(ctx, run.ID)
	if err != nil {
		t.Fatalf("attempt %d: list steps: %v", attempt, err)
	}
	for _, st := range steps {
		if st.Status != StepCompleted {
			t.Errorf("attempt %d: step %s ended as %q, expected completed",
				attempt, st.Name, st.Status)
		}
	}
}
