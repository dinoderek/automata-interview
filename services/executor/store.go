package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/lib/pq"
)

var ErrRunNotFound = errors.New("run not found")

func newID(prefix string) string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b))
}

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store { return &Store{db: db} }

const runCols = `id, workflow_name, status, created_at, started_at, finished_at`

func scanRun(row interface{ Scan(...any) error }) (*Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.WorkflowName, &r.Status, &r.CreatedAt, &r.StartedAt, &r.FinishedAt)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Store) ListDevices(ctx context.Context) ([]Device, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, type FROM devices ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Device{}
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.ID, &d.Name, &d.Type); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) ListRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+runCols+` FROM runs ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Run{}
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func (s *Store) GetRun(ctx context.Context, id string) (*Run, error) {
	r, err := scanRun(s.db.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrRunNotFound
	}
	return r, err
}

// CreateRun materialises a workflow template into a run and its steps.
func (s *Store) CreateRun(ctx context.Context, workflowName string, tmpl []stepTemplate) (*Run, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	runID := newID("run")
	run, err := scanRun(tx.QueryRowContext(ctx,
		`INSERT INTO runs (id, workflow_name) VALUES ($1, $2) RETURNING `+runCols,
		runID, workflowName))
	if err != nil {
		return nil, err
	}

	for _, st := range tmpl {
		deps := st.DependsOn
		if deps == nil {
			deps = []string{}
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO steps (id, run_id, name, device_id, depends_on)
			 VALUES ($1, $2, $3, $4, $5)`,
			newID("step"), runID, st.Name, st.DeviceID, pq.Array(deps)); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return run, nil
}

const stepCols = `id, run_id, name, device_id, status, depends_on,
	dispatch_count, dispatched_at, finished_at, error`

func scanStep(row interface{ Scan(...any) error }) (*Step, error) {
	var st Step
	err := row.Scan(&st.ID, &st.RunID, &st.Name, &st.DeviceID, &st.Status,
		pq.Array(&st.DependsOn), &st.DispatchCount, &st.DispatchedAt,
		&st.FinishedAt, &st.Error)
	if err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *Store) ListSteps(ctx context.Context, runID string) ([]Step, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+stepCols+` FROM steps WHERE run_id = $1 ORDER BY name`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Step{}
	for rows.Next() {
		st, err := scanStep(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *st)
	}
	return out, rows.Err()
}

func (s *Store) StartRun(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE runs SET status = $1, started_at = now(), updated_at = now()
		 WHERE id = $2 AND status = $3`, RunRunning, id, RunPending)
	return err
}

func (s *Store) FinishRun(ctx context.Context, id, status string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE runs SET status = $1, finished_at = now(), updated_at = now()
		 WHERE id = $2`, status, id)
	return err
}

// RecordStepDispatched notes that a step has been sent to its driver.
func (s *Store) RecordStepDispatched(ctx context.Context, stepID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE steps
		    SET status = $1,
		        dispatch_count = dispatch_count + 1,
		        dispatched_at = COALESCE(dispatched_at, now()),
		        updated_at = now()
		  WHERE id = $2`, StepDispatched, stepID)
	return err
}

// RecordStepFinished notes that a step finished, successfully or not.
func (s *Store) RecordStepFinished(ctx context.Context, stepID, status, errMsg string) error {
	var e *string
	if errMsg != "" {
		e = &errMsg
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE steps SET status = $1, error = $2, finished_at = now(), updated_at = now()
		 WHERE id = $3`, status, e, stepID)
	return err
}
