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

var (
	ErrRunNotFound   = errors.New("run not found")
	ErrRunNotPending = errors.New("run is not pending")

	// ErrAnotherRunActive: one run at a time. A run is active while it is
	// running or failed_draining (steps still on instruments).
	ErrAnotherRunActive = errors.New("another run is active")
)

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

const runCols = `id, workflow_name, status, failed_step, error, created_at, started_at, finished_at`

func scanRun(row interface{ Scan(...any) error }) (*Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.WorkflowName, &r.Status, &r.FailedStep, &r.Error,
		&r.CreatedAt, &r.StartedAt, &r.FinishedAt)
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

// StartRun moves a run from pending to running, provided no other run is
// active: one run at a time. Otherwise it changes nothing and returns
// ErrRunNotPending, or ErrAnotherRunActive naming the active run.
//
// The check and the update are one statement, but READ COMMITTED does not
// make it safe against a concurrent StartRun on its own; the scheduler's mutex
// serialises starts. (Across executor processes, a partial unique index on
// active runs would enforce it in the database.)
func (s *Store) StartRun(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE runs SET status = $1, started_at = now(), updated_at = now()
		 WHERE id = $2 AND status = $3
		   AND NOT EXISTS (SELECT 1 FROM runs WHERE status IN ($1, $4))`,
		RunRunning, id, RunPending, RunFailedDraining)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n == 1 {
		return err
	}

	// Not started: say why.
	run, err := s.GetRun(ctx, id)
	if err != nil {
		return err
	}
	if run.Status != RunPending {
		return ErrRunNotPending
	}
	var active string
	if err := s.db.QueryRowContext(ctx,
		`SELECT id FROM runs WHERE status IN ($1, $2) LIMIT 1`,
		RunRunning, RunFailedDraining).Scan(&active); err != nil {
		return fmt.Errorf("start refused, but finding the active run failed: %w", err)
	}
	return fmt.Errorf("%w: %s", ErrAnotherRunActive, active)
}

// FinishRun ends a run that is in status from: running -> completed, or
// failed_draining -> failed. A run in any other status is left alone.
func (s *Store) FinishRun(ctx context.Context, id, from, to string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE runs SET status = $1, finished_at = now(), updated_at = now()
		 WHERE id = $2 AND status = $3`, to, id, from)
	return err
}

// FailRun records the failure that stopped a running run: the step that
// failed (empty if the scheduler itself failed) and why. status is failed, or
// failed_draining if steps are still on instruments, in which case finished_at
// is left for FinishRun. Only a running run is changed, so the first failure
// is the one kept.
func (s *Store) FailRun(ctx context.Context, id, status, stepName, reason string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE runs
		    SET status = $1, failed_step = $2, error = $3, updated_at = now(),
		        finished_at = CASE WHEN $1 = $4 THEN now() END
		  WHERE id = $5 AND status = $6`,
		status, nullable(stepName), nullable(reason), RunFailed, id, RunRunning)
	return err
}

// AbandonRun fails an active run (running or failed_draining) outright, for a
// scheduler error. A failure already recorded on the run is kept: the first
// failure is the reason it stopped. Reports false if the run was not active.
func (s *Store) AbandonRun(ctx context.Context, id, reason string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE runs
		    SET status = $1, error = COALESCE(error, $2), finished_at = now(), updated_at = now()
		  WHERE id = $3 AND status IN ($4, $5)`,
		RunFailed, nullable(reason), id, RunRunning, RunFailedDraining)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// SkipPendingSteps marks every step of a failed run that never ran as skipped,
// so it is not mistaken for one waiting its turn.
func (s *Store) SkipPendingSteps(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE steps SET status = $1, updated_at = now() WHERE run_id = $2 AND status = $3`,
		StepSkipped, runID, StepPending)
	return err
}

// ListActiveRunIDs returns the runs that are running or failed_draining.
func (s *Store) ListActiveRunIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM runs WHERE status IN ($1, $2) ORDER BY created_at`,
		RunRunning, RunFailedDraining)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// RecordStepRunning notes that a driver accepted a pending step. dispatched_at
// is only stamped here, on acceptance, because the timeline treats it as the
// moment the step started occupying its device.
func (s *Store) RecordStepRunning(ctx context.Context, stepID string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE steps
		    SET status = $1,
		        dispatch_count = dispatch_count + 1,
		        dispatched_at = COALESCE(dispatched_at, now()),
		        updated_at = now()
		  WHERE id = $2 AND status = $3`, StepRunning, stepID, StepPending)
	if err != nil {
		return err
	}
	return requireOneRow(res, fmt.Errorf("step %s was not pending", stepID))
}

// RecordStepFinished records a driver's result for a running step, successful
// or not. It reports false, and changes nothing, if the step was not running in
// that run -- a duplicate, late or foreign result.
func (s *Store) RecordStepFinished(ctx context.Context, runID, stepID, status, errMsg string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE steps SET status = $1, error = $2, finished_at = now(), updated_at = now()
		 WHERE id = $3 AND run_id = $4 AND status = $5`,
		status, nullable(errMsg), stepID, runID, StepRunning)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// GetStep returns one step of a run.
func (s *Store) GetStep(ctx context.Context, runID, stepID string) (*Step, error) {
	return scanStep(s.db.QueryRowContext(ctx,
		`SELECT `+stepCols+` FROM steps WHERE id = $1 AND run_id = $2`, stepID, runID))
}

// RecordStepRetrying puts a running step whose attempt failed back to pending,
// keeping the failure in error, so it is dispatched again. finished_at stays
// unset: the step is not finished. Reports false if the step was not running.
func (s *Store) RecordStepRetrying(ctx context.Context, runID, stepID, errMsg string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE steps SET status = $1, error = $2, updated_at = now()
		 WHERE id = $3 AND run_id = $4 AND status = $5`,
		StepPending, nullable(errMsg), stepID, runID, StepRunning)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// ListInFlightSteps returns every running step of a run that is still being
// driven (running or failed_draining): the steps waiting on a driver's result.
func (s *Store) ListInFlightSteps(ctx context.Context) ([]Step, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+stepCols+` FROM steps
		  WHERE status = $1
		    AND run_id IN (SELECT id FROM runs WHERE status IN ($2, $3))
		  ORDER BY run_id, name`, StepRunning, RunRunning, RunFailedDraining)
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

// RecordStepLost fails a running step whose result never arrived. It applies
// only while the step is still on the same attempt: if the result came in, or
// a retry went out, since the step was judged lost, it reports false.
func (s *Store) RecordStepLost(ctx context.Context, runID, stepID string, attempt int, errMsg string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE steps SET status = $1, error = $2, finished_at = now(), updated_at = now()
		 WHERE id = $3 AND run_id = $4 AND status = $5 AND dispatch_count = $6`,
		StepFailed, nullable(errMsg), stepID, runID, StepRunning, attempt)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// RecordDispatchFailed fails a pending step whose command could not be
// delivered or answered. The driver may or may not have taken it.
func (s *Store) RecordDispatchFailed(ctx context.Context, stepID, errMsg string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE steps SET status = $1, error = $2, finished_at = now(), updated_at = now()
		 WHERE id = $3 AND status = $4`, StepFailed, nullable(errMsg), stepID, StepPending)
	return err
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func requireOneRow(res sql.Result, errIfNone error) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errIfNone
	}
	return nil
}
