package main

import "time"

const (
	RunPending   = "pending"
	RunRunning   = "running"
	RunCompleted = "completed"
	RunFailed    = "failed"
	RunAborted   = "aborted"
)

const (
	StepPending    = "pending"
	StepDispatched = "dispatched"
	StepRunning    = "running"
	StepCompleted  = "completed"
	StepFailed     = "failed"
)

type Device struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type Run struct {
	ID           string     `json:"id"`
	WorkflowName string     `json:"workflow_name"`
	Status       string     `json:"status"`
	CreatedAt    time.Time  `json:"created_at"`
	StartedAt    *time.Time `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at"`
}

type Step struct {
	ID            string     `json:"id"`
	RunID         string     `json:"run_id"`
	Name          string     `json:"name"`
	DeviceID      string     `json:"device_id"`
	Status        string     `json:"status"`
	DependsOn     []string   `json:"depends_on"`
	DispatchCount int        `json:"dispatch_count"`
	DispatchedAt  *time.Time `json:"dispatched_at"`
	FinishedAt    *time.Time `json:"finished_at"`
	Error         *string    `json:"error,omitempty"`
}
