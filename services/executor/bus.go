package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// StepCommand tells a driver to run one step. Sending it is a request: the
// driver answers immediately with a CommandAck saying whether it took the work.
// The step finishing is reported separately, later, as a StepResult.
type StepCommand struct {
	RunID    string `json:"run_id"`
	StepID   string `json:"step_id"`
	StepName string `json:"step_name"`
	DeviceID string `json:"device_id"`
}

// CommandAck is the driver's immediate answer to a StepCommand.
// A driver that is already working refuses the command.
type CommandAck struct {
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

// StepResult is a driver reporting that it has finished. Error is empty on
// success. Retryable is the driver's own view of whether the same step could
// sensibly be sent again; it only means anything when Error is set.
type StepResult struct {
	RunID     string `json:"run_id"`
	StepID    string `json:"step_id"`
	StepName  string `json:"step_name"`
	DeviceID  string `json:"device_id"`
	Error     string `json:"error,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

// DriverState is what a driver says about itself.
type DriverState struct {
	DeviceID      string   `json:"device_id"`
	Busy          bool     `json:"busy"`
	CurrentStep   string   `json:"current_step,omitempty"`
	Executed      []string `json:"executed"`
	Rejected      int      `json:"rejected"`
	DropResultPct int      `json:"drop_result_pct"`
	Dropped       int      `json:"dropped"`
	FailPct       int      `json:"fail_pct"`
	Failed        int      `json:"failed"`
}

// Bus is how the executor talks to the drivers.
type Bus interface {
	// SendCommand offers a step to a driver and waits for its immediate answer.
	// A driver that is busy returns Accepted: false. This does NOT wait for the
	// step to finish.
	SendCommand(ctx context.Context, cmd StepCommand) (CommandAck, error)

	// OnStepResult registers a handler for drivers reporting completion.
	// Handlers may be called concurrently.
	OnStepResult(handler func(context.Context, StepResult)) error

	// DriverState asks a driver what it is doing. Not required to build a
	// working executor -- it is there if you want it.
	DriverState(ctx context.Context, deviceID string) (DriverState, error)

	Close()
}

const (
	subjectResult  = "steps.result"
	commandTimeout = 3 * time.Second
)

func commandSubject(deviceID string) string { return "drivers." + deviceID + ".command" }
func stateSubject(deviceID string) string   { return "drivers." + deviceID + ".state" }

type natsBus struct {
	nc *nats.Conn
}

func NewNATSBus(url string) (Bus, error) {
	nc, err := nats.Connect(url, nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	return &natsBus{nc: nc}, nil
}

func (b *natsBus) SendCommand(ctx context.Context, cmd StepCommand) (CommandAck, error) {
	payload, err := json.Marshal(cmd)
	if err != nil {
		return CommandAck{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()

	reply, err := b.nc.RequestWithContext(ctx, commandSubject(cmd.DeviceID), payload)
	if err != nil {
		return CommandAck{}, fmt.Errorf("command %s to %s: %w", cmd.StepName, cmd.DeviceID, err)
	}

	var ack CommandAck
	if err := json.Unmarshal(reply.Data, &ack); err != nil {
		return CommandAck{}, fmt.Errorf("bad ack from %s: %w", cmd.DeviceID, err)
	}
	return ack, nil
}

func (b *natsBus) OnStepResult(handler func(context.Context, StepResult)) error {
	_, err := b.nc.Subscribe(subjectResult, func(m *nats.Msg) {
		var res StepResult
		if err := json.Unmarshal(m.Data, &res); err != nil {
			log.Printf("bus: bad result payload: %v", err)
			return
		}
		// Each result is handed to its own goroutine so that one slow handler
		// cannot hold up the rest of the bus. This is a choice, and it means
		// your handler can be called concurrently.
		//
		// If you would rather results arrived one at a time, this is the line to
		// change. Either is defensible -- tell us which you picked and why.
		go handler(context.Background(), res)
	})
	return err
}

func (b *natsBus) DriverState(ctx context.Context, deviceID string) (DriverState, error) {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()

	reply, err := b.nc.RequestWithContext(ctx, stateSubject(deviceID), nil)
	if err != nil {
		return DriverState{}, fmt.Errorf("state of %s: %w", deviceID, err)
	}
	var st DriverState
	if err := json.Unmarshal(reply.Data, &st); err != nil {
		return DriverState{}, fmt.Errorf("bad state from %s: %w", deviceID, err)
	}
	return st, nil
}

func (b *natsBus) Close() { b.nc.Close() }
