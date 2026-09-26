// A driver stands in for one piece of lab equipment.
//
// It does one thing at a time. Offered a step while it is already working, it
// refuses -- real instruments do not politely queue. Ask it for its state and it
// will tell you what it is doing and what it has done.
//
// You should not need to change this.
package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

type StepCommand struct {
	RunID    string `json:"run_id"`
	StepID   string `json:"step_id"`
	StepName string `json:"step_name"`
	DeviceID string `json:"device_id"`
}

type CommandAck struct {
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

type StepResult struct {
	RunID     string `json:"run_id"`
	StepID    string `json:"step_id"`
	StepName  string `json:"step_name"`
	DeviceID  string `json:"device_id"`
	Error     string `json:"error,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

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

const subjectResult = "steps.result"

type driver struct {
	id       string
	nc       *nats.Conn
	duration time.Duration
	dropPct  int
	failPct  int

	// Whether this instrument's errors are worth another attempt.
	failuresRetryable bool

	mu       sync.Mutex
	busy     bool
	current  string
	executed []string
	rejected int
	dropped  int
	failed   int
}

// handleCommand answers straight away, then does the work in the background.
func (d *driver) handleCommand(m *nats.Msg) {
	var cmd StepCommand
	if err := json.Unmarshal(m.Data, &cmd); err != nil {
		d.reply(m, CommandAck{Accepted: false, Reason: "malformed command"})
		return
	}

	d.mu.Lock()
	if d.busy {
		d.rejected++
		busyWith := d.current
		d.mu.Unlock()
		log.Printf("REFUSED %s: already running %s", cmd.StepName, busyWith)
		d.reply(m, CommandAck{Accepted: false, Reason: "busy with " + busyWith})
		return
	}
	d.busy = true
	d.current = cmd.StepName
	d.executed = append(d.executed, cmd.StepID)
	d.mu.Unlock()

	log.Printf("accepted %s (step=%s run=%s)", cmd.StepName, cmd.StepID, cmd.RunID)
	d.reply(m, CommandAck{Accepted: true})

	go d.execute(cmd)
}

func (d *driver) execute(cmd StepCommand) {
	time.Sleep(d.duration)

	d.mu.Lock()
	d.busy = false
	d.current = ""
	d.mu.Unlock()

	// Did the step itself go wrong? See FAIL_PCT.
	errMsg := ""
	retryable := false
	if d.failPct > 0 && rand.Intn(100) < d.failPct {
		errMsg = "instrument error during " + cmd.StepName
		retryable = d.failuresRetryable
		d.mu.Lock()
		d.failed++
		d.mu.Unlock()
		log.Printf("FAILED %s -- reporting an error (retryable=%t)", cmd.StepName, retryable)
	}

	// The instrument has finished, one way or the other. Whether anyone hears
	// about it is a separate question -- see DROP_RESULT_PCT.
	if d.dropPct > 0 && rand.Intn(100) < d.dropPct {
		d.mu.Lock()
		d.dropped++
		d.mu.Unlock()
		log.Printf("DROPPED the result for %s -- the work was done, the report was not sent", cmd.StepName)
		return
	}

	payload, err := json.Marshal(StepResult{
		RunID:     cmd.RunID,
		StepID:    cmd.StepID,
		StepName:  cmd.StepName,
		DeviceID:  cmd.DeviceID,
		Error:     errMsg,
		Retryable: retryable,
	})
	if err != nil {
		log.Printf("marshal result for %s: %v", cmd.StepName, err)
		return
	}
	if err := d.nc.Publish(subjectResult, payload); err != nil {
		log.Printf("publish result for %s: %v", cmd.StepName, err)
		return
	}
	if errMsg == "" {
		log.Printf("finished %s", cmd.StepName)
	}
}

func (d *driver) handleState(m *nats.Msg) {
	d.mu.Lock()
	st := DriverState{
		DeviceID:      d.id,
		Busy:          d.busy,
		CurrentStep:   d.current,
		Executed:      append([]string{}, d.executed...),
		Rejected:      d.rejected,
		DropResultPct: d.dropPct,
		Dropped:       d.dropped,
		FailPct:       d.failPct,
		Failed:        d.failed,
	}
	d.mu.Unlock()

	payload, err := json.Marshal(st)
	if err != nil {
		log.Printf("marshal state: %v", err)
		return
	}
	if err := m.Respond(payload); err != nil {
		log.Printf("respond state: %v", err)
	}
}

func (d *driver) reply(m *nats.Msg, ack CommandAck) {
	payload, err := json.Marshal(ack)
	if err != nil {
		log.Printf("marshal ack: %v", err)
		return
	}
	if err := m.Respond(payload); err != nil {
		log.Printf("respond ack: %v", err)
	}
}

func main() {
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		log.Fatal("WORKER_ID environment variable is required")
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		log.Fatal("NATS_URL environment variable is required")
	}

	duration := 2 * time.Second
	if v := os.Getenv("STEP_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			log.Fatalf("STEP_DURATION is not a duration: %v", err)
		}
		duration = d
	}

	pct := func(name string) int {
		v := os.Getenv(name)
		if v == "" {
			return 0
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 100 {
			log.Fatalf("%s must be a whole number from 0 to 100, got %q", name, v)
		}
		return n
	}
	dropPct := pct("DROP_RESULT_PCT")
	failPct := pct("FAIL_PCT")

	failuresRetryable := true
	if v := os.Getenv("FAILURES_RETRYABLE"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			log.Fatalf("FAILURES_RETRYABLE must be true or false, got %q", v)
		}
		failuresRetryable = b
	}

	nc, err := nats.Connect(natsURL, nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		log.Fatalf("Failed to connect to the bus: %v", err)
	}
	defer nc.Close()

	d := &driver{id: workerID, nc: nc, duration: duration, dropPct: dropPct, failPct: failPct,
		failuresRetryable: failuresRetryable, executed: []string{}}

	if _, err := nc.Subscribe("drivers."+workerID+".command", d.handleCommand); err != nil {
		log.Fatalf("Failed to subscribe to commands: %v", err)
	}
	if _, err := nc.Subscribe("drivers."+workerID+".state", d.handleState); err != nil {
		log.Fatalf("Failed to subscribe to state requests: %v", err)
	}

	switch {
	case dropPct > 0 && failPct > 0:
		log.Printf("driver %s ready (step duration %s, FAILING %d%%, DROPPING %d%% of results)", workerID, duration, failPct, dropPct)
	case dropPct > 0:
		log.Printf("driver %s ready (step duration %s, DROPPING %d%% of results)", workerID, duration, dropPct)
	case failPct > 0:
		log.Printf("driver %s ready (step duration %s, FAILING %d%% of steps)", workerID, duration, failPct)
	default:
		log.Printf("driver %s ready (step duration %s)", workerID, duration)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("driver %s shutting down", workerID)
}
