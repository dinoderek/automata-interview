package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		log.Fatal("NATS_URL environment variable is required")
	}
	workflowsPath := os.Getenv("WORKFLOWS_FILE")
	if workflowsPath == "" {
		workflowsPath = "/app/config/workflows.yaml"
	}

	// Fail fast if the definitions are broken, rather than at the first run.
	set, err := LoadWorkflows(workflowsPath)
	if err != nil {
		log.Fatalf("Failed to load workflows: %v", err)
	}
	log.Printf("loaded %d workflow(s) from %s, default %q", len(set.Names()), workflowsPath, set.Default())

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	bus, err := NewNATSBus(natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to the bus: %v", err)
	}
	defer bus.Close()

	store := NewStore(db)
	sched := NewScheduler(store, bus)

	if err := bus.OnStepResult(func(ctx context.Context, res StepResult) {
		sched.HandleResult(ctx, res)
	}); err != nil {
		log.Fatalf("Failed to subscribe to step results: %v", err)
	}

	// Results can be lost; the reconcile loop notices steps whose result never
	// came. See reconciler.go.
	go sched.RunReconciler(context.Background())

	h := NewHandlers(store, sched, workflowsPath)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /devices", h.listDevices)
	mux.HandleFunc("GET /workflows", h.listWorkflows)
	mux.HandleFunc("GET /drivers", h.listDriverStates)
	mux.HandleFunc("GET /runs", h.listRuns)
	mux.HandleFunc("POST /runs", h.createRun)
	mux.HandleFunc("GET /runs/{id}", h.getRun)
	mux.HandleFunc("POST /runs/{id}/start", h.startRun)
	mux.HandleFunc("GET /runs/{id}/timeline", h.timeline)

	log.Println("Starting executor on :5001")
	log.Fatal(http.ListenAndServe(":5001", mux))
}
