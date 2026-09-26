package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

type Handlers struct {
	store         *Store
	sched         *Scheduler
	workflowsPath string
}

func NewHandlers(store *Store, sched *Scheduler, workflowsPath string) *Handlers {
	return &Handlers{store: store, sched: sched, workflowsPath: workflowsPath}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (h *Handlers) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func (h *Handlers) listDevices(w http.ResponseWriter, r *http.Request) {
	devices, err := h.store.ListDevices(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, devices)
}

// listDriverStates asks every driver what it is doing. Useful for checking your
// work: each driver reports the steps it actually executed and how many commands
// it refused.
func (h *Handlers) listDriverStates(w http.ResponseWriter, r *http.Request) {
	devices, err := h.store.ListDevices(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	out := []any{}
	for _, d := range devices {
		st, err := h.sched.bus.DriverState(r.Context(), d.ID)
		if err != nil {
			out = append(out, map[string]any{"device_id": d.ID, "error": err.Error()})
			continue
		}
		out = append(out, st)
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handlers) listRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := h.store.ListRuns(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (h *Handlers) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := h.store.GetRun(r.Context(), r.PathValue("id"))
	if errors.Is(err, ErrRunNotFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	steps, err := h.store.ListSteps(r.Context(), run.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": run, "steps": steps})
}

type createRunRequest struct {
	WorkflowName string `json:"workflow_name"`
}

func (h *Handlers) createRun(w http.ResponseWriter, r *http.Request) {
	var req createRunRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid body")
			return
		}
	}

	// Read the definitions fresh so edits to workflows.yaml take effect without
	// a restart.
	set, err := LoadWorkflows(h.workflowsPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	name := req.WorkflowName
	if name == "" {
		name = set.Default()
	}
	tmpl, err := set.Get(name)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	run, err := h.store.CreateRun(r.Context(), name, tmpl)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, run)
}

// listWorkflows reports what is currently defined in workflows.yaml.
func (h *Handlers) listWorkflows(w http.ResponseWriter, r *http.Request) {
	set, err := LoadWorkflows(h.workflowsPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	out := []map[string]any{}
	for _, name := range set.Names() {
		steps, err := set.Get(name)
		if err != nil {
			continue
		}
		names := make([]string, 0, len(steps))
		for _, st := range steps {
			names = append(names, st.Name)
		}
		out = append(out, map[string]any{
			"name":       name,
			"is_default": name == set.Default(),
			"step_count": len(steps),
			"steps":      names,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handlers) startRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.store.GetRun(r.Context(), id); errors.Is(err, ErrRunNotFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if err := h.sched.Start(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "run started", "run_id": id})
}

type timelineEntry struct {
	Name          string     `json:"name"`
	DeviceID      string     `json:"device_id"`
	Status        string     `json:"status"`
	DependsOn     []string   `json:"depends_on"`
	DispatchCount int        `json:"dispatch_count"`
	StartOffsetMS *int64     `json:"start_offset_ms"`
	EndOffsetMS   *int64     `json:"end_offset_ms"`
	DurationMS    *int64     `json:"duration_ms"`
	DispatchedAt  *time.Time `json:"dispatched_at"`
	FinishedAt    *time.Time `json:"finished_at"`
}

// timeline shows when each step actually occupied its device, relative to the
// start of the run.
func (h *Handlers) timeline(w http.ResponseWriter, r *http.Request) {
	run, err := h.store.GetRun(r.Context(), r.PathValue("id"))
	if errors.Is(err, ErrRunNotFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	steps, err := h.store.ListSteps(r.Context(), run.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	origin := run.CreatedAt
	if run.StartedAt != nil {
		origin = *run.StartedAt
	}

	offset := func(t *time.Time) *int64 {
		if t == nil {
			return nil
		}
		ms := t.Sub(origin).Milliseconds()
		return &ms
	}

	entries := make([]timelineEntry, 0, len(steps))
	for _, st := range steps {
		e := timelineEntry{
			Name:          st.Name,
			DeviceID:      st.DeviceID,
			Status:        st.Status,
			DependsOn:     st.DependsOn,
			DispatchCount: st.DispatchCount,
			StartOffsetMS: offset(st.DispatchedAt),
			EndOffsetMS:   offset(st.FinishedAt),
			DispatchedAt:  st.DispatchedAt,
			FinishedAt:    st.FinishedAt,
		}
		if st.DispatchedAt != nil && st.FinishedAt != nil {
			d := st.FinishedAt.Sub(*st.DispatchedAt).Milliseconds()
			e.DurationMS = &d
		}
		entries = append(entries, e)
	}

	var totalMS *int64
	if run.FinishedAt != nil {
		ms := run.FinishedAt.Sub(origin).Milliseconds()
		totalMS = &ms
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"run_id":            run.ID,
		"status":            run.Status,
		"total_duration_ms": totalMS,
		"steps":             entries,
	})
}
