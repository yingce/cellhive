package server

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"cellhive/internal/timer"
	"cellhive/internal/workflow"
)

func (s *Server) requireWorkflows(w http.ResponseWriter) bool {
	if s.Workflows == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_workflows", "workflow store not configured")
		return false
	}
	return true
}

func (s *Server) workflowParams(r *http.Request) (ns, name, id string) {
	q := r.URL.Query()
	return s.scopeNS(r), q.Get("workflow"), q.Get("id")
}

// startWorkflowRun claims a run lease (concurrency safety) and dispatches the
// attempt; it is a no-op when another run is active or the instance is terminal.
func (s *Server) startWorkflowRun(ctx context.Context, ns, name, id string) {
	if s.DispatchWorkflow == nil {
		return
	}
	token := ""
	if s.Workflows != nil {
		tok, _, _, ok, err := s.Workflows.ClaimRun(ctx, ns, name, id, 60000)
		if err != nil || !ok {
			return
		}
		token = tok
	}
	_ = s.DispatchWorkflow(ctx, ns, name, id, token)
}

// dispatchWorkflowAsync runs startWorkflowRun on a detached context: a request
// context is canceled as soon as the handler returns, which would abort the
// claim/dispatch HTTP call.
func (s *Server) dispatchWorkflowAsync(ns, name, id string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		s.startWorkflowRun(ctx, ns, name, id)
	}()
}

// capturedWorkflow runs a workflow-cell write with capture (RPO=0).
func (s *Server) capturedWorkflow(ctx context.Context, ns, name string, fn func() error) error {
	return s.capturedWrite(ctx, workflow.Scope(ns, name), fn)
}

// handleWorkflowCreate creates an instance and kicks off its first run.
// workflowGate claims or forwards a workflow-cell write (ADR-118).
func (s *Server) workflowGate(w http.ResponseWriter, r *http.Request, ns, name string) bool {
	if ns == "" || name == "" {
		return false
	}
	return s.forwardOrClaim(w, r, workflow.Scope(ns, name), nil)
}

func (s *Server) handleWorkflowCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflows(w) {
		return
	}
	ns, name, id := s.workflowParams(r)
	if s.workflowGate(w, r, ns, name) {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	var in workflow.Instance
	if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
		var e error
		in, e = s.Workflows.Create(r.Context(), ns, name, id, body)
		return e
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "workflow_create_failed", err.Error())
		return
	}
	s.dispatchWorkflowAsync(ns, name, in.ID)
	writeJSON(w, http.StatusOK, map[string]any{"id": in.ID, "status": in.Status})
}

// handleWorkflowGet returns instance state (status/output/error).
func (s *Server) handleWorkflowGet(w http.ResponseWriter, r *http.Request) {
	if ns, name, _ := s.workflowParams(r); ns != "" && name != "" && s.forwardRead(w, r, workflow.Scope(ns, name), nil) {
		return
	}
	if !s.requireWorkflows(w) {
		return
	}
	ns, name, id := s.workflowParams(r)
	in, err := s.Workflows.Get(r.Context(), ns, name, id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "workflow_not_found", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": in.ID, "status": in.Status, "error": in.Error,
		"output":     base64.StdEncoding.EncodeToString(in.Output),
		"created_ms": in.CreatedMs, "updated_ms": in.UpdatedMs,
	})
}

// handleWorkflowEvent records an event for an instance and re-runs it so the
// workflow can observe it.
func (s *Server) handleWorkflowEvent(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflows(w) {
		return
	}
	ns, name, id := s.workflowParams(r)
	if s.workflowGate(w, r, ns, name) {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
		return s.Workflows.AppendEvent(r.Context(), ns, name, id, body)
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "workflow_event_failed", err.Error())
		return
	}
	s.dispatchWorkflowAsync(ns, name, id)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleWorkflowLifecycle applies pause/resume/terminate/restart. resume and
// restart re-dispatch the instance.
func (s *Server) handleWorkflowLifecycle(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.requireWorkflows(w) {
			return
		}
		ns, name, id := s.workflowParams(r)
		if s.workflowGate(w, r, ns, name) {
			return
		}
		err := s.capturedWorkflow(r.Context(), ns, name, func() error {
			switch action {
			case "pause":
				return s.Workflows.Pause(r.Context(), ns, name, id)
			case "resume":
				return s.Workflows.Resume(r.Context(), ns, name, id)
			case "terminate":
				return s.Workflows.Terminate(r.Context(), ns, name, id)
			case "restart":
				return s.Workflows.Restart(r.Context(), ns, name, id)
			}
			return nil
		})
		if err != nil {
			writeErr(w, http.StatusBadRequest, "workflow_"+action+"_failed", err.Error())
			return
		}
		if action == "resume" || action == "restart" {
			s.dispatchWorkflowAsync(ns, name, id)
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "action": action})
	}
}

// handleWorkflowDelete removes an instance and all of its persisted state
// (Cloudflare instance.delete()); it is idempotent.
func (s *Server) handleWorkflowDelete(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflows(w) {
		return
	}
	ns, name, id := s.workflowParams(r)
	if s.workflowGate(w, r, ns, name) {
		return
	}
	deleted := false
	err := s.capturedWorkflow(r.Context(), ns, name, func() error {
		var e error
		deleted, e = s.Workflows.Delete(r.Context(), ns, name, id)
		return e
	})
	if err != nil {
		writeErr(w, http.StatusBadRequest, "workflow_delete_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": deleted})
}

// handleWorkflowList lists instances of a workflow definition.
func (s *Server) handleWorkflowList(w http.ResponseWriter, r *http.Request) {
	if ns, name, _ := s.workflowParams(r); ns != "" && name != "" && s.forwardRead(w, r, workflow.Scope(ns, name), nil) {
		return
	}
	if !s.requireWorkflows(w) {
		return
	}
	ns, name, _ := s.workflowParams(r)
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
	}
	instances, err := s.Workflows.List(r.Context(), ns, name, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "workflow_list_failed", err.Error())
		return
	}
	out := make([]map[string]any, 0, len(instances))
	for _, in := range instances {
		out = append(out, map[string]any{
			"id": in.ID, "status": in.Status, "error": in.Error,
			"output":     base64.StdEncoding.EncodeToString(in.Output),
			"created_ms": in.CreatedMs, "updated_ms": in.UpdatedMs,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"instances": out})
}

// handleWorkflowAttempt manages durable retry attempt state for a step.
func (s *Server) handleWorkflowAttempt(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		q := r.URL.Query()
		if q.Get("ns") != "" && q.Get("workflow") != "" &&
			s.forwardRead(w, r, workflow.Scope(q.Get("ns"), q.Get("workflow")), nil) {
			return
		}
	}
	if !s.requireWorkflows(w) {
		return
	}
	q := r.URL.Query()
	ns, name, id, step := q.Get("ns"), q.Get("workflow"), q.Get("id"), q.Get("name")
	if r.Method != http.MethodGet && s.workflowGate(w, r, ns, name) {
		return
	}
	if !s.fenceRun(w, r, ns, name, id) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		attempts, lastErr, ok, err := s.Workflows.GetAttempt(r.Context(), ns, name, id, step)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "workflow_attempt_failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"found": ok, "attempts": attempts, "error": lastErr})
	case http.MethodPut:
		attempts := 0
		if v := q.Get("attempts"); v != "" {
			attempts, _ = strconv.Atoi(v)
		}
		if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
			return s.Workflows.SetAttempt(r.Context(), ns, name, id, step, attempts, q.Get("error"))
		}); err != nil {
			writeErr(w, http.StatusInternalServerError, "workflow_attempt_failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodDelete:
		if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
			return s.Workflows.ClearAttempt(r.Context(), ns, name, id, step)
		}); err != nil {
			writeErr(w, http.StatusInternalServerError, "workflow_attempt_failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method", r.Method)
	}
}

// handleWorkflowState reports the instance status for a running attempt; a
// fenced (stale) token returns 409 so the run can stop cooperatively.
func (s *Server) handleWorkflowState(w http.ResponseWriter, r *http.Request) {
	if q := r.URL.Query(); q.Get("ns") != "" && q.Get("workflow") != "" &&
		s.forwardRead(w, r, workflow.Scope(q.Get("ns"), q.Get("workflow")), nil) {
		return
	}
	if !s.requireWorkflows(w) {
		return
	}
	q := r.URL.Query()
	ns, name, id := q.Get("ns"), q.Get("workflow"), q.Get("id")
	if s.workflowGate(w, r, ns, name) {
		return
	}
	if !s.fenceRun(w, r, ns, name, id) {
		return
	}
	st, err := s.Workflows.RunStatus(r.Context(), ns, name, id, q.Get("run"))
	if err != nil {
		writeErr(w, http.StatusConflict, "stale_run", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": st})
}

// fenceRun validates the caller's run token (when present) and renews the lease.
// A stale token fails closed 409 so a superseded run cannot mutate state.
func (s *Server) fenceRun(w http.ResponseWriter, r *http.Request, ns, name, id string) bool {
	token := r.URL.Query().Get("run")
	if token == "" {
		return true
	}
	if _, err := s.Workflows.RunStatus(r.Context(), ns, name, id, token); err != nil {
		if errors.Is(err, workflow.ErrStaleRun) {
			writeErr(w, http.StatusConflict, "stale_run", "run token is no longer current")
		} else {
			writeErr(w, http.StatusNotFound, "workflow_not_found", err.Error())
		}
		return false
	}
	if err := s.Workflows.RenewRun(r.Context(), ns, name, id, token, 60000); err != nil {
		if errors.Is(err, workflow.ErrStaleRun) {
			writeErr(w, http.StatusConflict, "stale_run", "run token is no longer current")
			return false
		}
	}
	return true
}

// handleWorkflowEventConsume returns and consumes the oldest matching event.
func (s *Server) handleWorkflowEventConsume(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflows(w) {
		return
	}
	q := r.URL.Query()
	if s.workflowGate(w, r, q.Get("ns"), q.Get("workflow")) {
		return
	}
	if !s.fenceRun(w, r, q.Get("ns"), q.Get("workflow"), q.Get("id")) {
		return
	}
	var payload []byte
	var found bool
	if err := s.capturedWorkflow(r.Context(), q.Get("ns"), q.Get("workflow"), func() error {
		var e error
		payload, found, e = s.Workflows.ConsumeEvent(r.Context(), q.Get("ns"), q.Get("workflow"), q.Get("id"), q.Get("type"))
		return e
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "workflow_consume_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"found": found, "event": base64.StdEncoding.EncodeToString(payload)})
}

// handleWorkflowWait manages waitForEvent deadlines (GET/POST/DELETE).
func (s *Server) handleWorkflowWait(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		q := r.URL.Query()
		if q.Get("ns") != "" && q.Get("workflow") != "" &&
			s.forwardRead(w, r, workflow.Scope(q.Get("ns"), q.Get("workflow")), nil) {
			return
		}
	}
	if !s.requireWorkflows(w) {
		return
	}
	q := r.URL.Query()
	ns, name, id, step := q.Get("ns"), q.Get("workflow"), q.Get("id"), q.Get("name")
	if r.Method != http.MethodGet && s.workflowGate(w, r, ns, name) {
		return
	}
	if r.Method != http.MethodGet && !s.fenceRun(w, r, ns, name, id) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		deadline, found, err := s.Workflows.GetWait(r.Context(), ns, name, id, step)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "workflow_wait_failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"found": found, "deadline_ms": deadline})
	case http.MethodPost:
		deadline := int64(0)
		if v := q.Get("deadline_ms"); v != "" {
			deadline, _ = strconv.ParseInt(v, 10, 64)
		}
		if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
			return s.Workflows.SetWait(r.Context(), ns, name, id, step, deadline)
		}); err != nil {
			writeErr(w, http.StatusInternalServerError, "workflow_wait_failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodDelete:
		if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
			return s.Workflows.ClearWait(r.Context(), ns, name, id, step)
		}); err != nil {
			writeErr(w, http.StatusInternalServerError, "workflow_wait_failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method", r.Method)
	}
}

// handleWorkflowStepGet returns a memoized step result (internal; called by the
// running workflow's step.do).
func (s *Server) handleWorkflowStepGet(w http.ResponseWriter, r *http.Request) {
	if q := r.URL.Query(); q.Get("ns") != "" && q.Get("workflow") != "" &&
		s.forwardRead(w, r, workflow.Scope(q.Get("ns"), q.Get("workflow")), nil) {
		return
	}
	if !s.requireWorkflows(w) {
		return
	}
	q := r.URL.Query()
	if !s.fenceRun(w, r, q.Get("ns"), q.Get("workflow"), q.Get("id")) {
		return
	}
	result, found, err := s.Workflows.GetStep(r.Context(), q.Get("ns"), q.Get("workflow"), q.Get("id"), q.Get("name"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "workflow_step_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"found": found, "result": base64.StdEncoding.EncodeToString(result),
	})
}

// handleWorkflowStepPut memoizes a step result (internal).
func (s *Server) handleWorkflowStepPut(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflows(w) {
		return
	}
	q := r.URL.Query()
	if s.workflowGate(w, r, q.Get("ns"), q.Get("workflow")) {
		return
	}
	if !s.fenceRun(w, r, q.Get("ns"), q.Get("workflow"), q.Get("id")) {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if err := s.capturedWorkflow(r.Context(), q.Get("ns"), q.Get("workflow"), func() error {
		return s.Workflows.PutStep(r.Context(), q.Get("ns"), q.Get("workflow"), q.Get("id"), q.Get("name"), body)
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "workflow_step_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleWorkflowSleep schedules a workflow wake-up timer (internal). The running
// workflow then aborts the current attempt and is re-dispatched when it fires.
func (s *Server) handleWorkflowSleep(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflows(w) {
		return
	}
	if s.OpenTimer == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_timers", "timer store not configured")
		return
	}
	q := r.URL.Query()
	ns, name, id := q.Get("ns"), q.Get("workflow"), q.Get("id")
	if !s.fenceRun(w, r, ns, name, id) {
		return
	}
	wakeAt := int64(0)
	if v := q.Get("wake_at_ms"); v != "" {
		wakeAt, _ = strconv.ParseInt(v, 10, 64)
	}
	if wakeAt == 0 {
		writeErr(w, http.StatusBadRequest, "bad_wake", "wake_at_ms is required")
		return
	}
	// Timer scope: the workflow definition's cell; occurrence = instance id.
	sc := workflow.Scope(ns, name)
	store, err := s.OpenTimer(r.Context(), sc)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "timer_open_failed", err.Error())
		return
	}
	t := timer.New(wakeAt, timer.KindWorkflowSleep, sc.String(), id)
	if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
		return store.Upsert(r.Context(), t)
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "timer_upsert_failed", err.Error())
		return
	}
	if s.Timers != nil {
		s.Timers.Add(sc.String())
	}
	_ = s.Workflows.SetStatus(r.Context(), ns, name, id, workflow.StatusSleeping, "")
	_ = s.Workflows.ReleaseRun(r.Context(), ns, name, id, q.Get("run"))
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "wake_at_ms": wakeAt})
}

// handleWorkflowFinish stores the terminal output (or error) of a run (internal).
func (s *Server) handleWorkflowFinish(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflows(w) {
		return
	}
	q := r.URL.Query()
	if s.workflowGate(w, r, q.Get("ns"), q.Get("workflow")) {
		return
	}
	if !s.fenceRun(w, r, q.Get("ns"), q.Get("workflow"), q.Get("id")) {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if errText := q.Get("error"); errText != "" {
		if err := s.capturedWorkflow(r.Context(), q.Get("ns"), q.Get("workflow"), func() error {
			return s.Workflows.SetStatus(r.Context(), q.Get("ns"), q.Get("workflow"), q.Get("id"), workflow.StatusErrored, errText)
		}); err != nil {
			writeErr(w, http.StatusInternalServerError, "workflow_finish_failed", err.Error())
			return
		}
		_ = s.Workflows.ReleaseRun(r.Context(), q.Get("ns"), q.Get("workflow"), q.Get("id"), q.Get("run"))
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": workflow.StatusErrored})
		return
	}
	if err := s.capturedWorkflow(r.Context(), q.Get("ns"), q.Get("workflow"), func() error {
		return s.Workflows.SetOutput(r.Context(), q.Get("ns"), q.Get("workflow"), q.Get("id"), body)
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "workflow_finish_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": workflow.StatusComplete})
}
