package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"cellhive/internal/workflow"
)

const maxWorkflowPayloadBytes = 1 << 20

const maxWorkflowResultBytes = 8 << 20

// The wire body is base64; cap the decoded value without truncating the wire
// body into another valid base64 value that could be committed as a result.
func readWorkflowResult(w http.ResponseWriter, r *http.Request, badCode string) ([]byte, bool) {
	limit := base64.StdEncoding.EncodedLen(maxWorkflowResultBytes)
	encoded, err := io.ReadAll(io.LimitReader(r.Body, int64(limit+1)))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return nil, false
	}
	if len(encoded) > limit {
		writeErr(w, http.StatusRequestEntityTooLarge, "workflow_result_too_large", "workflow result exceeds 8 MiB")
		return nil, false
	}
	body, err := base64.StdEncoding.DecodeString(string(encoded))
	if err != nil {
		writeErr(w, http.StatusBadRequest, badCode, err.Error())
		return nil, false
	}
	if len(body) > maxWorkflowResultBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "workflow_result_too_large", "workflow result exceeds 8 MiB")
		return nil, false
	}
	return body, true
}

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

func (s *Server) workflowTarget(ctx context.Context, ns, name string) workflow.Target {
	if s.Control == nil {
		return workflow.Target{}
	}
	proj, err := s.Control.Projection(ctx)
	if err != nil {
		return workflow.Target{}
	}
	t, ok := proj.WorkflowTargetFor(ns, name)
	if !ok {
		return workflow.Target{}
	}
	return workflow.Target{Worker: t.Worker, BundleSHA: t.BundleSHA, Version: t.Version, ClassName: t.ClassName}
}

// startWorkflowRun claims a run lease (concurrency safety) and dispatches the
// attempt; it is a no-op when another run is active or the instance is terminal.
func (s *Server) startWorkflowRun(ctx context.Context, ns, name, id string) {
	if s.DispatchWorkflow == nil {
		return
	}
	token := ""
	if s.Workflows != nil {
		limit := 0
		if s.Control != nil {
			if raw, err := s.Control.ResourceConfig(ctx, ns, "workflow", name); err == nil {
				var policy workflow.Policy
				if json.Unmarshal(raw, &policy) == nil {
					limit = policy.Concurrency
				}
			}
		}
		tok, _, _, ok, err := s.Workflows.ClaimRunLimited(ctx, ns, name, id, 60000, limit)
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
	body, err := io.ReadAll(io.LimitReader(r.Body, maxWorkflowPayloadBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(body) > maxWorkflowPayloadBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "workflow_payload_too_large", "workflow payload exceeds 1 MiB")
		return
	}
	var in workflow.Instance
	if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
		var e error
		in, e = s.Workflows.CreateWithTarget(r.Context(), ns, name, id, body, s.workflowTarget(r.Context(), ns, name))
		return e
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "workflow_create_failed", err.Error())
		return
	}
	s.dispatchWorkflowAsync(ns, name, in.ID)
	writeJSON(w, http.StatusOK, map[string]any{"id": in.ID, "status": in.Status})
}

type workflowBatchItem struct {
	ID     string          `json:"id"`
	Params json.RawMessage `json:"params"`
}

func (s *Server) handleWorkflowCreateBatch(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflows(w) {
		return
	}
	ns, name, _ := s.workflowParams(r)
	if s.workflowGate(w, r, ns, name) {
		return
	}
	var items []workflowBatchItem
	if !decodeLimit(w, r, &items, maxWorkflowPayloadBytes+1) {
		return
	}
	if len(items) == 0 || len(items) > 100 {
		writeErr(w, http.StatusBadRequest, "workflow_batch_size", "workflow batch must contain 1..100 instances")
		return
	}
	batch := make([]workflow.Instance, 0, len(items))
	for _, item := range items {
		batch = append(batch, workflow.Instance{ID: item.ID, Params: item.Params})
	}
	var instances []workflow.Instance
	if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
		var err error
		instances, err = s.Workflows.CreateBatchWithTarget(r.Context(), ns, name, batch, s.workflowTarget(r.Context(), ns, name))
		return err
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "workflow_create_batch_failed", err.Error())
		return
	}
	for _, in := range instances {
		s.dispatchWorkflowAsync(ns, name, in.ID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"instances": instances})
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
	steps, err := s.Workflows.CompletedSteps(r.Context(), ns, name, id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "workflow_steps_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": in.ID, "status": in.Status, "error": in.Error,
		"output":     base64.StdEncoding.EncodeToString(in.Output),
		"created_ms": in.CreatedMs, "updated_ms": in.UpdatedMs,
		"target_version": in.TargetVersion, "steps": steps,
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
	offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
	if r.URL.Query().Get("offset") == "" {
		offset = 0
		err = nil
	}
	if err != nil || offset < 0 {
		writeErr(w, 400, "invalid_offset", "offset must be nonnegative")
		return
	}
	fromMs, err := strconv.ParseInt(r.URL.Query().Get("from_ms"), 10, 64)
	if r.URL.Query().Get("from_ms") == "" {
		fromMs = 0
		err = nil
	}
	if err != nil {
		writeErr(w, 400, "invalid_date", "from_ms must be milliseconds")
		return
	}
	toMs, err := strconv.ParseInt(r.URL.Query().Get("to_ms"), 10, 64)
	if r.URL.Query().Get("to_ms") == "" {
		toMs = 0
		err = nil
	}
	if err != nil {
		writeErr(w, 400, "invalid_date", "to_ms must be milliseconds")
		return
	}
	instances, err := s.Workflows.ListPage(r.Context(), ns, name, limit, offset, workflow.Status(r.URL.Query().Get("status")), fromMs, toMs)
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
			writeWorkflowError(w, "workflow_attempt_failed", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"found": ok, "attempts": attempts, "error": lastErr})
	case http.MethodPut:
		attempts := 0
		if v := q.Get("attempts"); v != "" {
			attempts, _ = strconv.Atoi(v)
		}
		if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
			return s.Workflows.SetAttempt(r.Context(), ns, name, id, step, attempts, q.Get("error"), q.Get("run"))
		}); err != nil {
			writeWorkflowError(w, "workflow_attempt_failed", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodDelete:
		if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
			return s.Workflows.ClearAttempt(r.Context(), ns, name, id, step, q.Get("run"))
		}); err != nil {
			writeWorkflowError(w, "workflow_attempt_failed", err)
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
		writeWorkflowError(w, "workflow_renew_failed", err)
		return false
	}
	return true
}

func writeWorkflowError(w http.ResponseWriter, code string, err error) {
	if errors.Is(err, workflow.ErrStaleRun) {
		writeErr(w, http.StatusConflict, "stale_run", "run token is no longer current")
		return
	}
	writeErr(w, http.StatusInternalServerError, code, err.Error())
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
	if q.Get("run") != "" && q.Get("name") == "" {
		writeErr(w, http.StatusBadRequest, "missing_step", "event consumption requires a step name")
		return
	}
	var payload []byte
	var found bool
	if err := s.capturedWorkflow(r.Context(), q.Get("ns"), q.Get("workflow"), func() error {
		var e error
		payload, found, e = s.Workflows.ConsumeEventForStep(r.Context(), q.Get("ns"), q.Get("workflow"), q.Get("id"), q.Get("name"), q.Get("type"), q.Get("run"))
		return e
	}); err != nil {
		writeWorkflowError(w, "workflow_consume_failed", err)
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
			writeWorkflowError(w, "workflow_wait_failed", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"found": found, "deadline_ms": deadline})
	case http.MethodPost:
		deadline := int64(0)
		if v := q.Get("deadline_ms"); v != "" {
			deadline, _ = strconv.ParseInt(v, 10, 64)
		}
		if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
			return s.Workflows.ParkWait(r.Context(), ns, name, id, step, q.Get("run"), q.Get("type"), deadline)
		}); err != nil {
			writeWorkflowError(w, "workflow_wait_failed", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case http.MethodDelete:
		if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
			return s.Workflows.ClearWait(r.Context(), ns, name, id, step, q.Get("run"))
		}); err != nil {
			writeWorkflowError(w, "workflow_wait_failed", err)
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
		writeWorkflowError(w, "workflow_step_failed", err)
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
	body, ok := readWorkflowResult(w, r, "bad_step")
	if !ok {
		return
	}
	if err := s.capturedWorkflow(r.Context(), q.Get("ns"), q.Get("workflow"), func() error {
		return s.Workflows.PutStep(r.Context(), q.Get("ns"), q.Get("workflow"), q.Get("id"), q.Get("name"), body, q.Get("run"))
	}); err != nil {
		writeWorkflowError(w, "workflow_step_failed", err)
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
	if s.workflowGate(w, r, ns, name) {
		return
	}
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
	if err := s.capturedWorkflow(r.Context(), ns, name, func() error {
		return s.Workflows.ParkSleep(r.Context(), ns, name, id, q.Get("run"), wakeAt)
	}); err != nil {
		writeWorkflowError(w, "timer_upsert_failed", err)
		return
	}
	if s.Timers != nil {
		s.Timers.Add(sc.String())
	}
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
	body, ok := readWorkflowResult(w, r, "bad_output")
	if !ok {
		return
	}
	if errText := q.Get("error"); errText != "" {
		if err := s.capturedWorkflow(r.Context(), q.Get("ns"), q.Get("workflow"), func() error {
			return s.Workflows.SetStatus(r.Context(), q.Get("ns"), q.Get("workflow"), q.Get("id"), workflow.StatusErrored, errText, q.Get("run"))
		}); err != nil {
			writeWorkflowError(w, "workflow_finish_failed", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": workflow.StatusErrored})
		return
	}
	if err := s.capturedWorkflow(r.Context(), q.Get("ns"), q.Get("workflow"), func() error {
		return s.Workflows.SetOutput(r.Context(), q.Get("ns"), q.Get("workflow"), q.Get("id"), body, q.Get("run"))
	}); err != nil {
		writeWorkflowError(w, "workflow_finish_failed", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": workflow.StatusComplete})
}
