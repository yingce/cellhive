package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"cellhive/internal/workflow"
)

func cloudflareWorkflowInstance(in workflow.Instance) map[string]any {
	status := string(in.Status)
	if status == string(workflow.StatusSleeping) {
		status = "waiting"
	}
	out := map[string]any{"id": in.ID, "status": status, "created_on": time.UnixMilli(in.CreatedMs).UTC().Format(time.RFC3339Nano),
		"modified_on": time.UnixMilli(in.UpdatedMs).UTC().Format(time.RFC3339Nano), "versionId": strconv.Itoa(in.TargetVersion), "steps": []any{}}
	if in.Error != "" {
		out["error"] = map[string]any{"name": "WorkflowError", "message": in.Error}
	}
	if len(in.Output) != 0 {
		var output any
		if json.Unmarshal(in.Output, &output) == nil {
			out["output"] = output
		}
	}
	return out
}

func cloudflareCompletedSteps(steps []workflow.CompletedStep) []any {
	out := make([]any, 0, len(steps))
	for _, step := range steps {
		var value any
		if json.Unmarshal(step.Result, &value) != nil {
			value = string(step.Result)
		}
		out = append(out, map[string]any{"name": step.Name, "type": "step", "success": true, "output": value, "attempts": []any{}})
	}
	return out
}

func workflowInstanceQuery(r *http.Request) url.Values {
	return url.Values{"workflow": {r.PathValue("workflow_name")}, "id": {r.PathValue("instance_id")}}
}

func (s *Server) handleCloudflareWorkflowInstanceCreate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("workflow_name")
	if target := s.workflowTarget(r.Context(), r.PathValue("account_id"), name); target.Worker == "" {
		writeErr(w, 400, "workflow_unbound", "workflow has no deployed Worker binding")
		return
	}
	var in struct {
		InstanceID string          `json:"instance_id"`
		Params     json.RawMessage `json:"params"`
	}
	if !decodeLimit(w, r, &in, 1<<20) {
		return
	}
	if len(in.Params) != 0 && !json.Valid(in.Params) {
		writeErr(w, 400, "invalid_params", "params must be JSON")
		return
	}
	rec, err := s.cloudflareBindingCall(r, "workflow", name, http.MethodPost, "/v1/workflow/create", url.Values{"workflow": {name}, "id": {in.InstanceID}}, in.Params)
	if cloudflareBridgeError(w, rec, err) {
		return
	}
	var result map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &result) != nil {
		writeErr(w, 502, "invalid_workflow_response", "invalid create response")
		return
	}
	cloudflareResult(w, result)
}

func (s *Server) handleCloudflareWorkflowInstanceList(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("workflow_name")
	for key := range r.URL.Query() {
		switch key {
		case "per_page", "page", "status", "date_start", "date_end":
		default:
			writeErr(w, 400, "unsupported_filter", "unsupported workflow list query: "+key)
			return
		}
	}
	limit := 100
	if raw := r.URL.Query().Get("per_page"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			writeErr(w, 400, "invalid_page", "per_page must be 1..100")
			return
		}
		limit = n
	}
	page := 1
	if raw := r.URL.Query().Get("page"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 10000 {
			writeErr(w, 400, "invalid_page", "page must be 1..10000")
			return
		}
		page = n
	}
	q := url.Values{"workflow": {name}, "limit": {strconv.Itoa(limit)}, "offset": {strconv.Itoa((page - 1) * limit)}}
	if raw := r.URL.Query().Get("status"); raw != "" {
		switch raw {
		case "queued", "running", "paused", "errored", "terminated", "complete", "waiting":
		default:
			writeErr(w, 400, "invalid_status", "unsupported workflow status")
			return
		}
		if raw == "waiting" {
			raw = string(workflow.StatusSleeping)
		}
		q.Set("status", raw)
	}
	for _, pair := range [][2]string{{"date_start", "from_ms"}, {"date_end", "to_ms"}} {
		if raw := r.URL.Query().Get(pair[0]); raw != "" {
			t, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				writeErr(w, 400, "invalid_date", pair[0]+" must be RFC3339")
				return
			}
			q.Set(pair[1], strconv.FormatInt(t.UnixMilli(), 10))
		}
	}
	rec, err := s.cloudflareBindingCall(r, "workflow", name, http.MethodGet, "/v1/workflow/list", q, nil)
	if cloudflareBridgeError(w, rec, err) {
		return
	}
	var data struct {
		Instances []workflow.Instance `json:"instances"`
	}
	// The native list response serializes selected fields, so read it through a
	// shape that retains status and timestamps before mapping to CF dates.
	if json.Unmarshal(rec.Body.Bytes(), &data) != nil {
		writeErr(w, 502, "invalid_workflow_response", "invalid list response")
		return
	}
	out := make([]any, 0, len(data.Instances))
	for _, in := range data.Instances {
		out = append(out, cloudflareWorkflowInstance(in))
	}
	cloudflareResult(w, out)
}

func (s *Server) handleCloudflareWorkflowInstanceGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("workflow_name")
	rec, err := s.cloudflareBindingCall(r, "workflow", name, http.MethodGet, "/v1/workflow/get", workflowInstanceQuery(r), nil)
	if cloudflareBridgeError(w, rec, err) {
		return
	}
	var response struct {
		workflow.Instance
		Steps []workflow.CompletedStep `json:"steps"`
	}
	if json.Unmarshal(rec.Body.Bytes(), &response) != nil {
		writeErr(w, 502, "invalid_workflow_response", "invalid instance response")
		return
	}
	out := cloudflareWorkflowInstance(response.Instance)
	out["steps"] = cloudflareCompletedSteps(response.Steps)
	cloudflareResult(w, out)
}

func (s *Server) handleCloudflareWorkflowInstanceStatus(w http.ResponseWriter, r *http.Request) {
	var fields map[string]json.RawMessage
	if !decodeLimit(w, r, &fields, 1<<20) {
		return
	}
	for key := range fields {
		if key != "status" && key != "from" {
			writeErr(w, 400, "unsupported_status_option", "unsupported status option: "+key)
			return
		}
	}
	if raw := fields["from"]; len(raw) != 0 && string(raw) != "null" {
		writeErr(w, 400, "unsupported_restart_from", "step-specific restart is not supported")
		return
	}
	var action string
	if err := json.Unmarshal(fields["status"], &action); err != nil {
		writeErr(w, 400, "invalid_status", "status must be a string")
		return
	}
	if action != "pause" && action != "resume" && action != "terminate" && action != "restart" {
		writeErr(w, 400, "invalid_status", "status must be pause, resume, terminate, or restart")
		return
	}
	name := r.PathValue("workflow_name")
	rec, err := s.cloudflareBindingCall(r, "workflow", name, http.MethodPost, "/v1/workflow/"+action, workflowInstanceQuery(r), nil)
	if cloudflareBridgeError(w, rec, err) {
		return
	}
	cloudflareResult(w, map[string]any{"id": r.PathValue("instance_id"), "status": action})
}

func (s *Server) handleCloudflareWorkflowInstanceEvent(w http.ResponseWriter, r *http.Request) {
	var payload json.RawMessage
	if !decodeLimit(w, r, &payload, 1<<20) || !json.Valid(payload) {
		return
	}
	name := r.PathValue("workflow_name")
	body, _ := json.Marshal(map[string]any{"type": r.PathValue("event_type"), "payload": payload})
	rec, err := s.cloudflareBindingCall(r, "workflow", name, http.MethodPost, "/v1/workflow/event", workflowInstanceQuery(r), body)
	if cloudflareBridgeError(w, rec, err) {
		return
	}
	cloudflareResult(w, map[string]any{"id": r.PathValue("instance_id"), "type": r.PathValue("event_type")})
}

func (s *Server) handleCloudflareWorkflowInstanceDeleteBatch(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Instances []string `json:"instances"`
	}
	if !decodeLimit(w, r, &in, 1<<20) {
		return
	}
	if len(in.Instances) == 0 || len(in.Instances) > 100 {
		writeErr(w, 400, "invalid_batch", "delete requires 1..100 instance IDs")
		return
	}
	name := r.PathValue("workflow_name")
	for _, id := range in.Instances {
		if id == "" {
			writeErr(w, 400, "invalid_instance", "instance ID is required")
			return
		}
	}
	deleted := make([]map[string]string, 0, len(in.Instances))
	for _, id := range in.Instances {
		rec, err := s.cloudflareBindingCall(r, "workflow", name, http.MethodPost, "/v1/workflow/delete", url.Values{"workflow": {name}, "id": {id}}, nil)
		if cloudflareBridgeError(w, rec, err) {
			return
		}
		deleted = append(deleted, map[string]string{"id": id})
	}
	cloudflareResult(w, map[string]any{"deleted": deleted, "errors": []any{}})
}
