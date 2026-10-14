package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"cellhive/internal/control"
)

func (s *Server) workflowResult(r *http.Request, name string) (map[string]any, error) {
	ns := r.PathValue("account_id")
	res, err := s.Control.GetResource(r.Context(), ns, "workflow", name)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"name": res.Name, "id": res.Name, "created_on": res.CreatedMs, "modified_on": res.CreatedMs}
	p, err := s.Control.Projection(r.Context())
	if err == nil {
		for _, app := range p.Apps {
			if app.Namespace != ns {
				continue
			}
			for _, worker := range app.Workers {
				for _, b := range worker.Version.Bindings {
					if b.Type == "workflow" && b.ID == name {
						result["script_name"] = worker.Worker
						result["class_name"] = b.ClassName
					}
				}
			}
		}
	}
	return result, nil
}

func (s *Server) handleCloudflareWorkflowGet(w http.ResponseWriter, r *http.Request) {
	result, err := s.workflowResult(r, r.PathValue("workflow_name"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "workflow_not_found", err.Error())
		return
	}
	cloudflareResult(w, result)
}

func (s *Server) handleCloudflareWorkflowList(w http.ResponseWriter, r *http.Request) {
	items, err := s.Control.Resources(r.Context(), r.PathValue("account_id"), "workflow")
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "workflows_failed", err.Error())
		return
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		result, err := s.workflowResult(r, item.Name)
		if err == nil {
			out = append(out, result)
		}
	}
	cloudflareResult(w, out)
}

func (s *Server) handleCloudflareWorkflowPut(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ScriptName       string          `json:"script_name"`
		ClassName        string          `json:"class_name"`
		Limits           json.RawMessage `json:"limits"`
		Concurrency      json.RawMessage `json:"concurrency"`
		Schedules        json.RawMessage `json:"schedules"`
		DefaultRetention json.RawMessage `json:"default_retention"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil || in.ScriptName == "" || in.ClassName == "" {
		writeErr(w, http.StatusBadRequest, "invalid_workflow", "script_name and class_name are required")
		return
	}
	if len(in.Limits) > 0 || len(in.Concurrency) > 0 || len(in.Schedules) > 0 || len(in.DefaultRetention) > 0 {
		writeErr(w, http.StatusBadRequest, "unsupported_workflow_settings", "workflow limits, concurrency, schedules and retention are not supported")
		return
	}
	ns, name := r.PathValue("account_id"), r.PathValue("workflow_name")
	_, err := s.Control.CreateResource(r.Context(), ns, "workflow", name, defaultResourceScope(ns, "workflow", name), "wrangler")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "workflow_put_failed", err.Error())
		return
	}
	result, err := s.workflowResult(r, name)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "workflow_get_failed", err.Error())
		return
	}
	result["script_name"] = in.ScriptName
	result["class_name"] = in.ClassName
	cloudflareResult(w, result)
}

func (s *Server) handleCloudflareWorkflowDelete(w http.ResponseWriter, r *http.Request) {
	ns, name := r.PathValue("account_id"), r.PathValue("workflow_name")
	if err := s.Control.DeleteResource(r.Context(), ns, "workflow", name, "wrangler"); err != nil && !errors.Is(err, control.ErrNotFound) {
		writeErr(w, http.StatusBadRequest, "workflow_delete_failed", err.Error())
		return
	}
	cloudflareResult(w, nil)
}
