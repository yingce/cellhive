package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/workflow"
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
	if len(in.Limits) > 0 {
		writeErr(w, http.StatusBadRequest, "unsupported_workflow_settings", "workflow limits are not supported")
		return
	}
	var policy workflow.Policy
	if len(in.Concurrency) > 0 {
		var cfg struct {
			Limit int `json:"limit"`
		}
		if err := json.Unmarshal(in.Concurrency, &cfg); err != nil || cfg.Limit < 1 {
			writeErr(w, http.StatusBadRequest, "invalid_workflow", "concurrency.limit must be a positive integer")
			return
		}
		policy.Concurrency = cfg.Limit
	}
	if len(in.Schedules) > 0 {
		if err := json.Unmarshal(in.Schedules, &policy.Schedules); err != nil {
			var one string
			if json.Unmarshal(in.Schedules, &one) == nil {
				policy.Schedules = []string{one}
			} else {
				var objects []struct {
					Cron string `json:"cron"`
				}
				if json.Unmarshal(in.Schedules, &objects) != nil {
					writeErr(w, http.StatusBadRequest, "invalid_workflow", "schedules must be cron strings")
					return
				}
				for _, obj := range objects {
					if obj.Cron == "" {
						writeErr(w, http.StatusBadRequest, "invalid_workflow", "schedule cron is required")
						return
					}
					policy.Schedules = append(policy.Schedules, obj.Cron)
				}
			}
		}
	}
	if len(in.DefaultRetention) > 0 {
		var cfg struct {
			SuccessRetention any `json:"success_retention"`
			ErrorRetention   any `json:"error_retention"`
		}
		if err := json.Unmarshal(in.DefaultRetention, &cfg); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_workflow", "default_retention must be an object")
			return
		}
		var err error
		policy.SuccessRetentionMs, err = parseRetention(cfg.SuccessRetention)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_workflow", err.Error())
			return
		}
		policy.ErrorRetentionMs, err = parseRetention(cfg.ErrorRetention)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_workflow", err.Error())
			return
		}
	}
	policyRaw, _ := json.Marshal(policy)
	ns, name := r.PathValue("account_id"), r.PathValue("workflow_name")
	_, err := s.Control.CreateResourceWithConfig(r.Context(), ns, "workflow", name, defaultResourceScope(ns, "workflow", name), policyRaw, "wrangler")
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

func parseRetention(v any) (int64, error) {
	if v == nil {
		return 0, nil
	}
	switch x := v.(type) {
	case float64:
		if x > 0 && x == float64(int64(x)) {
			return int64(x), nil
		}
	case string:
		s := strings.TrimSpace(strings.ToLower(x))
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d.Milliseconds(), nil
		}
		fields := strings.Fields(s)
		if len(fields) == 2 {
			n, err := strconv.ParseFloat(fields[0], 64)
			if err == nil {
				mult := map[string]float64{"day": 24 * float64(time.Hour), "days": 24 * float64(time.Hour), "hour": float64(time.Hour), "hours": float64(time.Hour), "minute": float64(time.Minute), "minutes": float64(time.Minute)}[fields[1]]
				if mult > 0 && n > 0 {
					return int64(n * mult), nil
				}
			}
		}
	}
	return 0, fmt.Errorf("retention must be a positive milliseconds value or duration string")
}

func (s *Server) handleCloudflareWorkflowDelete(w http.ResponseWriter, r *http.Request) {
	ns, name := r.PathValue("account_id"), r.PathValue("workflow_name")
	if err := s.Control.DeleteResource(r.Context(), ns, "workflow", name, "wrangler"); err != nil && !errors.Is(err, control.ErrNotFound) {
		writeErr(w, http.StatusBadRequest, "workflow_delete_failed", err.Error())
		return
	}
	cloudflareResult(w, nil)
}
