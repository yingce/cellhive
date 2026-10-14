package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"cellhive/internal/control"
)

func (s *Server) handleCloudflareAccounts(w http.ResponseWriter, r *http.Request) {
	apps, err := s.Control.Apps(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "accounts_failed", err.Error())
		return
	}
	out := make([]any, 0, len(apps))
	for _, app := range apps {
		out = append(out, map[string]any{"id": app.Namespace, "name": app.Namespace, "status": "active"})
	}
	cloudflareResult(w, out)
}

func (s *Server) handleCloudflareMemberships(w http.ResponseWriter, r *http.Request) {
	apps, err := s.Control.Apps(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "memberships_failed", err.Error())
		return
	}
	out := make([]any, 0, len(apps))
	for _, app := range apps {
		out = append(out, map[string]any{"id": app.Namespace, "account": map[string]any{"id": app.Namespace, "name": app.Namespace}})
	}
	cloudflareResult(w, out)
}

func (s *Server) handleCloudflareWorkerDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.Control.DeleteWorker(r.Context(), r.PathValue("account_id"), r.PathValue("script_name"), "wrangler"); err != nil {
		writeErr(w, http.StatusNotFound, "worker_not_found", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "result": nil})
}

func (s *Server) handleCloudflareWorkerVersionDeploy(w http.ResponseWriter, r *http.Request) {
	ns, worker, wanted := r.PathValue("account_id"), r.PathValue("script_name"), r.PathValue("version_id")
	releases, err := s.Control.Releases(r.Context(), ns, worker)
	if err != nil {
		writeErr(w, http.StatusNotFound, "version_not_found", err.Error())
		return
	}
	version := 0
	for _, rel := range releases {
		if wanted == rel.BundleSHA || wanted == strconv.Itoa(rel.Version) {
			version = rel.Version
			break
		}
	}
	if version == 0 {
		writeErr(w, http.StatusNotFound, "version_not_found", "version does not exist")
		return
	}
	wk, err := s.Control.Promote(r.Context(), ns, worker, version, "wrangler")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "version_deploy_failed", err.Error())
		return
	}
	cloudflareResult(w, map[string]any{"id": wanted, "version_id": wanted, "script_name": wk.Name})
}

func (s *Server) activeCloudflareVersion(r *http.Request) (control.Version, error) {
	p, err := s.Control.Projection(r.Context())
	if err != nil {
		return control.Version{}, err
	}
	ns, worker := r.PathValue("account_id"), r.PathValue("script_name")
	for _, app := range p.Apps {
		if app.Namespace != ns {
			continue
		}
		for _, wk := range app.Workers {
			if wk.Worker == worker {
				return wk.Version, nil
			}
		}
	}
	return control.Version{}, control.ErrNotFound
}

func cloudflareVersionResult(v control.Version) map[string]any {
	id := v.BundleSHA
	if id == "" {
		id = strconv.Itoa(v.Number)
	}
	return map[string]any{"id": id, "number": v.Number, "etag": v.BundleSHA, "created_on": v.CreatedMs, "modified_on": v.CreatedMs, "resources": map[string]any{"script": map[string]any{"etag": v.BundleSHA}}}
}

func (s *Server) handleCloudflareWorkerVersions(w http.ResponseWriter, r *http.Request) {
	releases, err := s.Control.Releases(r.Context(), r.PathValue("account_id"), r.PathValue("script_name"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "versions_failed", err.Error())
		return
	}
	out := make([]any, 0, len(releases))
	for _, rel := range releases {
		out = append(out, map[string]any{"id": rel.BundleSHA, "number": rel.Version, "etag": rel.BundleSHA, "created_on": rel.CreatedMs, "modified_on": rel.CreatedMs, "deployment_id": rel.BundleSHA})
	}
	cloudflareResult(w, out)
}

func (s *Server) handleCloudflareWorkerVersionGet(w http.ResponseWriter, r *http.Request) {
	v, err := s.activeCloudflareVersion(r)
	if err != nil {
		writeErr(w, http.StatusNotFound, "version_not_found", err.Error())
		return
	}
	wanted := r.PathValue("version_id")
	if wanted != "" && wanted != v.BundleSHA && wanted != strconv.Itoa(v.Number) {
		writeErr(w, http.StatusNotFound, "version_not_found", "version is not active")
		return
	}
	cloudflareResult(w, cloudflareVersionResult(v))
}

func (s *Server) handleCloudflareWorkerSchedules(w http.ResponseWriter, r *http.Request) {
	v, err := s.activeCloudflareVersion(r)
	if err != nil {
		writeErr(w, http.StatusNotFound, "worker_not_found", err.Error())
		return
	}
	if r.Method == http.MethodGet {
		out := make([]any, 0, len(v.Crons))
		for _, cron := range v.Crons {
			out = append(out, map[string]any{"cron": cron, "created_on": v.CreatedMs, "modified_on": v.CreatedMs})
		}
		cloudflareResult(w, out)
		return
	}
	var in []struct {
		Cron string `json:"cron"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_schedules", err.Error())
		return
	}
	crons := make([]string, 0, len(in))
	for _, item := range in {
		if strings.TrimSpace(item.Cron) == "" {
			writeErr(w, http.StatusBadRequest, "invalid_schedules", "cron is required")
			return
		}
		crons = append(crons, item.Cron)
	}
	newV, err := s.Control.Deploy(r.Context(), r.PathValue("account_id"), r.PathValue("script_name"), control.DeploySpec{BundleSHA: v.BundleSHA, Bindings: v.Bindings, Vars: v.Vars, Consumers: v.Consumers, Crons: crons, AssetsSHA: v.AssetsSHA, Assets: v.Assets, CompatDate: v.CompatDate, CompatFlags: v.CompatFlags, Limits: v.Limits}, "wrangler-schedules")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "schedule_update_failed", err.Error())
		return
	}
	cloudflareResult(w, []any{map[string]any{"cron": crons, "version": newV.Number}})
}

func (s *Server) handleCloudflareWorkerSecretGet(w http.ResponseWriter, r *http.Request) {
	val, err := s.Control.GetSecret(r.Context(), r.PathValue("account_id"), r.PathValue("script_name"), r.PathValue("secret_name"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "secret_not_found", err.Error())
		return
	}
	cloudflareResult(w, map[string]any{"name": r.PathValue("secret_name"), "type": "secret_text", "text": string(val)})
}
