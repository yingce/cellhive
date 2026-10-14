package server

// A small Cloudflare Workers API compatibility surface for the stock Wrangler
// client. The API intentionally translates at the edge into CellHive's native
// control requests; it does not make the control store depend on Cloudflare's
// URL or JSON conventions.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	"cellhive/internal/control"
)

func (s *Server) registerCloudflareWorkerRoutes(mux *http.ServeMux) {
	compat := s.cloudflareAuth
	for _, prefix := range []string{"", "/client/v4"} {
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/subdomain", compat(s.handleCloudflareWorkerSubdomain))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/subdomain", compat(s.handleCloudflareWorkerScriptSubdomain))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/services/{script_name}", compat(s.handleCloudflareWorkerService))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/deployments", compat(s.handleCloudflareWorkerDeployments))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/deployments", compat(s.handleCloudflareWorkerDeploymentCreate))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/settings", compat(s.handleCloudflareWorkerSettings))
		mux.HandleFunc("PATCH "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/script-settings", compat(s.handleCloudflareWorkerSettings))
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/content", compat(s.handleCloudflareWorkerUpload))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/versions", compat(s.handleCloudflareWorkerUpload))
	}
}

func (s *Server) handleCloudflareWorkerSubdomain(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "result": map[string]any{
		"subdomain": "cellhive",
	}})
}

func (s *Server) handleCloudflareWorkerScriptSubdomain(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "result": map[string]any{
		"enabled": true, "previews_enabled": false, "subdomain": "cellhive",
	}})
}

func (s *Server) handleCloudflareWorkerService(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "result": map[string]any{
		"id":                  r.PathValue("script_name"),
		"default_environment": map[string]any{"environment": "production", "script": map[string]any{"tag": "cellhive"}},
	}})
}

func (s *Server) handleCloudflareWorkerDeployments(w http.ResponseWriter, r *http.Request) {
	// An empty deployment list is a valid Cloudflare response and makes a
	// first Wrangler publish proceed without a prior deployment lookup.
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "result": map[string]any{"deployments": []any{}}})
}

func (s *Server) handleCloudflareWorkerDeploymentCreate(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "result": map[string]any{
		"id": "cellhive-deployment", "strategy": "percentage",
	}})
}

func (s *Server) handleCloudflareWorkerSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "result": map[string]any{
		"bindings": []any{}, "compatibility_date": "2026-09-23", "compatibility_flags": []string{},
	}})
}

// cloudflareAuth accepts the bearer token used by Wrangler and translates it to
// the native admin header before invoking the existing control handler.
func (s *Server) cloudflareAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("x-cellhive-admin-token")
		if token == "" {
			authz := r.Header.Get("Authorization")
			if strings.HasPrefix(authz, "Bearer ") {
				token = strings.TrimSpace(strings.TrimPrefix(authz, "Bearer "))
			}
		}
		if token == "" || s.Cfg.AdminToken == "" || token != s.Cfg.AdminToken {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "Cloudflare API token required")
			return
		}
		r2 := r.Clone(r.Context())
		r2.Header.Set("x-cellhive-admin-token", token)
		next(w, r2)
	}
}

func (s *Server) handleCloudflareWorkerUpload(w http.ResponseWriter, r *http.Request) {
	ns, worker := r.PathValue("account_id"), r.PathValue("script_name")
	if ns == "" || worker == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "account_id and script_name are required")
		return
	}
	metadata, module, err := parseCloudflareWorkerMultipart(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_multipart", err.Error())
		return
	}
	if len(module) == 0 {
		writeErr(w, http.StatusBadRequest, "missing_module", "metadata main_module was not uploaded")
		return
	}
	st := s.artifactStore()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	sha, _, err := st.PutBundle(r.Context(), module)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "bundle_upload_failed", err.Error())
		return
	}

	// Wrangler's account model has no direct CellHive equivalent. The
	// compatibility contract maps account_id to namespace and creates that
	// namespace idempotently on first publish.
	if s.Control == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_control", "control store not configured")
		return
	}
	if _, err := s.Control.CreateApp(r.Context(), ns, "wrangler"); err != nil {
		// CreateApp is idempotent in the control store. Preserve a real failure.
		if !strings.Contains(err.Error(), "already exists") {
			writeErr(w, http.StatusBadRequest, "namespace_create_failed", err.Error())
			return
		}
	}

	req := deployReq{
		Namespace: ns, Worker: worker, BundleSHA: sha,
		Vars: metadata.Vars, Bindings: metadata.Bindings,
		CompatibilityDate:  metadata.CompatibilityDate,
		CompatibilityFlags: metadata.CompatibilityFlags,
		Crons:              metadata.Crons, Consumers: metadata.Consumers,
		Migrations: metadata.Migrations,
	}
	body, _ := json.Marshal(req)
	inner := r.Clone(r.Context())
	inner.Method = http.MethodPost
	inner.Body = io.NopCloser(bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	s.adminAuth(s.handleControlDeploy)(recorder, inner)
	if recorder.Code < 200 || recorder.Code >= 300 {
		for k, values := range recorder.Header() {
			for _, value := range values {
				w.Header().Add(k, value)
			}
		}
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(recorder.Body.Bytes())
		return
	}
	var version control.Version
	if err := json.Unmarshal(recorder.Body.Bytes(), &version); err != nil {
		writeErr(w, http.StatusInternalServerError, "deploy_response_invalid", err.Error())
		return
	}
	// Wrangler unwraps the Cloudflare API envelope and needs a stable version
	// identifier for its publish output.
	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"result": map[string]any{
			"id":              version.BundleSHA,
			"version_id":      version.BundleSHA,
			"etag":            version.BundleSHA,
			"number":          version.Number,
			"startup_time_ms": 0,
			"resources":       map[string]any{"script": map[string]any{"etag": version.BundleSHA}},
		},
	})
}

type cloudflareWorkerMetadata struct {
	Vars               map[string]string
	Bindings           []control.Binding
	CompatibilityDate  string
	CompatibilityFlags []string
	Crons              []string
	Consumers          []control.Consumer
	Migrations         []map[string]any
	MainModule         string
}

func parseCloudflareWorkerMultipart(r *http.Request) (cloudflareWorkerMetadata, []byte, error) {
	var out cloudflareWorkerMetadata
	out.Vars = map[string]string{}
	parts, err := r.MultipartReader()
	if err != nil {
		return out, nil, err
	}
	var modules = map[string][]byte{}
	for {
		part, err := parts.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return out, nil, err
		}
		data, err := io.ReadAll(io.LimitReader(part, maxArtifactBytes+1))
		if err != nil {
			return out, nil, err
		}
		if len(data) > maxArtifactBytes {
			return out, nil, io.ErrShortBuffer
		}
		name := part.FormName()
		if name == "metadata" {
			if err := decodeCloudflareMetadata(data, &out); err != nil {
				return out, nil, err
			}
			continue
		}
		if filename := part.FileName(); filename != "" {
			modules[filename] = data
		}
	}
	module := modules[out.MainModule]
	if len(module) == 0 && out.MainModule == "" {
		for _, name := range []string{"worker.js", "index.js"} {
			if len(modules[name]) > 0 {
				module = modules[name]
				break
			}
		}
	}
	return out, module, nil
}

func decodeCloudflareMetadata(data []byte, out *cloudflareWorkerMetadata) error {
	var raw struct {
		MainModule         string           `json:"main_module"`
		CompatibilityDate  string           `json:"compatibility_date"`
		CompatibilityFlags []string         `json:"compatibility_flags"`
		Bindings           []map[string]any `json:"bindings"`
		Migrations         []map[string]any `json:"migrations"`
		Triggers           struct {
			Crons []string `json:"crons"`
		} `json:"triggers"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	out.MainModule, out.CompatibilityDate, out.CompatibilityFlags = raw.MainModule, raw.CompatibilityDate, raw.CompatibilityFlags
	out.Migrations, out.Crons = raw.Migrations, raw.Triggers.Crons
	for _, b := range raw.Bindings {
		name, typ := stringValue(b["name"]), stringValue(b["type"])
		switch typ {
		case "plain_text":
			out.Vars[name] = stringValue(b["text"])
		case "kv_namespace":
			out.Bindings = append(out.Bindings, control.Binding{Type: "kv", Name: name, ID: stringValue(b["namespace_id"])})
		case "d1":
			out.Bindings = append(out.Bindings, control.Binding{Type: "d1", Name: name, ID: stringValue(b["id"])})
		case "r2_bucket":
			out.Bindings = append(out.Bindings, control.Binding{Type: "r2", Name: name, ID: stringValue(b["bucket_name"])})
		case "queue":
			out.Bindings = append(out.Bindings, control.Binding{Type: "queue", Name: name, ID: stringValue(b["queue_name"])})
		case "service":
			out.Bindings = append(out.Bindings, control.Binding{Type: "service", Name: name, ID: stringValue(b["service"]), Entrypoint: stringValue(b["entrypoint"])})
		case "workflow":
			out.Bindings = append(out.Bindings, control.Binding{Type: "workflow", Name: name, ID: stringValue(b["workflow_name"]), ClassName: stringValue(b["class_name"])})
		}
	}
	return nil
}

func stringValue(v any) string {
	s, _ := v.(string)
	return s
}
