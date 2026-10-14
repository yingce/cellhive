package server

// A small Cloudflare Workers API compatibility surface for the stock Wrangler
// client. The API intentionally translates at the edge into CellHive's native
// control requests; it does not make the control store depend on Cloudflare's
// URL or JSON conventions.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"time"

	"cellhive/internal/control"
)

func (s *Server) registerCloudflareWorkerRoutes(mux *http.ServeMux) {
	compat := s.cloudflareAuth
	for _, prefix := range []string{"", "/client/v4"} {
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/subdomain", compat(s.handleCloudflareWorkerSubdomain))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/subdomain", compat(s.handleCloudflareWorkerScriptSubdomain))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/subdomain", compat(s.handleCloudflareWorkerScriptSubdomain))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/services/{script_name}", compat(s.handleCloudflareWorkerService))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/deployments", compat(s.handleCloudflareWorkerDeployments))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/deployments", compat(s.handleCloudflareWorkerDeploymentCreate))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/settings", compat(s.handleCloudflareWorkerSettings))
		mux.HandleFunc("PATCH "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/script-settings", compat(s.handleCloudflareWorkerSettings))
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/content", compat(s.handleCloudflareWorkerUpload))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/versions", compat(s.handleCloudflareWorkerUpload))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/secrets", compat(s.handleCloudflareWorkerSecrets))
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/secrets", compat(s.handleCloudflareWorkerSecretPut))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/secrets/{secret_name}", compat(s.handleCloudflareWorkerSecretDelete))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/routes", compat(s.handleCloudflareRoutes))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workers/routes", compat(s.handleCloudflareRoutePut))
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/workers/routes/{route_id}", compat(s.handleCloudflareRoutePut))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/workers/routes/{route_id}", compat(s.handleCloudflareRouteDelete))
		// Resource APIs used by wrangler kv/d1/r2/queues create/list/delete.
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/storage/kv/namespaces", compat(s.handleCloudflareKVCreate))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/storage/kv/namespaces", compat(s.handleCloudflareKVList))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/storage/kv/namespaces/{resource_id}", compat(s.handleCloudflareResourceDelete("kv")))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/d1/database", compat(s.handleCloudflareD1Create))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/d1/database", compat(s.handleCloudflareD1List))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/d1/database/{resource_id}", compat(s.handleCloudflareResourceDelete("d1")))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/d1/database/{resource_id}", compat(s.handleCloudflareD1Get))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/r2/buckets", compat(s.handleCloudflareR2Create))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/r2/buckets", compat(s.handleCloudflareR2List))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/r2/buckets/{resource_id}", compat(s.handleCloudflareResourceDelete("r2")))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/r2/buckets/{resource_id}", compat(s.handleCloudflareR2Get))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/queues", compat(s.handleCloudflareQueueCreate))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/queues", compat(s.handleCloudflareQueueList))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/queues/{resource_id}", compat(s.handleCloudflareResourceDelete("queue")))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/queues/{resource_id}", compat(s.handleCloudflareQueueGet))
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

func cloudflareResult(w http.ResponseWriter, result any) {
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "result": result})
}

func (s *Server) handleCloudflareWorkerSecretPut(w http.ResponseWriter, r *http.Request) {
	ns, worker := r.PathValue("account_id"), r.PathValue("script_name")
	var in struct{ Name, Text, Type string }
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil || in.Name == "" || in.Type != "secret_text" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "name and secret_text type are required")
		return
	}
	if err := s.Control.PutSecret(r.Context(), ns, worker, in.Name, []byte(in.Text), "wrangler"); err != nil {
		writeErr(w, http.StatusBadRequest, "secret_put_failed", err.Error())
		return
	}
	cloudflareResult(w, map[string]any{"name": in.Name})
}

func (s *Server) handleCloudflareWorkerSecrets(w http.ResponseWriter, r *http.Request) {
	metas, err := s.Control.ListSecrets(r.Context(), r.PathValue("account_id"), r.PathValue("script_name"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "secret_list_failed", err.Error())
		return
	}
	if metas == nil {
		metas = []control.SecretMeta{}
	}
	cloudflareResult(w, metas)
}

func (s *Server) handleCloudflareWorkerSecretDelete(w http.ResponseWriter, r *http.Request) {
	ns, worker, key := r.PathValue("account_id"), r.PathValue("script_name"), r.PathValue("secret_name")
	if err := s.Control.DeleteSecret(r.Context(), ns, worker, key, "wrangler"); err != nil {
		writeErr(w, http.StatusNotFound, "secret_delete_failed", err.Error())
		return
	}
	cloudflareResult(w, nil)
}

type cloudflareRoute struct{ ID, Pattern, Script string }

func (s *Server) handleCloudflareRoutes(w http.ResponseWriter, r *http.Request) {
	p, err := s.Control.Projection(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "routes_failed", err.Error())
		return
	}
	ns := r.PathValue("account_id")
	out := []cloudflareRoute{}
	for _, app := range p.Apps {
		if app.Namespace != ns {
			continue
		}
		for _, rt := range app.Routes {
			out = append(out, cloudflareRoute{ID: rt.Host, Pattern: rt.Host + rt.Path, Script: rt.Worker})
		}
	}
	cloudflareResult(w, out)
}

func (s *Server) handleCloudflareRoutePut(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("account_id")
	var in struct{ Pattern, Script string }
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if in.Pattern == "" || in.Script == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "pattern and script are required")
		return
	}
	u, err := url.Parse(in.Pattern)
	if err != nil || u.Host == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "pattern must be an absolute URL")
		return
	}
	// Cloudflare routes implicitly own the hostname. CellHive keeps host
	// registration explicit internally, so the compatibility adapter performs
	// the idempotent registration before writing the route.
	if _, err := s.Control.PutCustomHost(r.Context(), ns, u.Host, "wrangler"); err != nil {
		writeErr(w, http.StatusBadRequest, "route_host_failed", err.Error())
		return
	}
	if err := s.Control.PutRoute(r.Context(), ns, control.Route{Host: u.Host, Path: u.EscapedPath(), Worker: in.Script}, "wrangler"); err != nil {
		writeErr(w, http.StatusBadRequest, "route_put_failed", err.Error())
		return
	}
	cloudflareResult(w, cloudflareRoute{ID: u.Host, Pattern: in.Pattern, Script: in.Script})
}

func (s *Server) handleCloudflareRouteDelete(w http.ResponseWriter, r *http.Request) {
	id, _ := url.PathUnescape(r.PathValue("route_id"))
	if id == "" {
		id = r.URL.Query().Get("id")
	}
	if err := s.Control.DeleteRoute(r.Context(), r.PathValue("account_id"), id, "wrangler"); err != nil {
		writeErr(w, http.StatusNotFound, "route_delete_failed", err.Error())
		return
	}
	cloudflareResult(w, nil)
}

func (s *Server) cloudflareResource(ctx context.Context, ns, kind, name string) (control.Resource, error) {
	if s.Control == nil {
		return control.Resource{}, control.ErrNotFound
	}
	return s.Control.CreateResource(ctx, ns, kind, name, defaultResourceScope(ns, kind, name), "wrangler")
}

func (s *Server) cloudflareResourceCreate(w http.ResponseWriter, r *http.Request, kind string, name string) {
	ns := r.PathValue("account_id")
	if name == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "resource name is required")
		return
	}
	if _, err := s.Control.CreateApp(r.Context(), ns, "wrangler"); err != nil && !strings.Contains(err.Error(), "already exists") {
		writeErr(w, http.StatusBadRequest, "account_create_failed", err.Error())
		return
	}
	res, err := s.cloudflareResource(r.Context(), ns, kind, name)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "resource_create_failed", err.Error())
		return
	}
	switch kind {
	case "kv":
		cloudflareResult(w, map[string]any{"id": res.Name, "title": res.Name})
	case "d1":
		cloudflareResult(w, map[string]any{"uuid": res.Name, "name": res.Name, "version": "production"})
	case "r2":
		cloudflareResult(w, map[string]any{"name": res.Name, "creation_date": time.Now().UTC().Format(time.RFC3339)})
	case "queue":
		cloudflareResult(w, map[string]any{"queue_id": res.Name, "queue_name": res.Name, "created_on": time.Now().UTC().Format(time.RFC3339), "modified_on": time.Now().UTC().Format(time.RFC3339)})
	default:
		cloudflareResult(w, res)
	}
}

func (s *Server) handleCloudflareKVCreate(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Title string `json:"title"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	s.cloudflareResourceCreate(w, r, "kv", in.Title)
}
func (s *Server) handleCloudflareD1Create(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	s.cloudflareResourceCreate(w, r, "d1", in.Name)
}
func (s *Server) handleCloudflareR2Create(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	s.cloudflareResourceCreate(w, r, "r2", in.Name)
}
func (s *Server) handleCloudflareQueueCreate(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name      string `json:"name"`
		QueueName string `json:"queue_name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	name := in.Name
	if name == "" {
		name = in.QueueName
	}
	s.cloudflareResourceCreate(w, r, "queue", name)
}

func (s *Server) cloudflareResourceList(w http.ResponseWriter, r *http.Request, kind string, shape func(control.ResourceRef) any) {
	items, err := s.Control.Resources(r.Context(), r.PathValue("account_id"), kind)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resources_failed", err.Error())
		return
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, shape(item))
	}
	cloudflareResult(w, out)
}
func (s *Server) handleCloudflareKVList(w http.ResponseWriter, r *http.Request) {
	s.cloudflareResourceList(w, r, "kv", func(x control.ResourceRef) any { return map[string]any{"id": x.Name, "title": x.Name} })
}
func (s *Server) handleCloudflareD1List(w http.ResponseWriter, r *http.Request) {
	s.cloudflareResourceList(w, r, "d1", func(x control.ResourceRef) any {
		return map[string]any{"uuid": x.Name, "name": x.Name, "version": "production"}
	})
}
func (s *Server) handleCloudflareR2List(w http.ResponseWriter, r *http.Request) {
	items, err := s.Control.Resources(r.Context(), r.PathValue("account_id"), "r2")
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resources_failed", err.Error())
		return
	}
	out := make([]any, 0, len(items))
	for _, x := range items {
		out = append(out, map[string]any{"name": x.Name, "creation_date": time.Now().UTC().Format(time.RFC3339)})
	}
	cloudflareResult(w, map[string]any{"buckets": out})
}
func (s *Server) handleCloudflareQueueList(w http.ResponseWriter, r *http.Request) {
	s.cloudflareResourceList(w, r, "queue", func(x control.ResourceRef) any { return map[string]any{"queue_id": x.Name, "queue_name": x.Name} })
}
func (s *Server) handleCloudflareResourceDelete(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ns, name := r.PathValue("account_id"), r.PathValue("resource_id")
		if err := s.Control.DeleteResource(r.Context(), ns, kind, name, "wrangler"); err != nil {
			writeErr(w, http.StatusNotFound, "resource_delete_failed", err.Error())
			return
		}
		cloudflareResult(w, nil)
	}
}

func (s *Server) handleCloudflareD1Get(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("resource_id")
	res, err := s.Control.GetResource(r.Context(), r.PathValue("account_id"), "d1", name)
	if err != nil {
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
		return
	}
	cloudflareResult(w, map[string]any{"uuid": res.Name, "name": res.Name, "version": "production"})
}
func (s *Server) handleCloudflareR2Get(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("resource_id")
	res, err := s.Control.GetResource(r.Context(), r.PathValue("account_id"), "r2", name)
	if err != nil {
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
		return
	}
	cloudflareResult(w, map[string]any{"name": res.Name, "creation_date": time.Now().UTC().Format(time.RFC3339)})
}
func (s *Server) handleCloudflareQueueGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("resource_id")
	res, err := s.Control.GetResource(r.Context(), r.PathValue("account_id"), "queue", name)
	if err != nil {
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
		return
	}
	cloudflareResult(w, map[string]any{"queue_id": res.Name, "queue_name": res.Name})
}
