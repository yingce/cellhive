package server

// A small Cloudflare Workers API compatibility surface for the stock Wrangler
// client. The API intentionally translates at the edge into CellHive's native
// control requests; it does not make the control store depend on Cloudflare's
// URL or JSON conventions.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/queue"
)

func (s *Server) registerCloudflareWorkerRoutes(mux *http.ServeMux) {
	compat := s.cloudflareAuth
	for _, prefix := range []string{"", "/client/v4"} {
		mux.HandleFunc("GET "+prefix+"/memberships", compat(s.handleCloudflareMemberships))
		mux.HandleFunc("GET "+prefix+"/accounts", compat(s.handleCloudflareAccounts))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}", compat(s.handleCloudflareWorkerDelete))
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
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/versions", compat(s.handleCloudflareWorkerVersions))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/versions/{version_id}", compat(s.handleCloudflareWorkerVersionGet))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/versions/{version_id}/deploy", compat(s.handleCloudflareWorkerVersionDeploy))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/schedules", compat(s.handleCloudflareWorkerSchedules))
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/schedules", compat(s.handleCloudflareWorkerSchedules))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/secrets", compat(s.handleCloudflareWorkerSecrets))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/secrets/{secret_name}", compat(s.handleCloudflareWorkerSecretGet))
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/secrets", compat(s.handleCloudflareWorkerSecretPut))
		mux.HandleFunc("PATCH "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/secrets-bulk", compat(s.handleCloudflareWorkerSecretBulk))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/secrets/{secret_name}", compat(s.handleCloudflareWorkerSecretDelete))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/tails", compat(s.handleCloudflareTailCreate))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/tails/{tail_id}", compat(s.handleCloudflareTailDelete))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/tails/{tail_id}/stream", s.handleCloudflareTailStream)
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
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/d1/database/{resource_id}/query", compat(s.handleCloudflareD1Query))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/d1/database/{resource_id}/import", compat(s.handleCloudflareD1Import))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/d1/database/{resource_id}/export", compat(s.handleCloudflareD1Export))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/d1/database/{resource_id}/export/download/{download_id}", s.handleCloudflareD1ExportDownload)
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/d1/database/{resource_id}/import/upload/{upload_id}", s.handleCloudflareD1ImportUpload)
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/storage/kv/namespaces/{resource_id}/values/{key...}", compat(s.handleCloudflareKVValuePut))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/storage/kv/namespaces/{resource_id}/values/{key...}", compat(s.handleCloudflareKVValueGet))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/storage/kv/namespaces/{resource_id}/values/{key...}", compat(s.handleCloudflareKVValueDelete))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/storage/kv/namespaces/{resource_id}/keys", compat(s.handleCloudflareKVKeys))
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/storage/kv/namespaces/{resource_id}/bulk", compat(s.handleCloudflareKVBulkPut))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/storage/kv/namespaces/{resource_id}/bulk", compat(s.handleCloudflareKVBulkDelete))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/r2/buckets", compat(s.handleCloudflareR2Create))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/r2/buckets", compat(s.handleCloudflareR2List))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/r2/buckets/{resource_id}", compat(s.handleCloudflareResourceDelete("r2")))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/r2/buckets/{resource_id}", compat(s.handleCloudflareR2Get))
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/r2/buckets/{resource_id}/objects/{key...}", compat(s.handleCloudflareR2ObjectPut))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/r2/buckets/{resource_id}/objects/{key...}", compat(s.handleCloudflareR2ObjectGet))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/r2/buckets/{resource_id}/objects/{key...}", compat(s.handleCloudflareR2ObjectDelete))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/queues", compat(s.handleCloudflareQueueCreate))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/queues", compat(s.handleCloudflareQueueList))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/queues/{resource_id}", compat(s.handleCloudflareResourceDelete("queue")))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/queues/{resource_id}", compat(s.handleCloudflareQueueGet))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/queues/{resource_id}/consumers", compat(s.handleCloudflareQueueConsumerCreate))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/queues/{resource_id}/consumers", compat(s.handleCloudflareQueueConsumerList))
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/queues/{resource_id}/consumers/{consumer_id}", compat(s.handleCloudflareQueueConsumerUpdate))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/queues/{resource_id}/consumers/{consumer_id}", compat(s.handleCloudflareQueueConsumerGet))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/queues/{resource_id}/consumers/{consumer_id}", compat(s.handleCloudflareQueueConsumerDelete))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/queues/{resource_id}/purge", compat(s.handleCloudflareQueuePurge))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/queues/{resource_id}/purge", compat(s.handleCloudflareQueuePurgeStatus))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/vectorize/v2/indexes", compat(s.handleCloudflareVectorizeCreate))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/vectorize/v2/indexes", compat(s.handleCloudflareVectorizeList))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/vectorize/v2/indexes/{index_name}", compat(s.handleCloudflareVectorizeGet))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/vectorize/v2/indexes/{index_name}", compat(s.handleCloudflareVectorizeDelete))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/vectorize/v2/indexes/{index_name}/info", compat(s.handleCloudflareVectorizeInfo))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workers/scripts/{script_name}/assets-upload-session", compat(s.handleCloudflareAssetsSession))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workers/assets/upload", s.handleCloudflareAssetsUpload)
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workers/assets/upload/{asset_hash}", s.handleCloudflareAssetsUpload)
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workflows", compat(s.handleCloudflareWorkflowList))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workflows/{workflow_name}", compat(s.handleCloudflareWorkflowGet))
		mux.HandleFunc("PUT "+prefix+"/accounts/{account_id}/workflows/{workflow_name}", compat(s.handleCloudflareWorkflowPut))
		mux.HandleFunc("DELETE "+prefix+"/accounts/{account_id}/workflows/{workflow_name}", compat(s.handleCloudflareWorkflowDelete))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workflows/{workflow_name}/instances", compat(s.handleCloudflareWorkflowInstanceCreate))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workflows/{workflow_name}/instances", compat(s.handleCloudflareWorkflowInstanceList))
		mux.HandleFunc("GET "+prefix+"/accounts/{account_id}/workflows/{workflow_name}/instances/{instance_id}", compat(s.handleCloudflareWorkflowInstanceGet))
		mux.HandleFunc("PATCH "+prefix+"/accounts/{account_id}/workflows/{workflow_name}/instances/{instance_id}/status", compat(s.handleCloudflareWorkflowInstanceStatus))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workflows/{workflow_name}/instances/{instance_id}/events/{event_type}", compat(s.handleCloudflareWorkflowInstanceEvent))
		mux.HandleFunc("POST "+prefix+"/accounts/{account_id}/workflows/{workflow_name}/instances/batch/delete", compat(s.handleCloudflareWorkflowInstanceDeleteBatch))
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
	ns, worker := r.PathValue("account_id"), r.PathValue("script_name")
	events, err := s.Control.DeploymentEvents(r.Context(), ns, worker, 10)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "deployments_failed", err.Error())
		return
	}
	deployments := make([]any, 0, len(events))
	for _, event := range events {
		deployments = append(deployments, cloudflareDeploymentResult(event))
	}
	cloudflareResult(w, map[string]any{"deployments": deployments})
}

func (s *Server) handleCloudflareWorkerDeploymentCreate(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Strategy string `json:"strategy"`
		Versions []struct {
			VersionID  string  `json:"version_id"`
			Percentage float64 `json:"percentage"`
		} `json:"versions"`
	}
	if !decodeLimit(w, r, &in, 1<<20) {
		return
	}
	if in.Strategy != "percentage" || len(in.Versions) != 1 || in.Versions[0].Percentage != 100 {
		writeErr(w, http.StatusBadRequest, "unsupported_traffic_split", "CellHive supports one version at 100% traffic")
		return
	}
	ns, worker := r.PathValue("account_id"), r.PathValue("script_name")
	version, err := strconv.Atoi(in.Versions[0].VersionID)
	if err != nil || version <= 0 {
		writeErr(w, http.StatusBadRequest, "invalid_version", "version_id must be a positive version number")
		return
	}
	if !s.cloudflarePromote(w, r, ns, worker, version) {
		return
	}
	events, err := s.Control.DeploymentEvents(r.Context(), ns, worker, 1)
	if err != nil || len(events) == 0 || events[0].Version != version {
		writeErr(w, http.StatusInternalServerError, "deployment_readback_failed", "committed deployment event is unavailable")
		return
	}
	cloudflareResult(w, cloudflareDeploymentResult(events[0]))
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
	if metadata.AssetsJWT != "" {
		claim, err := s.verifyAssetsClaim(metadata.AssetsJWT)
		if err != nil || claim.NS != ns || claim.Worker != worker {
			writeErr(w, http.StatusBadRequest, "invalid_assets", "assets token does not belong to this worker")
			return
		}
		metadata.AssetsToken = claim.Token
		data, err := st.GetAsset(r.Context(), ns, worker, claim.Token, wranglerManifestPath)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_assets", "asset upload session missing")
			return
		}
		var manifest map[string]wranglerAsset
		if json.Unmarshal(data, &manifest) != nil {
			writeErr(w, http.StatusBadRequest, "invalid_assets", "asset manifest invalid")
			return
		}
		for path := range manifest {
			if _, err := st.GetAsset(r.Context(), ns, worker, claim.Token, path); err != nil {
				writeErr(w, http.StatusBadRequest, "incomplete_assets", "asset "+path+" was not uploaded")
				return
			}
		}
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
	// Wrangler registers locally defined Workflows after the Worker upload.
	// Their binding must resolve during CellHive's atomic deploy, so reserve
	// that declared workflow name now. KV/D1/R2/Queue remain explicit resources.
	for _, b := range metadata.Bindings {
		if b.Type != "workflow" || b.ID == "" || b.ClassName == "" {
			continue
		}
		if _, err := s.Control.GetResource(r.Context(), ns, "workflow", b.ID); err != nil {
			if _, err := s.Control.CreateResource(r.Context(), ns, "workflow", b.ID, defaultResourceScope(ns, "workflow", b.ID), "wrangler"); err != nil {
				writeErr(w, http.StatusBadRequest, "workflow_create_failed", err.Error())
				return
			}
		}
	}

	req := deployReq{
		Namespace: ns, Worker: worker, BundleSHA: sha,
		AssetsSHA: metadata.AssetsToken, Assets: metadata.Assets,
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
			"id":              strconv.Itoa(version.Number),
			"version_id":      strconv.Itoa(version.Number),
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
	AssetsJWT          string
	AssetsToken        string
	Assets             *control.AssetsConfig
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
		Assets             struct {
			JWT    string `json:"jwt"`
			Config struct {
				NotFoundHandling string `json:"not_found_handling"`
			} `json:"config"`
		} `json:"assets"`
		Triggers struct {
			Crons []string `json:"crons"`
		} `json:"triggers"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	out.MainModule, out.CompatibilityDate, out.CompatibilityFlags = raw.MainModule, raw.CompatibilityDate, raw.CompatibilityFlags
	out.Migrations, out.Crons = raw.Migrations, raw.Triggers.Crons
	if raw.Assets.JWT != "" {
		out.AssetsJWT = raw.Assets.JWT
		out.Assets = &control.AssetsConfig{NotFoundHandling: raw.Assets.Config.NotFoundHandling}
	}
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
	body, _ := json.Marshal(secretPutReq{Namespace: ns, Worker: worker, Key: in.Name, Value: base64.StdEncoding.EncodeToString([]byte(in.Text))})
	req := r.Clone(r.Context())
	req.Method = http.MethodPost
	req.URL.Path = "/v1/control/secret"
	req.URL.RawPath = ""
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenInternal)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if cloudflareBridgeError(w, rec, nil) {
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
	req := r.Clone(r.Context())
	req.Method = http.MethodDelete
	req.URL.Path = "/v1/control/secret"
	req.URL.RawPath = ""
	req.URL.RawQuery = url.Values{"namespace": {ns}, "worker": {worker}, "key": {key}}.Encode()
	req.Header.Set("x-cellhive-admin-token", s.Cfg.AdminToken)
	rec := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rec, req)
	if cloudflareBridgeError(w, rec, nil) {
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
	s.cloudflareResourceList(w, r, "queue", func(x control.ResourceRef) any { return s.cloudflareQueueResult(r, x.Name) })
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
	cloudflareResult(w, s.cloudflareQueueResult(r, res.Name))
}

func (s *Server) cloudflareQueueResult(r *http.Request, queue string) map[string]any {
	out := map[string]any{"queue_id": queue, "queue_name": queue, "consumers": []any{}}
	if p, err := s.Control.Projection(r.Context()); err == nil {
		items := make([]any, 0)
		for _, target := range p.QueueTargets() {
			if target.Namespace != r.PathValue("account_id") || target.Queue != queue {
				continue
			}
			items = append(items, map[string]any{
				"consumer_id": target.Worker,
				"type":        "worker", "script": target.Worker,
				"settings":          map[string]any{"batch_size": target.Consumer.MaxBatchSize, "max_retries": target.Consumer.MaxRetries, "max_concurrency": target.Consumer.MaxConcurrency, "max_wait_time_ms": target.Consumer.MaxBatchTimeoutSeconds * 1000},
				"dead_letter_queue": target.Consumer.DeadLetterQueue,
			})
		}
		out["consumers"] = items
	}
	return out
}

type cloudflareQueueConsumerRequest struct {
	Type            string `json:"type"`
	ScriptName      string `json:"script_name"`
	DeadLetterQueue string `json:"dead_letter_queue"`
	Settings        struct {
		BatchSize      int `json:"batch_size"`
		MaxRetries     int `json:"max_retries"`
		MaxConcurrency int `json:"max_concurrency"`
		MaxWaitTimeMs  int `json:"max_wait_time_ms"`
		RetryDelay     int `json:"retry_delay"`
	} `json:"settings"`
}

func (s *Server) cloudflareQueueConsumerApply(w http.ResponseWriter, r *http.Request, remove bool) {
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	ns, queue := r.PathValue("account_id"), r.PathValue("resource_id")
	if _, err := s.Control.GetResource(r.Context(), ns, "queue", queue); err != nil {
		writeErr(w, 404, "queue_not_found", "queue does not exist")
		return
	}
	var in cloudflareQueueConsumerRequest
	if !remove {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil || in.Type != "worker" || in.ScriptName == "" {
			writeErr(w, http.StatusBadRequest, "invalid_consumer", "type=worker and script_name are required")
			return
		}
	} else {
		in.ScriptName = r.PathValue("consumer_id")
	}
	if !remove {
		if in.Settings.RetryDelay != 0 {
			writeErr(w, http.StatusBadRequest, "unsupported_retry_delay", "queue consumer retry_delay is not supported")
			return
		}
		if in.Settings.BatchSize < 0 || in.Settings.MaxRetries < 0 || in.Settings.MaxConcurrency < 0 || in.Settings.MaxWaitTimeMs < 0 || in.Settings.MaxWaitTimeMs%1000 != 0 {
			writeErr(w, http.StatusBadRequest, "invalid_consumer", "invalid queue consumer settings")
			return
		}
		if id := r.PathValue("consumer_id"); id != "" && id != in.ScriptName {
			writeErr(w, http.StatusBadRequest, "consumer_mismatch", "consumer_id must match script_name")
			return
		}
	}
	var nv control.Version
	err := s.capturedWrite(r.Context(), control.ScopeFor(ns), func() error {
		v, err := s.activeCloudflareVersionForWorker(r.Context(), ns, in.ScriptName)
		if err != nil {
			return err
		}
		consumers := make([]control.Consumer, 0, len(v.Consumers)+1)
		found := false
		for _, c := range v.Consumers {
			if c.Queue == queue {
				found = true
				continue
			}
			consumers = append(consumers, c)
		}
		if remove && !found {
			return control.ErrNotFound
		}
		if !remove {
			consumers = append(consumers, control.Consumer{Queue: queue, DeadLetterQueue: in.DeadLetterQueue, MaxBatchSize: in.Settings.BatchSize, MaxRetries: in.Settings.MaxRetries, MaxConcurrency: in.Settings.MaxConcurrency, MaxBatchTimeoutSeconds: in.Settings.MaxWaitTimeMs / 1000})
		}
		nv, err = s.Control.Deploy(r.Context(), ns, in.ScriptName, control.DeploySpec{BundleSHA: v.BundleSHA, Bindings: v.Bindings, Vars: v.Vars, Consumers: consumers, Crons: v.Crons, AssetsSHA: v.AssetsSHA, Assets: v.Assets, CompatDate: v.CompatDate, CompatFlags: v.CompatFlags, Limits: v.Limits, SessionPolicy: v.SessionPolicy}, "wrangler-queue-consumer")
		return err
	})
	if err != nil {
		writeErr(w, http.StatusBadRequest, "consumer_update_failed", err.Error())
		return
	}
	s.invalidateBindings(ns)
	cloudflareResult(w, map[string]any{"consumer_id": in.ScriptName, "type": "worker", "script": in.ScriptName, "queue_name": queue, "version": nv.Number})
}

func (s *Server) activeCloudflareVersionForWorker(ctx context.Context, ns, worker string) (control.Version, error) {
	p, err := s.Control.Projection(ctx)
	if err != nil {
		return control.Version{}, err
	}
	for _, app := range p.Apps {
		if app.Namespace == ns {
			for _, wk := range app.Workers {
				if wk.Worker == worker {
					return wk.Version, nil
				}
			}
		}
	}
	return control.Version{}, control.ErrNotFound
}

func (s *Server) handleCloudflareQueueConsumerCreate(w http.ResponseWriter, r *http.Request) {
	s.cloudflareQueueConsumerApply(w, r, false)
}
func (s *Server) handleCloudflareQueueConsumerUpdate(w http.ResponseWriter, r *http.Request) {
	s.cloudflareQueueConsumerApply(w, r, false)
}
func (s *Server) handleCloudflareQueueConsumerDelete(w http.ResponseWriter, r *http.Request) {
	s.cloudflareQueueConsumerApply(w, r, true)
}
func (s *Server) handleCloudflareQueueConsumerList(w http.ResponseWriter, r *http.Request) {
	q := s.cloudflareQueueResult(r, r.PathValue("resource_id"))
	cloudflareResult(w, q["consumers"])
}
func (s *Server) handleCloudflareQueueConsumerGet(w http.ResponseWriter, r *http.Request) {
	q := s.cloudflareQueueResult(r, r.PathValue("resource_id"))
	for _, item := range q["consumers"].([]any) {
		if item.(map[string]any)["consumer_id"] == r.PathValue("consumer_id") {
			cloudflareResult(w, item)
			return
		}
	}
	writeErr(w, http.StatusNotFound, "consumer_not_found", "consumer does not exist")
}

func (s *Server) handleCloudflareQueuePurge(w http.ResponseWriter, r *http.Request) {
	if s.Queue == nil {
		writeErr(w, http.StatusServiceUnavailable, "queue_unavailable", "queue store is not configured")
		return
	}
	ns, name := r.PathValue("account_id"), r.PathValue("resource_id")
	if _, err := s.Control.GetResource(r.Context(), ns, "queue", name); err != nil {
		writeErr(w, http.StatusNotFound, "queue_not_found", err.Error())
		return
	}
	if s.forwardOrClaim(w, r, queue.Scope(ns, name), nil) {
		return
	}
	if err := s.capturedWrite(r.Context(), queue.Scope(ns, name), func() error { return s.Queue.Purge(r.Context(), ns, name) }); err != nil {
		writeErr(w, http.StatusBadRequest, "queue_purge_failed", err.Error())
		return
	}
	cloudflareResult(w, map[string]any{"queue_id": name, "status": "complete"})
}

func (s *Server) handleCloudflareQueuePurgeStatus(w http.ResponseWriter, r *http.Request) {
	cloudflareResult(w, map[string]any{"queue_id": r.PathValue("resource_id"), "status": "complete"})
}
