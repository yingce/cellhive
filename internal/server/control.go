package server

import (
	"bytes"
	"cellhive/internal/objectstore"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/auth"
	"cellhive/internal/bucket"
	"cellhive/internal/control"
	"cellhive/internal/wranglercompat"
)

// adminAuthenticator returns the configured admin authenticator, or a static
// token built from the ops credential (ADR-036). Empty credentials fail closed.
func (s *Server) adminAuthenticator() auth.Authenticator {
	if s.AdminAuth != nil {
		return s.AdminAuth
	}
	return auth.StaticToken{Token: s.Cfg.AdminToken}
}

// adminAuth authenticates control-plane requests (static token or OIDC/JWT) and
// attaches the caller's principal for namespace authorization + audit (ADR-131).
func (s *Server) adminAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, ok := s.adminPrincipal(r)
		if !ok {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "admin credential required")
			return
		}
		r = withPrincipal(r, p)
		// Namespace/platform authorization runs before the handler so it also
		// covers requests this node would forward to the control-cell owner.
		if !s.authorizeControlPath(w, r) {
			return
		}
		next(w, r)
	}
}

// pageSlice applies optional limit/offset query parameters to an admin list
// (ADR-131); default returns everything.
func pageSlice[T any](w http.ResponseWriter, r *http.Request, items []T) ([]T, bool) {
	q := r.URL.Query()
	limit, offset := 0, 0
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "bad_request", "limit must be a non-negative integer")
			return nil, false
		}
		limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "bad_request", "offset must be a non-negative integer")
			return nil, false
		}
		offset = n
	}
	if offset >= len(items) {
		return []T{}, true
	}
	items = items[offset:]
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	return items, true
}

func (s *Server) requireControl(w http.ResponseWriter) bool {
	if s.Control == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_control", "control store not configured")
		return false
	}
	return true
}

func decodeControl(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
		return false
	}
	return true
}

// AdminHandler builds the admin mux (mounted on the admin listener only).
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin", s.adminAuth(s.handleAdminUI))
	mux.HandleFunc("GET /v1/control/logs", s.adminAuth(s.handleLogQuery))
	mux.HandleFunc("POST /v1/control/logs/subscribe", s.adminAuth(s.handleLogSubscribe))
	mux.HandleFunc("GET /v1/control/status", s.adminAuth(s.handleControlStatus))
	mux.HandleFunc("POST /v1/control/app", s.adminAuth(s.handleControlApp))
	mux.HandleFunc("POST /v1/control/resource", s.adminAuth(s.handleControlResource))
	mux.HandleFunc("POST /v1/control/deploy", s.adminAuth(s.handleControlDeploy))
	mux.HandleFunc("POST /v1/control/promote", s.adminAuth(s.handleControlPromote))
	mux.HandleFunc("POST /v1/control/rollback", s.adminAuth(s.handleControlRollback))
	mux.HandleFunc("POST /v1/control/route", s.adminAuth(s.handleControlRoutePut))
	mux.HandleFunc("POST /v1/control/domain", s.adminAuth(s.handleControlDomainAdd))
	mux.HandleFunc("GET /v1/control/domains", s.adminAuth(s.handleControlDomainList))
	mux.HandleFunc("POST /v1/control/service-acl", s.adminAuth(s.handleControlServiceACLPut))
	mux.HandleFunc("DELETE /v1/control/service-acl", s.adminAuth(s.handleControlServiceACLDelete))
	mux.HandleFunc("GET /v1/control/service-acls", s.adminAuth(s.handleControlServiceACLList))
	mux.HandleFunc("DELETE /v1/control/domain", s.adminAuth(s.handleControlDomainDelete))
	mux.HandleFunc("DELETE /v1/control/route", s.adminAuth(s.handleControlRouteDelete))
	mux.HandleFunc("POST /v1/control/secret", s.adminAuth(s.handleControlSecretPut))
	mux.HandleFunc("GET /v1/control/secret", s.adminAuth(s.handleControlSecretGet))
	mux.HandleFunc("DELETE /v1/control/secret", s.adminAuth(s.handleControlSecretDelete))
	mux.HandleFunc("GET /v1/control/secrets", s.adminAuth(s.handleControlSecretList))
	mux.HandleFunc("GET /v1/control/audit", s.adminAuth(s.handleControlAudit))
	mux.HandleFunc("DELETE /v1/control/app", s.adminAuth(s.handleControlAppDelete))
	mux.HandleFunc("DELETE /v1/control/worker", s.adminAuth(s.handleControlWorkerDelete))
	mux.HandleFunc("GET /v1/control/apps", s.adminAuth(s.handleControlApps))
	mux.HandleFunc("GET /v1/control/resources", s.adminAuth(s.handleControlResources))
	mux.HandleFunc("DELETE /v1/control/resource", s.adminAuth(s.handleControlResourceDelete))
	mux.HandleFunc("GET /v1/control/queue/status", s.adminAuth(s.handleControlQueueStatus))
	mux.HandleFunc("POST /v1/control/queue/replay-dlq", s.adminAuth(s.handleControlQueueReplayDLQ))
	mux.HandleFunc("GET /v1/control/releases", s.adminAuth(s.handleControlReleases))
	mux.HandleFunc("GET /v1/control/capacity", s.adminAuth(s.handleControlCapacity))
	mux.HandleFunc("POST /v1/control/bundle", s.adminAuth(s.handleBundlePut))
	mux.HandleFunc("POST /v1/control/gc/bundles", s.adminAuth(s.handleBundleGC))
	mux.HandleFunc("POST /v1/control/gc/assets", s.adminAuth(s.handleAssetGC))
	mux.HandleFunc("POST /v1/control/asset", s.adminAuth(s.handleAssetPut))
	// Per-domain resource registry + operator stats (ADR-157) mirror the
	// internal listener so the operator CLI works against the admin port.
	s.registerResourceRoutes(mux, s.adminAuth)
	// Vectorize operator surface (stats + metadata indexes); the binding data
	// API stays on the internal listener with scoped tokens.
	s.registerVectorizeRoutes(mux, false, s.adminAuth)
	return mux
}

func (s *Server) handleControlStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if !s.requireAll(w, r) {
		return
	}
	p, err := s.Control.Projection(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "projection_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "apps": len(p.Apps), "etag": p.ETag})
}

// handleControlWorkerDelete removes a worker: control metadata, its assets, its
// workflow instances, and its DO classes (cold path).
func (s *Server) handleControlWorkerDelete(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	ns, worker := r.URL.Query().Get("namespace"), r.URL.Query().Get("worker")
	if ns == "" || worker == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and worker are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	// Serialize delete against deploy (ADR-107): hold the delete lock for the
	// duration so a concurrent deploy cannot resurrect the worker mid-delete.
	var held bool
	if err := s.capturedWrite(r.Context(), control.ScopeFor(ns), func() error {
		var e error
		held, e = s.Control.AcquireDeleteLock(r.Context(), ns, worker, "admin", 10*time.Minute)
		return e
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "delete_lock_failed", err.Error())
		return
	}
	if held {
		writeErr(w, http.StatusConflict, "delete_in_progress", "another delete is in progress for "+ns+"/"+worker)
		return
	}
	defer func() {
		_ = s.capturedWrite(context.Background(), control.ScopeFor(ns), func() error {
			return s.Control.ReleaseDeleteLock(context.Background(), ns, worker)
		})
	}()
	// Capture bindings before deleting control metadata.
	var classes, workflows []string
	if proj, err := s.Control.Projection(r.Context()); err == nil {
		if pw, ok := proj.WorkerFor(ns, worker); ok {
			for _, b := range pw.Version.Bindings {
				if b.Type == "do" {
					cls := b.ID
					if cls == "" {
						cls = b.Name
					}
					classes = append(classes, cls)
				}
			}
		}
		for _, wt := range proj.WorkflowTargets() {
			if wt.Namespace == ns && wt.Worker == worker {
				workflows = append(workflows, wt.Name)
			}
		}
	}
	if err := s.capturedWrite(r.Context(), control.ScopeFor(ns), func() error {
		return s.Control.DeleteWorker(r.Context(), ns, worker, "admin")
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "delete_failed", err.Error())
		return
	}
	s.invalidateBindings(ns)
	// Cleanup is best-effort but must be reported: a silent failure would tell
	// the operator the worker is gone while its assets/workflows remain.
	var failed []string
	assets := 0
	if s.Bucket != nil {
		st := objectstore.NewOwned(s.Bucket, objectstore.PrefixAssets, objectstore.OwnerArtifacts)
		keys, err := st.ListPrefix(r.Context(), "assets/"+ns+"/"+worker+"/")
		if err != nil {
			failed = append(failed, "asset list: "+err.Error())
		}
		for _, k := range keys {
			if err := st.Delete(r.Context(), k); err != nil {
				failed = append(failed, "asset delete "+k+": "+err.Error())
			} else {
				assets++
			}
		}
	}
	wfDropped := 0
	for _, wf := range workflows {
		if err := s.Workflows.Drop(r.Context(), ns, wf); err != nil {
			failed = append(failed, "workflow drop "+wf+": "+err.Error())
		} else {
			wfDropped++
		}
	}
	clsDeleted := 0
	for _, cls := range classes {
		if err := s.Control.DeleteClass(r.Context(), ns, worker, cls, "admin"); err != nil {
			failed = append(failed, "class delete "+cls+": "+err.Error())
		} else {
			clsDeleted++
		}
	}
	resp := map[string]any{
		"ok": len(failed) == 0, "worker": worker, "assets_deleted": assets,
		"workflows_dropped": wfDropped, "classes_deleted": clsDeleted,
	}
	if len(failed) > 0 {
		resp["errors"] = failed
		writeJSON(w, http.StatusInternalServerError, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleControlAppDelete removes an app (namespace) and all its data.
func (s *Server) handleControlAppDelete(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace is required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	var workflows []string
	if proj, err := s.Control.Projection(r.Context()); err == nil {
		for _, wt := range proj.WorkflowTargets() {
			if wt.Namespace == ns {
				workflows = append(workflows, wt.Name)
			}
		}
	}
	if err := s.capturedWrite(r.Context(), control.GlobalScope(), func() error {
		return s.Control.DeleteApp(r.Context(), ns, "admin")
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "delete_failed", err.Error())
		return
	}
	s.invalidateBindings(ns)
	var failed []string
	deleted := map[string]int{}
	if s.Bucket != nil {
		assets := objectstore.NewOwned(s.Bucket, objectstore.PrefixAssets, objectstore.OwnerArtifacts)
		if keys, err := assets.ListPrefix(r.Context(), "assets/"+ns+"/"); err != nil {
			failed = append(failed, "asset list: "+err.Error())
		} else {
			for _, k := range keys {
				if err := assets.Delete(r.Context(), k); err != nil {
					failed = append(failed, "asset delete "+k+": "+err.Error())
				} else {
					deleted["assets"]++
				}
			}
		}
		cells := objectstore.NewOwned(s.Bucket, objectstore.PrefixCells, objectstore.OwnerReplica)
		if keys, err := cells.ListPrefix(r.Context(), "cells/"+ns+"/"); err != nil {
			failed = append(failed, "segment list: "+err.Error())
		} else {
			for _, k := range keys {
				if err := cells.Delete(r.Context(), k); err != nil {
					failed = append(failed, "segment delete "+k+": "+err.Error())
				} else {
					deleted["segments"]++
				}
			}
		}
	}
	wfDropped := 0
	for _, wf := range workflows {
		if err := s.Workflows.Drop(r.Context(), ns, wf); err != nil {
			failed = append(failed, "workflow drop "+wf+": "+err.Error())
		} else {
			wfDropped++
		}
	}
	if s.Store != nil {
		if err := s.Store.DeleteNamespace(r.Context(), ns); err != nil {
			failed = append(failed, "local cells: "+err.Error())
		}
	}
	resp := map[string]any{"ok": len(failed) == 0, "namespace": ns, "deleted": deleted, "workflows_dropped": wfDropped}
	if len(failed) > 0 {
		resp["errors"] = failed
		writeJSON(w, http.StatusInternalServerError, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleControlApps lists registered apps (namespaces).
func (s *Server) handleControlApps(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	apps, err := s.appsForRequest(r)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "apps_failed", err.Error())
		return
	}
	apps = filterByNS(allowedNS(r.Context()), apps, func(a control.App) string { return a.Namespace })
	apps, ok := pageSlice(w, r, apps)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"apps": apps})
}

// appsForRequest lists apps, fetching from the control-cell owner when this node
// is not the owner. The response is returned to the caller (not proxied) so the
// namespace filter always runs under the caller's own authorization.
func (s *Server) appsForRequest(r *http.Request) ([]control.App, error) {
	resp, forwarded, err := s.fetchViaOwner(r, control.Scope())
	if !forwarded {
		return s.Control.Apps(r.Context())
	}
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("owner apps: %s", resp.Status)
	}
	var out struct {
		Apps []control.App `json:"apps"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&out); err != nil {
		return nil, err
	}
	return out.Apps, nil
}

// handleControlResources lists a namespace's registered resources.
// handleControlResourceDelete revokes a resource registration (ADR-156). It
// refuses while a worker version still binds the resource unless force=1, so an
// accidental revoke cannot silently break a live deploy; the data cells are kept
// (a revoke stops resolution, it is not a purge).
func (s *Server) handleControlResourceDelete(w http.ResponseWriter, r *http.Request) {
	// Compatibility alias for the per-domain DELETE /v1/<kind>/resources
	// (ADR-157); the generic endpoint keeps reading the kind from the query.
	s.doResourceDelete(w, r, "")
}

func (s *Server) handleControlResources(w http.ResponseWriter, r *http.Request) {
	// Compatibility alias for GET /v1/<kind>/resources (ADR-157).
	s.doResourceList(w, r, "")
}

// handleControlCapacity returns the autoscaler's capacity recommendation from
// node lease signals (roadmap P4). Actual provisioning is external.
func (s *Server) handleControlCapacity(w http.ResponseWriter, r *http.Request) {
	if !s.requireAll(w, r) {
		return
	}
	if s.Advisor == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_advisor", "autoscaler not configured")
		return
	}
	plan, err := s.Advisor.Recommend(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "capacity_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

// handleControlReleases returns a worker's release log (newest first).
func (s *Server) handleControlReleases(w http.ResponseWriter, r *http.Request) {
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	ns, worker := r.URL.Query().Get("namespace"), r.URL.Query().Get("worker")
	if ns == "" || worker == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and worker are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	releases, err := s.Control.Releases(r.Context(), ns, worker)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "releases_failed", err.Error())
		return
	}
	releases, ok := pageSlice(w, r, releases)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "worker": worker, "releases": releases})
}

// handleControlRoutes serves the routing projection on the internal listener
// (:7001) for user-runtime's pure-pull refresh (ADR-031).
// projectionTTL bounds cross-node staleness of the cached routing projection: a
// node that did not perform the last control write rebuilds after this long.
const projectionTTL = time.Second

// projectionCached returns the routing projection, rebuilt only when the control
// store's revision changed or the TTL expired (ADR-117). The revision is bumped
// by every control write transaction, so a local write invalidates immediately;
// the TTL bounds cross-node staleness. It serves a stale copy when the rebuild
// fails, so an unavailable cell never takes routing down.
func (s *Server) projectionCached(ctx context.Context) (control.Projection, error) {
	if s.Control == nil {
		return control.Projection{}, errors.New("control plane not configured")
	}
	rev, rerr := s.Control.Rev(ctx)
	s.projMu.Lock()
	cached, builtRev, at := s.projCache, s.projBuiltRev, s.projAt
	fresh := cached != nil && rerr == nil && rev == builtRev && time.Since(at) < projectionTTL
	s.projMu.Unlock()
	if fresh {
		return *cached, nil
	}
	p, err := s.Control.Projection(ctx)
	if err != nil {
		if cached != nil {
			return *cached, nil // serve stale on error
		}
		return control.Projection{}, err
	}
	s.projMu.Lock()
	s.projCache = &p
	s.projBuiltRev = rev
	s.projAt = time.Now()
	s.projMu.Unlock()
	s.projRev.Store(rev)
	return p, nil
}

// controlEtag hashes a canonical view body. Callers store it opaquely and send
// it back as If-None-Match.
func controlEtag(data []byte) string {
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:16]) + `"`
}

func (s *Server) handleControlRoutes(w http.ResponseWriter, r *http.Request) {
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	p, err := s.projectionCached(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "projection_failed", err.Error())
		return
	}
	w.Header().Set("etag", p.ETag)
	if inm := r.Header.Get("if-none-match"); inm != "" && inm == p.ETag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// handleControlHost serves the small per-host routing view (ADR-115): routes
// plus a pointer to each referenced worker's active version. Unknown hosts get
// 404 with an ETag so the loader can cache the negative result. Bindings/vars
// are intentionally not part of this view (see handleControlWorker).
func (s *Server) handleControlHost(w http.ResponseWriter, r *http.Request) {
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	host := control.NormalizeHost(r.URL.Query().Get("host"))
	if host == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "host is required")
		return
	}
	// Unregistered hosts never serve traffic (ADR-133: registration is
	// authorization, so a registered host routes).
	if !s.hostRegistered(r.Context(), host) {
		body, _ := json.Marshal(map[string]any{"host": host, "reason": "host_not_registered"})
		w.Header().Set("content-type", "application/json")
		w.Header().Set("etag", controlEtag(body))
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write(body)
		return
	}
	p, err := s.projectionCached(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "projection_failed", err.Error())
		return
	}
	view := p.HostView(host)
	body, err := json.Marshal(view)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "marshal_failed", err.Error())
		return
	}
	etag := controlEtag(body)
	w.Header().Set("etag", etag)
	if inm := r.Header.Get("if-none-match"); inm != "" && inm == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("content-type", "application/json")
	if len(view.Routes) == 0 {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write(body)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// handleControlWorker serves the immutable per-version execution environment of
// a worker's active version (ADR-115) by point reads, so the loader can cache
// it for the version's lifetime.
func (s *Server) handleControlWorker(w http.ResponseWriter, r *http.Request) {
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	ns := r.URL.Query().Get("ns")
	worker := r.URL.Query().Get("worker")
	if ns == "" || worker == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "ns and worker are required")
		return
	}
	var view control.WorkerView
	var ok bool
	var err error
	if sha := r.URL.Query().Get("sha"); sha != "" {
		// A service binding pinned this bundle sha at deploy time (ADR-104);
		// resolve that exact immutable env.
		view, ok, err = s.Control.VersionEnvBySHA(r.Context(), ns, worker, sha)
	} else {
		version := 0
		if v := r.URL.Query().Get("version"); v != "" {
			n, cerr := strconv.Atoi(v)
			if cerr != nil || n < 0 {
				writeErr(w, http.StatusBadRequest, "bad_request", "version must be a non-negative integer")
				return
			}
			version = n
		}
		view, ok, err = s.Control.VersionEnv(r.Context(), ns, worker, version)
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "worker_env_failed", err.Error())
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "worker_not_found", "worker has no active version")
		return
	}
	body, err := json.Marshal(view)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "marshal_failed", err.Error())
		return
	}
	etag := controlEtag(body)
	w.Header().Set("etag", etag)
	if inm := r.Header.Get("if-none-match"); inm != "" && inm == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

type appCreateReq struct {
	Namespace string `json:"namespace"`
}

func (s *Server) handleControlApp(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	var req appCreateReq
	if !decodeControl(w, r, &req) {
		return
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	var app control.App
	if err := s.capturedWrite(r.Context(), control.GlobalScope(), func() error {
		var e error
		app, e = s.Control.CreateApp(r.Context(), req.Namespace, "admin")
		return e
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "app_create_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, app)
}

type resourceCreateReq struct {
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Scope     string `json:"scope"`
	// ConnectionString seals the origin URL of a hyperdrive resource (ADR-129).
	// Only valid for kind=hyperdrive; never stored in plaintext.
	ConnectionString string `json:"connection_string,omitempty"`
	// Config is the kind-specific resource config (kind=vectorize:
	// {dimensions, metric, description}). Sealed server-side like every other
	// resource config.
	Config json.RawMessage `json:"config,omitempty"`
}

func (s *Server) handleControlResource(w http.ResponseWriter, r *http.Request) {
	// Compatibility alias for POST /v1/<kind>/resources (ADR-157).
	s.doResourceCreate(w, r, "")
}

type deployReq struct {
	Namespace          string                `json:"namespace"`
	Worker             string                `json:"worker"`
	BundleSHA          string                `json:"bundle_sha"`
	AssetsSHA          string                `json:"assets_sha,omitempty"`
	Bindings           []control.Binding     `json:"bindings,omitempty"`
	Vars               map[string]string     `json:"vars,omitempty"`
	CompatibilityDate  string                `json:"compatibility_date,omitempty"`
	CompatibilityFlags []string              `json:"compatibility_flags,omitempty"`
	Consumers          []control.Consumer    `json:"consumers,omitempty"`
	Crons              []string              `json:"crons,omitempty"`
	Assets             *control.AssetsConfig `json:"assets,omitempty"`
	Migrations         []map[string]any      `json:"migrations,omitempty"`
	IdempotencyKey     string                `json:"idempotency_key,omitempty"`
	// SessionPolicy ("preserve" | "restart") selects the Durable Object restart
	// behavior on deploy (ADR-107). Empty honors CELLHIVE_DO_EAGER_RESTART.
	SessionPolicy string `json:"session_policy,omitempty"`
	// DryRun validates the deploy (compatibility gate, bundle existence,
	// service-binding ACL) and returns without touching any state (ADR-148).
	DryRun bool `json:"dry_run,omitempty"`
}

// validateDeploy applies the platform compatibility gate server-side (ADR-065).
// It returns the findings; the caller rejects the deploy when any are errors.
func (s *Server) validateDeploy(ctx context.Context, req deployReq) wranglercompat.Result {
	in := wranglercompat.Input{
		Namespace:          req.Namespace,
		Worker:             req.Worker,
		BundleSHA:          req.BundleSHA,
		AssetsSHA:          req.AssetsSHA,
		CompatibilityDate:  req.CompatibilityDate,
		CompatibilityFlags: req.CompatibilityFlags,
		Vars:               req.Vars,
		IsRegistered: func(kind, name string) bool {
			ok, err := s.Control.HasBinding(ctx, req.Namespace, kind, name)
			return err == nil && ok
		},
	}
	for _, b := range req.Bindings {
		in.Bindings = append(in.Bindings, wranglercompat.Binding{Type: b.Type, Name: b.Name, ID: b.ID, ClassName: b.ClassName, Entrypoint: b.Entrypoint})
	}
	res := wranglercompat.Validate(in)
	res.Errors = append(res.Errors, wranglercompat.ValidateMigrations(req.Migrations)...)
	res.Errors = append(res.Errors, wranglercompat.ValidateCrons(req.Crons)...)
	res.Errors = append(res.Errors, s.validateServiceBindings(ctx, req.Namespace, req.Bindings)...)

	// Bundle must exist in the object store (content-addressed). Deploy is an
	// infrequent admin path, so a point-read here is acceptable; a future
	// bucket Stat primitive can make this cheap (docs/dev-mode.md).
	if in.BundleSHA != "" && s.Bucket != nil {
		if _, err := s.artifactStore().GetBundle(ctx, in.BundleSHA); errors.Is(err, bucket.ErrNotFound) {
			res.Errors = append(res.Errors, wranglercompat.Finding{
				Severity: wranglercompat.SeverityError, Code: "missing_bundle", FieldPath: "bundle_sha",
				Message: "bundle " + in.BundleSHA + " not found in the object store",
			})
		} else if err != nil {
			res.Errors = append(res.Errors, wranglercompat.Finding{
				Severity: wranglercompat.SeverityError, Code: "bundle_check_failed", FieldPath: "bundle_sha",
				Message: err.Error(),
			})
		}
	}
	return res
}

func (s *Server) handleControlDeploy(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	var req deployReq
	if !decodeControl(w, r, &req) {
		return
	}
	if res := s.validateDeploy(r.Context(), req); !res.OK() {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":    "deploy_rejected",
			"findings": res.Errors,
		})
		return
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	if req.DryRun {
		// Validation only: no migrations, no app auto-create, no version.
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "dry_run": true, "worker": req.Worker, "namespace": req.Namespace})
		return
	}
	// Apply DO class migrations before activating the new version (ADR-082). Both
	// this write and the optional app auto-create go through capturedWrite so
	// they carry the same durability proof as the deploy itself (ADR-135).
	if err := s.capturedWrite(r.Context(), control.ScopeFor(req.Namespace), func() error {
		return s.applyDOMigrations(r.Context(), req)
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "migration_failed", err.Error())
		return
	}
	if s.Cfg.AutoCreateApp {
		// A deploy may bring its own namespace so a fresh install can deploy
		// straight into it (e.g. "root"); CreateApp is idempotent (ADR-131).
		if err := s.capturedWrite(r.Context(), control.ScopeFor(req.Namespace), func() error {
			_, err := s.Control.CreateApp(r.Context(), req.Namespace, actorFor(r.Context()))
			return err
		}); err != nil {
			writeErr(w, http.StatusBadRequest, "app_create_failed", err.Error())
			return
		}
	}
	var v control.Version
	if err := s.capturedWrite(r.Context(), control.ScopeFor(req.Namespace), func() error {
		var e error
		v, e = s.Control.Deploy(r.Context(), req.Namespace, req.Worker, control.DeploySpec{
			BundleSHA: req.BundleSHA, AssetsSHA: req.AssetsSHA, Assets: req.Assets,
			Bindings: req.Bindings, Vars: req.Vars, Consumers: req.Consumers, Crons: req.Crons,
			IdempotencyKey: req.IdempotencyKey,
			SessionPolicy:  req.SessionPolicy,
			CompatDate:     req.CompatibilityDate,
			CompatFlags:    req.CompatibilityFlags,
		}, "admin")
		return e
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "deploy_failed", err.Error())
		return
	}
	s.invalidateBindings(req.Namespace)
	// Built-in worker domain (ADR-131): deploy gives the worker an immediate
	// entry point without any DNS or certificate setup.
	s.ensureBuiltinHost(r.Context(), req.Namespace, req.Worker, "")
	// Durable Object restart policy (ADR-107): "restart" forces an eager
	// abort/restart, "preserve" keeps resident objects until they idle out;
	// empty honors CELLHIVE_DO_EAGER_RESTART (default false = lazy restart).
	if deployRestartPolicy(s.Cfg.DoEagerRestart, req.SessionPolicy) && v.StorageID != "" {
		s.restartDurableObjects(r.Context(), v.StorageID)
	}
	writeJSON(w, http.StatusOK, v)
}

type promoteReq struct {
	Namespace string `json:"namespace"`
	Worker    string `json:"worker"`
	Version   int    `json:"version"`
}

func (s *Server) handleControlPromote(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	var req promoteReq
	if !decodeControl(w, r, &req) {
		return
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	var wk control.Worker
	if err := s.capturedWrite(r.Context(), control.ScopeFor(req.Namespace), func() error {
		var e error
		wk, e = s.Control.Promote(r.Context(), req.Namespace, req.Worker, req.Version, actorFor(r.Context()))
		return e
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "promote_failed", err.Error())
		return
	}
	s.invalidateBindings(req.Namespace)
	s.ensureBuiltinHost(r.Context(), req.Namespace, req.Worker, "")
	writeJSON(w, http.StatusOK, wk)
}

type rollbackReq struct {
	Namespace string `json:"namespace"`
	Worker    string `json:"worker"`
}

func (s *Server) handleControlRollback(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	var req rollbackReq
	if !decodeControl(w, r, &req) {
		return
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	var wk control.Worker
	if err := s.capturedWrite(r.Context(), control.ScopeFor(req.Namespace), func() error {
		var e error
		wk, e = s.Control.Rollback(r.Context(), req.Namespace, req.Worker, actorFor(r.Context()))
		return e
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "rollback_failed", err.Error())
		return
	}
	s.invalidateBindings(req.Namespace)
	writeJSON(w, http.StatusOK, wk)
}

type routeReq struct {
	Namespace string `json:"namespace"`
	Host      string `json:"host"`
	Path      string `json:"path,omitempty"`
	Worker    string `json:"worker"`
}

func (s *Server) handleControlRoutePut(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	var req routeReq
	if !decodeControl(w, r, &req) {
		return
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	if err := s.capturedWrite(r.Context(), control.ScopeFor(req.Namespace), func() error {
		return s.Control.PutRoute(r.Context(), req.Namespace, control.Route{
			Host: req.Host, Path: req.Path, Worker: req.Worker,
		}, "admin")
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "route_put_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleControlRouteDelete(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	ns := r.URL.Query().Get("namespace")
	host := r.URL.Query().Get("host")
	if !s.authorizeNS(w, r, ns) {
		return
	}
	if err := s.capturedWrite(r.Context(), control.ScopeFor(ns), func() error {
		return s.Control.DeleteRoute(r.Context(), ns, host, "admin")
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "route_delete_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type secretPutReq struct {
	Namespace string `json:"namespace"`
	Worker    string `json:"worker"`
	Key       string `json:"key"`
	Value     string `json:"value"` // base64
}

func (s *Server) handleControlSecretPut(w http.ResponseWriter, r *http.Request) {
	if !s.requireControl(w) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	var req secretPutReq
	if !decodeControl(w, r, &req) {
		return
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	val, err := base64.StdEncoding.DecodeString(req.Value)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_value", "value must be base64")
		return
	}
	if err := s.capturedWrite(r.Context(), control.ScopeFor(req.Namespace), func() error {
		return s.Control.PutSecret(r.Context(), req.Namespace, req.Worker, req.Key, val, "admin")
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "secret_put_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleControlSecretDelete removes a secret (audited).
func (s *Server) handleControlSecretDelete(w http.ResponseWriter, r *http.Request) {
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	q := r.URL.Query()
	ns, worker, key := q.Get("namespace"), q.Get("worker"), q.Get("key")
	if ns == "" || worker == "" || key == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace, worker and key are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	if err := s.capturedWrite(r.Context(), control.ScopeFor(ns), func() error {
		return s.Control.DeleteSecret(r.Context(), ns, worker, key, actorFor(r.Context()))
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "secret_delete_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleControlSecretList lists a worker's secret keys (never the values).
func (s *Server) handleControlSecretList(w http.ResponseWriter, r *http.Request) {
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	q := r.URL.Query()
	ns, worker := q.Get("namespace"), q.Get("worker")
	if ns == "" || worker == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and worker are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	metas, err := s.Control.ListSecrets(r.Context(), ns, worker)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "secret_list_failed", err.Error())
		return
	}
	if metas == nil {
		metas = []control.SecretMeta{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"secrets": metas})
}

func (s *Server) handleControlSecretGet(w http.ResponseWriter, r *http.Request) {
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	ns, worker, key := r.URL.Query().Get("namespace"), r.URL.Query().Get("worker"), r.URL.Query().Get("key")
	if !s.authorizeNS(w, r, ns) {
		return
	}
	val, err := s.Control.GetSecret(r.Context(), ns, worker, key)
	if err != nil {
		writeErr(w, http.StatusNotFound, "secret_get_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"value": base64.StdEncoding.EncodeToString(val)})
}

func (s *Server) handleControlAudit(w http.ResponseWriter, r *http.Request) {
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	q := r.URL.Query()
	ns := q.Get("namespace")
	limit := 100
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
			if limit > 1000 {
				limit = 1000
			}
		}
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	var since int64
	if v := q.Get("since_ms"); v != "" {
		since, _ = strconv.ParseInt(v, 10, 64)
	}
	audit, err := s.Control.AuditQuery(r.Context(), ns, limit, since)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "audit_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"audit": audit})
}

// applyDOMigrations records class renames/deletes before the new version goes
// active (ADR-082).
func (s *Server) applyDOMigrations(ctx context.Context, req deployReq) error {
	for _, m := range req.Migrations {
		if list, ok := m["renamed_classes"].([]any); ok {
			for _, item := range list {
				mm, _ := item.(map[string]any)
				from, _ := mm["from"].(string)
				to, _ := mm["to"].(string)
				if err := s.Control.RenameClass(ctx, req.Namespace, req.Worker, from, to, "admin"); err != nil {
					return err
				}
			}
		}
		if list, ok := m["deleted_classes"].([]any); ok {
			for _, item := range list {
				cls, _ := item.(string)
				if err := s.Control.DeleteClass(ctx, req.Namespace, req.Worker, cls, "admin"); err != nil {
					return err
				}
			}
		}
		// Same-worker transfer moves the class's storage identity to `to`
		// (equivalent to rename; conflicts with an existing class fail closed).
		if list, ok := m["transferred_classes"].([]any); ok {
			for _, item := range list {
				mm, _ := item.(map[string]any)
				from, _ := mm["from"].(string)
				to, _ := mm["to"].(string)
				if err := s.Control.RenameClass(ctx, req.Namespace, req.Worker, from, to, "admin"); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// deployRestartPolicy decides whether a deploy eagerly restarts a worker's
// Durable Objects: "restart" forces it, "preserve" forbids it, empty honors the
// configured default (ADR-107).
func deployRestartPolicy(eager bool, policy string) bool {
	switch policy {
	case "restart":
		return true
	case "preserve":
		return false
	}
	return eager
}

// restartDurableObjects asks each do-runtime to eagerly abort the storage id's
// resident objects (ADR-082). Best effort.
func (s *Server) restartDurableObjects(ctx context.Context, storageID string) {
	body, err := json.Marshal(map[string]string{"storage_id": storageID})
	if err != nil {
		return
	}
	client := s.ForwardClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	failed := 0
	for _, rt := range s.Cfg.DoRuntimes {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(rt, "/")+"/v1/do/restart", bytes.NewReader(body))
		if err != nil {
			failed++
			continue
		}
		req.Header.Set("content-type", "application/json")
		req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenInternal)
		resp, err := client.Do(req)
		if err != nil {
			failed++
			if s.Log != nil {
				s.Log.Warn("do restart failed", "runtime", rt, "storage_id", storageID, "err", err)
			}
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			failed++
			if s.Log != nil {
				s.Log.Warn("do restart rejected", "runtime", rt, "storage_id", storageID, "status", resp.StatusCode)
			}
		}
	}
	if failed > 0 && s.Log != nil && failed == len(s.Cfg.DoRuntimes) {
		// The deploy already succeeded (lazy restart semantics), but silence here
		// would hide that no runtime accepted the restart request.
		s.Log.Warn("do restart: no runtime accepted the request", "storage_id", storageID, "runtimes", len(s.Cfg.DoRuntimes))
	}
}
