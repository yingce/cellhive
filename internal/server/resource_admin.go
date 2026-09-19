package server

import (
	"net/http"

	"cellhive/internal/control"
	"cellhive/internal/vectorize"
)

// Per-domain resource registry endpoints (ADR-157). Each binding kind exposes
// the same registry operations under its own path:
//
//	POST   /v1/<kind>/resources   register a resource
//	GET    /v1/<kind>/resources   list resources (namespace required)
//	DELETE /v1/<kind>/resources   revoke a resource (+ ?force=1)
//	GET    /v1/<kind>/stats       cheap operator stats (see stats.go)
//
// The kind is fixed by the path, so clients never repeat it in the body. The
// generic /v1/control/resource* endpoints remain as compatibility aliases and
// delegate here with kind "".

// registryKinds are the kinds that own a resource registry entry (kind=do has
// no registry).
var registryKinds = []string{"kv", "d1", "queue", "r2", "workflow", "hyperdrive", "vectorize"}

// statsKinds additionally covers kinds without a registry entry.
var statsKinds = []string{"kv", "d1", "queue", "r2", "workflow", "hyperdrive", "vectorize", "do"}

// registerResourceRoutes wires one set of registry+stats routes per kind. wrap
// is the auth middleware of the mux being built (internal s.auth / admin
// s.adminAuth), so the same surface is available on both listeners.
func (s *Server) registerResourceRoutes(mux *http.ServeMux, wrap func(http.HandlerFunc) http.HandlerFunc) {
	for _, kind := range registryKinds {
		k := kind
		mux.HandleFunc("POST /v1/"+k+"/resources", wrap(s.resourceCreate(k)))
		mux.HandleFunc("GET /v1/"+k+"/resources", wrap(s.resourceList(k)))
		mux.HandleFunc("DELETE /v1/"+k+"/resources", wrap(s.resourceDelete(k)))
	}
	for _, kind := range statsKinds {
		k := kind
		mux.HandleFunc("GET /v1/"+k+"/stats", wrap(s.resourceStats(k)))
	}
	// Queue dead-letter replay lives with the rest of the queue operations.
	mux.HandleFunc("POST /v1/queue/dead-letters/replay", wrap(s.handleControlQueueReplayDLQ))
}

func (s *Server) resourceCreate(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { s.doResourceCreate(w, r, kind) }
}

func (s *Server) resourceList(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { s.doResourceList(w, r, kind) }
}

func (s *Server) resourceDelete(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { s.doResourceDelete(w, r, kind) }
}

// doResourceCreate registers a resource. kind=="" reads the kind from the body
// (compatibility endpoint); otherwise the path kind wins and a conflicting body
// kind is rejected.
func (s *Server) doResourceCreate(w http.ResponseWriter, r *http.Request, kind string) {
	if !s.requireControl(w) {
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	var req resourceCreateReq
	if !decodeControl(w, r, &req) {
		return
	}
	if kind != "" {
		if req.Kind != "" && req.Kind != kind {
			writeErr(w, http.StatusBadRequest, "bad_request",
				"body kind "+req.Kind+" does not match the /v1/"+kind+"/resources path")
			return
		}
		req.Kind = kind
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	if req.Scope == "" {
		req.Scope = defaultResourceScope(req.Namespace, req.Kind, req.Name)
	}
	var cfg []byte
	if len(req.Config) > 0 && string(req.Config) != "null" {
		if req.Kind != "vectorize" {
			writeErr(w, http.StatusBadRequest, "bad_request", "config is only valid for kind=vectorize")
			return
		}
		vc, err := vectorize.ParseConfig(req.Config)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "vectorize_bad_config", err.Error())
			return
		}
		if err := vc.Validate(); err != nil {
			writeErr(w, http.StatusBadRequest, "vectorize_bad_config", err.Error())
			return
		}
		cfg = req.Config
	}
	if req.Kind == "vectorize" && len(cfg) == 0 {
		writeErr(w, http.StatusBadRequest, "vectorize_bad_config",
			"vectorize resources require config {dimensions, metric} (e.g. --dimensions 768 --metric cosine)")
		return
	}
	if req.ConnectionString != "" {
		if req.Kind != "hyperdrive" {
			writeErr(w, http.StatusBadRequest, "bad_request", "connection_string is only valid for kind=hyperdrive")
			return
		}
		if !isHyperdriveConnString(req.ConnectionString) {
			writeErr(w, http.StatusBadRequest, "bad_request",
				"connection_string must be a postgres://, postgresql:// or mysql:// URL")
			return
		}
		cfg = []byte(req.ConnectionString)
	}
	var res control.Resource
	if err := s.capturedWrite(r.Context(), control.ScopeFor(req.Namespace), func() error {
		var e error
		res, e = s.Control.CreateResourceWithConfig(r.Context(), req.Namespace, req.Kind, req.Name, req.Scope, cfg, "admin")
		return e
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "resource_create_failed", err.Error())
		return
	}
	s.invalidateBindings(req.Namespace)
	if req.Kind == "vectorize" {
		s.invalidateVectorizeConfig(req.Namespace, req.Name)
	}
	writeJSON(w, http.StatusOK, res)
}

// doResourceList lists registry entries of one kind.
func (s *Server) doResourceList(w http.ResponseWriter, r *http.Request, kind string) {
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
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
	filter := kind
	if filter == "" {
		filter = r.URL.Query().Get("kind")
	}
	res, err := s.Control.Resources(r.Context(), ns, filter)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resources_failed", err.Error())
		return
	}
	res, ok := pageSlice(w, r, res)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"namespace": ns, "kind": filter, "resources": res})
}

// doResourceDelete revokes a registry entry (ADR-156 semantics).
func (s *Server) doResourceDelete(w http.ResponseWriter, r *http.Request, kind string) {
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	q := r.URL.Query()
	ns, name := q.Get("namespace"), q.Get("name")
	if kind == "" {
		kind = q.Get("kind")
	}
	if ns == "" || kind == "" || name == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace, kind and name are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	scope, ok, err := s.Control.Binding(r.Context(), ns, kind, name)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resource_lookup_failed", err.Error())
		return
	}
	// The live declaration may come from a worker version rather than the
	// registry; the referencing set tells us whether revoking breaks something.
	refs, rerr := s.Control.ResourceReferencedBy(r.Context(), ns, kind, name, scope)
	if rerr != nil {
		writeErr(w, http.StatusInternalServerError, "resource_lookup_failed", rerr.Error())
		return
	}
	if len(refs) > 0 && !truthy(q.Get("force")) {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "resource_in_use", "namespace": ns, "kind": kind, "name": name,
			"referenced_by": refs,
			"message":       "workers still bind this resource; revoke with force=1 to stop them (their loads will fail closed)",
		})
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "resource_not_found", "no registered "+kind+" resource named "+name)
		return
	}
	if err := s.capturedWrite(r.Context(), control.ScopeFor(ns), func() error {
		return s.Control.DeleteResource(r.Context(), ns, kind, name, actorFor(r.Context()))
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "resource_delete_failed", err.Error())
		return
	}
	s.invalidateBindings(ns)
	if kind == "vectorize" {
		s.invalidateVectorizeConfig(ns, name)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "namespace": ns, "kind": kind, "name": name,
		"referenced_by": refs, "forced": len(refs) > 0,
	})
}

// defaultResourceScope mirrors the CLI default: <ns>/__<kind>__/<name>.
func defaultResourceScope(ns, kind, name string) string {
	if ns == "" || kind == "" || name == "" {
		return ""
	}
	return ns + "/__" + kind + "__/" + name
}
