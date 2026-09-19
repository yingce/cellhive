package server

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"cellhive/internal/scopedtoken"
)

// doBindingSpec builds the tenant binding spec and vars for a worker's active
// version (see bindingSpecFor).
func (s *Server) doBindingSpec(ctx context.Context, ns, worker string) (map[string]any, map[string]string) {
	return s.bindingSpecFor(ctx, ns, worker, 0)
}

// bindingSpecFor builds the tenant binding spec (SCOPE_SPEC) and vars a loaded
// worker needs so its `env` exposes the worker's bindings, mirroring
// user-runtime loader.js bindingSpec. cell-agent is authoritative for the scoped
// tokens. version <= 0 means the active version; a specific version resolves
// that exact immutable env (ADR-128).
func (s *Server) bindingSpecFor(ctx context.Context, ns, worker string, version int) (map[string]any, map[string]string) {
	if s.Control == nil {
		return nil, nil
	}
	v, ok, err := s.Control.VersionEnv(ctx, ns, worker, version)
	if err != nil || !ok {
		return nil, nil
	}
	deleted := map[string]bool{}
	for _, d := range v.DeletedClasses {
		deleted[d] = true
	}
	spec := map[string]any{}
	for _, b := range v.Bindings {
		token, err := scopedtoken.Mint([]byte(s.Cfg.ScopeSecret), scopedtoken.Claims{
			Namespace: ns, Kind: b.Type, Name: b.Name,
		})
		if err != nil {
			continue
		}
		switch b.Type {
		case "do":
			cls := b.ID
			if cls == "" {
				cls = b.Name
			}
			storageClass := cls
			if alt, ok := v.ClassStorage[cls]; ok && alt != "" {
				storageClass = alt
			}
			spec[b.Name] = map[string]any{
				"kind": "do", "ns": ns, "worker": worker, "bundle_sha": v.BundleSHA, "version": v.Version,
				"compat_date": v.CompatDate, "compat_flags": v.CompatFlags,
				"storage_id": v.StorageID, "class": cls, "storage_class": storageClass,
				"deleted": deleted[cls], "name": b.Name, "token": token,
			}
		case "vectorize":
			// The resource is the index name; the binding name is the env key.
			index := b.ID
			if index == "" {
				index = b.Name
			}
			itok, err := scopedtoken.Mint([]byte(s.Cfg.ScopeSecret), scopedtoken.Claims{
				Namespace: ns, Kind: "vectorize", Name: index,
			})
			if err != nil {
				continue
			}
			spec[b.Name] = map[string]any{
				"kind": "vectorize", "ns": ns, "name": b.Name, "index": index, "token": itok,
			}
		case "kv", "d1", "r2", "queue", "workflow":
			spec[b.Name] = map[string]any{"kind": b.Type, "ns": ns, "name": b.Name, "token": token}
		case "ai":
			spec[b.Name] = map[string]any{"kind": "ai", "ns": ns, "name": b.Name}
		case "hyperdrive":
			if entry, ok := s.hyperdriveSpec(ctx, ns, b.Name, b.ID); ok {
				spec[b.Name] = entry
			}
		case "service":
			target := b.ID
			if target == "" {
				target = b.Name
			}
			// "worker" is same-namespace; "ns/worker" is a cross-namespace target
			// (deploy-time gate + runtime ACL, ADR-144).
			targetNS := ns
			if i := strings.Index(target, "/"); i > 0 && i < len(target)-1 {
				targetNS, target = target[:i], target[i+1:]
			}
			spec[b.Name] = map[string]any{"kind": "service", "ns": targetNS, "caller_ns": ns, "name": b.Name, "target": target, "entrypoint": b.Entrypoint, "token": token}
		}
	}
	return spec, v.Vars
}

// handleInternalBindings returns the binding spec a runtime injects into a
// tenant worker's env (internal; called once per isolate/facet at creation, a
// cold path). Query: ns, worker, optional version (0/absent = active). Served at
// /v1/internal/worker/bindings and, for the do-runtime, /v1/internal/do/bindings
// (ADR-089/128).
func (s *Server) handleInternalBindings(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ns, worker := q.Get("ns"), q.Get("worker")
	if ns == "" || worker == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "ns and worker are required")
		return
	}
	version := 0
	if vs := q.Get("version"); vs != "" {
		n, err := strconv.Atoi(vs)
		if err != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, "bad_request", "version must be a non-negative integer")
			return
		}
		version = n
	}
	view, ok, verr := s.Control.VersionEnv(r.Context(), ns, worker, version)
	if verr != nil || !ok {
		writeJSON(w, http.StatusOK, map[string]any{"bindings": map[string]any{}, "vars": map[string]any{}})
		return
	}
	spec, vars := s.bindingSpecFor(r.Context(), ns, worker, version)
	if spec == nil {
		writeJSON(w, http.StatusOK, map[string]any{"bindings": map[string]any{}, "vars": map[string]any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"bindings": spec, "vars": vars,
		"compat_date": view.CompatDate, "compat_flags": view.CompatFlags,
	})
}
