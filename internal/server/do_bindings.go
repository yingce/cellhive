package server

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"cellhive/internal/control"
	"cellhive/internal/scopedtoken"
)

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
		claims := scopedtoken.Claims{Namespace: ns, Kind: b.Type, Name: b.Name}
		if b.Type == "kv" && b.ID != "" {
			claims.Worker = worker
			claims.Version = v.Version
			claims.Resource = b.ID
		}
		token, err := scopedtoken.Mint([]byte(s.Cfg.ScopeSecret), claims)
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
				"limits":     v.Limits,
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
		case "queue":
			// A queue binding's name is arbitrary; the actual queue is the
			// resource ID. Scope the token to the resource so producer sends
			// land in the same queue the consumer projection references.
			qname := b.ID
			if qname == "" {
				qname = b.Name
			}
			qtok := token
			if qname != b.Name {
				qtok, err = scopedtoken.Mint([]byte(s.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: ns, Kind: "queue", Name: qname})
				if err != nil {
					continue
				}
			}
			spec[b.Name] = map[string]any{"kind": "queue", "ns": ns, "name": qname, "token": qtok}
		case "kv", "d1", "r2", "workflow":
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
	vars, err := s.runtimeVars(ctx, ns, worker, v.Version, v.Vars)
	if err != nil {
		return nil, nil
	}
	return spec, vars
}

func (s *Server) runtimeVars(ctx context.Context, ns, worker string, version int, declared map[string]string) (map[string]string, error) {
	vars := make(map[string]string, len(declared))
	for k, val := range declared {
		vars[k] = val
	}
	secrets, err := s.Control.VersionSecrets(ctx, ns, worker, version)
	if err != nil {
		return nil, err
	}
	for key, val := range secrets {
		vars[key] = val
	}
	return vars, nil
}

// currentRuntimeVars is used only before a deploy/secret write. Execution
// views always use the immutable per-version secret snapshot above.
func (s *Server) currentRuntimeVars(ctx context.Context, ns, worker string, declared map[string]string) map[string]string {
	vars := make(map[string]string, len(declared))
	for key, val := range declared {
		vars[key] = val
	}
	if metas, err := s.Control.ListSecrets(ctx, ns, worker); err == nil {
		for _, meta := range metas {
			if val, err := s.Control.GetSecret(ctx, ns, worker, meta.Key); err == nil {
				vars[meta.Key] = string(val)
			}
		}
	}
	return vars
}

// handleInternalBindings returns the binding spec a runtime injects into a
// tenant worker's env (internal; called once per isolate/facet at creation, a
// cold path). Query: ns, worker, optional version (0/absent = active). Served at
// /v1/internal/worker/bindings and, for the do-runtime, /v1/internal/do/bindings
// (ADR-089/128).
func (s *Server) handleInternalBindings(w http.ResponseWriter, r *http.Request) {
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
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
	// Pin the already-resolved version: an intervening deploy must not pair
	// version A's identity with the new active version B's variables/bindings.
	spec, vars := s.bindingSpecFor(r.Context(), ns, worker, view.Version)
	if spec == nil {
		writeErr(w, http.StatusServiceUnavailable, "worker_secret_failed", "worker bindings or secrets unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"bindings": spec, "vars": vars,
		"version": view.Version, "bundle_sha": view.BundleSHA, "storage_id": view.StorageID,
		"compat_date": view.CompatDate, "compat_flags": view.CompatFlags,
		"limits": view.Limits,
	})
}
