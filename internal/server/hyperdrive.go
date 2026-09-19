package server

import (
	"context"
	"net/http"
	"strings"
)

// isHyperdriveConnString reports whether s is an acceptable origin URL for a
// hyperdrive resource.
func isHyperdriveConnString(s string) bool {
	low := strings.ToLower(strings.TrimSpace(s))
	return strings.HasPrefix(low, "postgres://") ||
		strings.HasPrefix(low, "postgresql://") ||
		strings.HasPrefix(low, "mysql://")
}

// hyperdriveRef resolves a hyperdrive reference to its origin connection
// string. An inline URL (has a scheme) wins for back-compat with
// `--hyperdrive NAME=URL`; otherwise the reference names a registered
// hyperdrive resource whose connection string is sealed in the control plane
// (ADR-129). ok=false when neither resolves.
func (s *Server) hyperdriveRef(ctx context.Context, ns, ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", false
	}
	if strings.Contains(ref, "://") {
		return ref, true
	}
	if s.Control == nil {
		return "", false
	}
	b, err := s.Control.ResourceConfig(ctx, ns, "hyperdrive", ref)
	if err != nil {
		return "", false
	}
	cs := strings.TrimSpace(string(b))
	if cs == "" {
		return "", false
	}
	return cs, true
}

// hyperdriveSpec resolves a hyperdrive binding (id first, then the binding name)
// to the spec entry the runtime injects as env.<binding> (ADR-125/129).
func (s *Server) hyperdriveSpec(ctx context.Context, ns, name, id string) (map[string]any, bool) {
	for _, ref := range []string{id, name} {
		if cs, ok := s.hyperdriveRef(ctx, ns, ref); ok {
			return map[string]any{"kind": "hyperdrive", "ns": ns, "name": name, "connectionString": cs}, true
		}
	}
	return nil, false
}

// handleInternalHyperdrive returns a registered hyperdrive origin connection
// string (internal; a cold path, resolved once per loaded worker).
func (s *Server) handleInternalHyperdrive(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ns, name := q.Get("ns"), q.Get("name")
	if ns == "" || name == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "ns and name are required")
		return
	}
	cs, ok := s.hyperdriveRef(r.Context(), ns, name)
	if !ok {
		writeErr(w, http.StatusNotFound, "not_found", "no hyperdrive resource "+ns+"/"+name)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"connection_string": cs})
}
