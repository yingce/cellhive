package server

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"cellhive/internal/hyperdriveauth"
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
			cs, ok = s.hyperdriveBindingConnString(cs, ns, name)
			if !ok {
				return nil, false
			}
			return map[string]any{"kind": "hyperdrive", "ns": ns, "name": name, "connectionString": cs}, true
		}
	}
	return nil, false
}

// hyperdriveProxyFor returns the pooler endpoint for one origin scheme.
// CELLHIVE_HYPERDRIVE_PROXY accepts a bare host:port (PostgreSQL back-compat)
// or comma-separated scheme://host:port entries, so one variable can carry
// both postgres:// and mysql:// endpoints:
//
//	proxysql:6133
//	mysql://proxysql:6033
//	postgres://proxysql:6133,mysql://proxysql:6033
//
// A scheme with no matching entry fails closed.
func (s *Server) hyperdriveProxyFor(scheme string) string {
	raw := strings.TrimSpace(s.Cfg.HyperdriveProxy)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.Contains(part, "://") {
			// Back-compat: bare host:port is the PostgreSQL endpoint.
			if scheme == "postgres" || scheme == "postgresql" {
				return part
			}
			continue
		}
		u, err := url.Parse(part)
		if err != nil || u.Host == "" {
			continue
		}
		switch strings.ToLower(u.Scheme) {
		case "postgres", "postgresql":
			if scheme == "postgres" || scheme == "postgresql" {
				return u.Host
			}
		case "mysql":
			if scheme == "mysql" {
				return u.Host
			}
		}
	}
	return ""
}

// hyperdriveBindingConnString applies the deployment-level Hyperdrive mode.
// "direct" returns the origin URL unchanged (back-compat default); "proxy"
// rewrites the host/port to the protocol's pooler without changing the
// database/user/password or query parameters. PostgreSQL and MySQL can target
// separate endpoints of a protocol-aware pooler (ProxySQL/MaxScale). A missing
// endpoint for the origin's protocol fails closed. HyperdriveDBMode=binding
// only applies to PostgreSQL; MySQL keeps the origin schema/database.
func (s *Server) hyperdriveBindingConnString(origin, namespace, resource string) (string, bool) {
	mode := strings.ToLower(strings.TrimSpace(s.Cfg.HyperdriveMode))
	if mode == "" {
		mode = "direct"
	}
	if mode != "direct" && mode != "proxy" {
		return "", false
	}
	u, err := url.Parse(strings.TrimSpace(origin))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	if s.Cfg.HyperdriveManagedUsers {
		c := hyperdriveauth.Derive(s.Cfg.ScopeSecret, namespace, resource)
		if c.User == "" {
			return "", false
		}
		u.User = url.UserPassword(c.User, c.Password)
	}
	if mode == "direct" {
		return u.String(), true
	}
	scheme := strings.ToLower(u.Scheme)
	proxyHost := s.hyperdriveProxyFor(scheme)
	if proxyHost == "" {
		return "", false
	}
	u.Host = proxyHost
	if scheme != "mysql" && strings.EqualFold(strings.TrimSpace(s.Cfg.HyperdriveDBMode), "binding") {
		u.Path = "/" + hyperdriveLogicalDB(resource)
		u.RawPath = ""
	}
	return u.String(), true
}

// hyperdriveLogicalDB is the stable PostgreSQL pooler database key for a resource.
func hyperdriveLogicalDB(resource string) string {
	resource = strings.ToLower(strings.TrimSpace(resource))
	var b strings.Builder
	b.WriteString("hd_")
	for _, r := range resource {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "hd" || out == "" {
		return "hd_default"
	}
	if len(out) > 60 {
		out = out[:60]
	}
	return out
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
	cs, ok = s.hyperdriveBindingConnString(cs, ns, name)
	if !ok {
		writeErr(w, http.StatusServiceUnavailable, "hyperdrive_proxy_unavailable", "Hyperdrive proxy mode is enabled but the configured pooler is not usable")

		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"connection_string": cs})
}
