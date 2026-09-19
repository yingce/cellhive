package server

import (
	"context"
	"net/http"
	"regexp"
	"strings"

	"cellhive/internal/control"
)

// hostLabelRe matches a DNS label usable inside the built-in domain.
var hostLabelRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// hostLabel derives a worker's built-in domain label. The stored host_label wins;
// otherwise `<ns>-<worker>` is used when both are DNS-label safe.
func hostLabel(ns, worker, stored string) string {
	if stored != "" {
		if hostLabelRe.MatchString(stored) && len(stored) <= 63 {
			return stored
		}
		return ""
	}
	label := strings.ToLower(ns + "-" + worker)
	if len(label) > 63 || !hostLabelRe.MatchString(label) {
		return ""
	}
	return label
}

// builtinHost returns `<label>.<base>` for a worker, or "" when built-in domains
// are disabled or the label cannot be represented as a DNS label.
func (s *Server) builtinHost(ns, worker, stored string) string {
	if s.Cfg.BaseDomain == "" {
		return ""
	}
	label := hostLabel(ns, worker, stored)
	if label == "" {
		return ""
	}
	return label + "." + s.Cfg.BaseDomain
}

// WorkerHostLabel exposes the derived built-in domain label for CLI/console use.
func (s *Server) WorkerHostLabel(ns, worker string) string {
	view, ok, err := s.Control.WorkerEnv(context.Background(), ns, worker)
	if err != nil || !ok {
		return ""
	}
	return hostLabel(ns, worker, view.HostLabel)
}

// ensureBuiltinHost materializes <label>.<base> → worker (idempotent). Called on
// deploy/promote so a worker always has an immediate entry point.
func (s *Server) ensureBuiltinHost(ctx context.Context, ns, worker, stored string) {
	host := s.builtinHost(ns, worker, stored)
	if host == "" {
		return
	}
	if err := s.Control.EnsureBuiltinHost(ctx, ns, worker, host, ""); err != nil && s.Log != nil {
		s.Log.Warn("builtin host", "ns", ns, "worker", worker, "host", host, "err", err)
	}
}

// Custom domains are registered, not DNS-verified (deferred; see ADR-133): the
// control-plane credential that manages the namespace is the authority for its
// hosts. verify_state/verify_method stay reserved so a challenge flow can be
// reintroduced later.

// --- admin endpoints --------------------------------------------------------

type domainReq struct {
	Namespace string `json:"namespace"`
	Host      string `json:"host"`
}

// handleControlDomainAdd registers a custom domain and returns its challenge.
func (s *Server) handleControlDomainAdd(w http.ResponseWriter, r *http.Request) {
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	var req domainReq
	if !decodeControl(w, r, &req) {
		return
	}
	if req.Namespace == "" || req.Host == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and host are required")
		return
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	// The built-in domain space is system-owned (ADR-131): custom claims under
	// the base domain are rejected so tenants cannot shadow built-in hosts.
	if base := strings.ToLower(strings.TrimSpace(s.Cfg.BaseDomain)); base != "" {
		h := control.NormalizeHost(req.Host)
		if h == base || strings.HasSuffix(h, "."+base) {
			writeErr(w, http.StatusConflict, "host_reserved", "host is inside the platform's built-in domain "+base)
			return
		}
	}
	h, err := s.Control.PutCustomHost(r.Context(), req.Namespace, req.Host, "admin")
	if err != nil {
		writeErr(w, http.StatusConflict, "domain_rejected", err.Error())
		return
	}
	s.invalidateBindings(req.Namespace)
	writeJSON(w, http.StatusOK, h)
}

// handleControlDomainList lists a namespace's domains.
func (s *Server) handleControlDomainList(w http.ResponseWriter, r *http.Request) {
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
	hosts, err := s.Control.Hosts(r.Context(), ns)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "domains_failed", err.Error())
		return
	}
	if hosts == nil {
		hosts = []control.Host{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"domains": hosts})
}

// handleControlDomainDelete removes a custom domain and its routes.
func (s *Server) handleControlDomainDelete(w http.ResponseWriter, r *http.Request) {
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	ns := r.URL.Query().Get("namespace")
	host := r.URL.Query().Get("host")
	if ns == "" || host == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and host are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	if err := s.Control.DeleteHost(r.Context(), ns, host, "admin"); err != nil {
		writeErr(w, http.StatusConflict, "domain_delete_failed", err.Error())
		return
	}
	s.invalidateBindings(ns)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "host": control.NormalizeHost(host)})
}

// hostRegistered reports whether a host is registered in the control plane
// (ADR-133: registration is authorization, so a registered host routes).
func (s *Server) hostRegistered(ctx context.Context, host string) bool {
	_, err := s.Control.GetHost(ctx, host)
	return err == nil
}
