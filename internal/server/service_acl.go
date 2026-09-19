package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"cellhive/internal/control"
	"cellhive/internal/wranglercompat"
)

// service binding ACL (ADR-144). A service binding target is "worker"
// (same namespace, always allowed) or "ns/worker" (cross-namespace, requires a
// grant from the target namespace). Grants live in control.service_acls and are
// enforced both at deploy time and on every runtime call.

type serviceACLReq struct {
	Namespace string `json:"namespace"`
	Worker    string `json:"worker"`
	CallerNS  string `json:"caller_namespace"`
}

// handleControlServiceACLPut grants caller_namespace access to ns/worker.
func (s *Server) handleControlServiceACLPut(w http.ResponseWriter, r *http.Request) {
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	var req serviceACLReq
	if !decodeControl(w, r, &req) {
		return
	}
	if req.Namespace == "" || req.Worker == "" || req.CallerNS == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace, worker and caller_namespace are required")
		return
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	if err := s.capturedWrite(r.Context(), control.ScopeFor(req.Namespace), func() error {
		return s.Control.PutServiceACL(r.Context(), req.Namespace, req.Worker, req.CallerNS, actorFor(r.Context()))
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "service_acl_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "namespace": req.Namespace, "worker": req.Worker, "caller_namespace": req.CallerNS})
}

// handleControlServiceACLDelete revokes a grant.
func (s *Server) handleControlServiceACLDelete(w http.ResponseWriter, r *http.Request) {
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	q := r.URL.Query()
	ns, worker, caller := q.Get("namespace"), q.Get("worker"), q.Get("caller_namespace")
	if ns == "" || worker == "" || caller == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace, worker and caller_namespace are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	if err := s.capturedWrite(r.Context(), control.ScopeFor(ns), func() error {
		return s.Control.DeleteServiceACL(r.Context(), ns, worker, caller, actorFor(r.Context()))
	}); err != nil {
		writeErr(w, http.StatusBadRequest, "service_acl_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleControlServiceACLList lists a namespace's grants.
func (s *Server) handleControlServiceACLList(w http.ResponseWriter, r *http.Request) {
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
	acls, err := s.Control.ListServiceACLs(r.Context(), ns)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "service_acl_failed", err.Error())
		return
	}
	if acls == nil {
		acls = []control.ServiceACL{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"service_acls": acls})
}

// validateServiceBindings enforces the deploy-time half of the service-binding
// ACL: a cross-namespace target must be granted by the target namespace. Same
// namespace targets need no grant.
func (s *Server) validateServiceBindings(ctx context.Context, ns string, bindings []control.Binding) []wranglercompat.Finding {
	var out []wranglercompat.Finding
	for i, b := range bindings {
		if b.Type != "service" {
			continue
		}
		target := b.ID
		if target == "" {
			target = b.Name
		}
		path := fmt.Sprintf("bindings[%d].service", i)
		tns, worker, ok := splitServiceTarget(ns, target)
		if !ok {
			out = append(out, wranglercompat.Finding{
				Severity: wranglercompat.SeverityError, Code: "invalid_service_target", FieldPath: path,
				Message: "service target must be \"worker\" or \"ns/worker\", got " + strconv.Quote(target),
			})
			continue
		}
		if tns == ns {
			continue // same namespace: always allowed
		}
		if s.Control == nil {
			out = append(out, wranglercompat.Finding{
				Severity: wranglercompat.SeverityError, Code: "service_acl_check_failed", FieldPath: path,
				Message: "control plane not configured; cannot verify cross-namespace service bindings",
			})
			continue
		}
		allowed, err := s.Control.HasServiceACL(ctx, tns, worker, ns)
		if err != nil {
			out = append(out, wranglercompat.Finding{
				Severity: wranglercompat.SeverityError, Code: "service_acl_check_failed", FieldPath: path,
				Message: err.Error(),
			})
			continue
		}
		if !allowed {
			out = append(out, wranglercompat.Finding{
				Severity: wranglercompat.SeverityError, Code: "service_binding_denied", FieldPath: path,
				Message: "namespace " + tns + " has not granted " + ns + " access to service " + worker +
					" (cellhive service-acl add " + tns + " " + worker + " " + ns + ")",
			})
		}
	}
	return out
}

// splitServiceTarget parses "worker" (same namespace) or "ns/worker".
func splitServiceTarget(callerNS, target string) (string, string, bool) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", "", false
	}
	if !strings.Contains(target, "/") {
		if !validName(target) {
			return "", "", false
		}
		return callerNS, target, true
	}
	tns, worker, _ := strings.Cut(target, "/")
	if !validName(tns) || !validName(worker) {
		return "", "", false
	}
	return tns, worker, true
}

// validName is a light namespace/worker sanity check (no path metacharacters).
func validName(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	return !strings.ContainsAny(s, "/\\ \t\n")
}
