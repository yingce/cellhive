package server

import (
	"net/http"
	"strings"

	"cellhive/internal/bearertoken"
	"cellhive/internal/scopedtoken"
)

// authenticateBindingToken verifies only credential integrity and lifetime.
// It deliberately performs no control-plane or resource access.
func (s *Server) authenticateBindingToken(w http.ResponseWriter, r *http.Request, kind string) (scopedtoken.Claims, bool) {
	kid, c, err := s.verifyBearer(r)
	if err != nil {
		writeErr(w, http.StatusForbidden, "scope_invalid", err.Error())
		return scopedtoken.Claims{}, false
	}
	if c.Type == "namespace" || c.Type == "admin" {
		ns := r.URL.Query().Get("ns")
		name := r.URL.Query().Get("name")
		if name == "" {
			name = r.URL.Query().Get(scopeResourceParam[kind])
		}
		if ns == "" || name == "" || (c.Type == "namespace" && !bearertoken.Allows(c.Namespaces, ns)) {
			writeErr(w, http.StatusForbidden, "scope_mismatch", "explicit authorized namespace and binding required")
			return scopedtoken.Claims{}, false
		}
		if target := r.URL.Query().Get("target_ns"); kind == "service" && target != "" && c.Type == "namespace" && !bearertoken.Allows(c.Namespaces, target) {
			writeErr(w, http.StatusForbidden, "namespace_forbidden", "service target exceeds namespace grants")
			return scopedtoken.Claims{}, false
		}
		return scopedtoken.Claims{Namespace: ns, Kind: kind, Name: name, ExpiresMs: c.ExpiresAt * 1000}, true
	}
	if c.Type != "binding" {
		writeErr(w, http.StatusForbidden, "scope_required", "binding or namespace JWT required")
		return scopedtoken.Claims{}, false
	}
	if scopedtoken.HasWildcard(c.Kind) || scopedtoken.HasWildcard(c.Name) {
		writeErr(w, http.StatusForbidden, "scope_invalid", "binding JWT requires concrete kind and name")
		return scopedtoken.Claims{}, false
	}
	claims := scopedtoken.Claims{Namespace: c.Namespace, Kind: c.Kind, Name: c.Name, Worker: c.Worker, Version: c.Version, Resource: c.Resource, ExpiresMs: c.ExpiresAt * 1000}
	if strings.HasPrefix(kid, "issuer/") {
		claims.Iss = kid
	}
	return claims, true
}

// authorizeBinding checks the requested resource and the existing issuer policy.
// Platform tokens require a registered binding; delegated tokens retain ADR-181
// scope authorization. Neither policy grants ownership of a cell.
func (s *Server) authorizeBinding(w http.ResponseWriter, r *http.Request, kind string, claims scopedtoken.Claims) (string, bool) {
	if !scopedtoken.MatchKind(claims, kind) {
		writeErr(w, http.StatusForbidden, "scope_kind", "token is not valid for a "+kind+" binding")
		return "", false
	}
	// The namespace is exact and authoritative (isolation boundary).
	if q := r.URL.Query().Get("ns"); q != "" && q != claims.Namespace {
		writeErr(w, http.StatusForbidden, "scope_mismatch", "request namespace does not match the token")
		return "", false
	}
	if param, ok := scopeResourceParam[kind]; ok {
		// The resource name comes from the request; a glob token matches it.
		got := r.URL.Query().Get(param)
		match := claims.Name
		if claims.Resource != "" {
			match = claims.Resource
		}
		if got == "" || !scopedtoken.MatchName(scopedtoken.Claims{Name: match}, got) {
			writeErr(w, http.StatusForbidden, "scope_resource_mismatch", "request resource does not match the token")
			return "", false
		}
	} else if scopedtoken.HasWildcard(claims.Name) {
		// Token-derived resources (kv/vectorize/service/do) need a
		// concrete name to resolve a cell; a wildcard cannot resolve.
		writeErr(w, http.StatusForbidden, "scope_name_required", "this binding kind requires a concrete resource name")
		return "", false
	}
	resolved := claims.Resource
	if claims.Iss == "" || kind == "service" {
		if s.Control == nil {
			writeErr(w, http.StatusServiceUnavailable, "scope_check_failed", "control registry unavailable")
			return "", false
		}
		// Internal tokens are bounded by the registered-binding ACL;
		// delegated tokens are authorized by their ns/scope (ADR-181).
		var ok bool
		var cerr error
		if claims.Worker != "" || claims.Version != 0 {
			ok, cerr = s.pinnedBindingAllowed(r.Context(), claims, false)
		} else if claims.Resource != "" {
			// Resource is a signed, immutable routing claim. The loader issued
			// it after resolving the binding; avoid a control-cell lookup on
			// every data request. Pinned KV tokens retain their revocation check
			// above because worker/version authorization is time bounded.
			ok = true
		} else {
			var id string
			ok, id, cerr = s.bindingInfo(r.Context(), claims.Namespace, kind, claims.Name)
			resolved = id
		}
		if cerr != nil {
			writeErr(w, http.StatusInternalServerError, "scope_check_failed", cerr.Error())
			return "", false
		}
		if !ok {
			writeErr(w, http.StatusForbidden, "binding_not_registered", "binding is not declared for this namespace")
			return "", false
		}
	}
	if claims.Worker != "" || claims.Version != 0 {
		s.notePinnedAuth(claims)
	}
	return resolved, true
}
