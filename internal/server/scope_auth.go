package server

import (
	"net/http"
	"time"

	"cellhive/internal/scopedtoken"
)

// authenticateBindingToken verifies only credential integrity and lifetime.
// It deliberately performs no control-plane or resource access.
func (s *Server) authenticateBindingToken(w http.ResponseWriter, r *http.Request) (scopedtoken.Claims, bool) {
	tok := r.Header.Get("x-cellhive-scope-token")
	if tok == "" {
		writeErr(w, http.StatusForbidden, "scope_required", "a scoped token is required for binding calls")
		return scopedtoken.Claims{}, false
	}
	claims, err := scopedtoken.VerifyIssuer(s.scopeSecret(), tok, time.Now())
	if err != nil {
		writeErr(w, http.StatusForbidden, "scope_invalid", err.Error())
		return scopedtoken.Claims{}, false
	}
	delegated := claims.Iss != ""
	// A delegated (external) token must expire: without an expiry it
	// cannot be bounded by the issuer's own lifetime (ADR-181).
	if delegated && claims.ExpiresMs == 0 {
		writeErr(w, http.StatusForbidden, "scope_exp_required", "a delegated token must carry an expiry")
		return scopedtoken.Claims{}, false
	}
	return claims, true
}

// authorizeBinding checks the requested resource and the existing issuer policy.
// Platform tokens require a registered binding; delegated tokens retain ADR-181
// scope authorization. Neither policy grants ownership of a cell.
func (s *Server) authorizeBinding(w http.ResponseWriter, r *http.Request, kind string, claims scopedtoken.Claims) bool {
	if !scopedtoken.MatchKind(claims, kind) {
		writeErr(w, http.StatusForbidden, "scope_kind", "token is not valid for a "+kind+" binding")
		return false
	}
	// The namespace is exact and authoritative (isolation boundary).
	if q := r.URL.Query().Get("ns"); q != "" && q != claims.Namespace {
		writeErr(w, http.StatusForbidden, "scope_mismatch", "request namespace does not match the token")
		return false
	}
	if param, ok := scopeResourceParam[kind]; ok {
		// The resource name comes from the request; a glob token matches it.
		got := r.URL.Query().Get(param)
		if got == "" || !scopedtoken.MatchName(claims, got) {
			writeErr(w, http.StatusForbidden, "scope_resource_mismatch", "request resource does not match the token")
			return false
		}
	} else if scopedtoken.HasWildcard(claims.Name) {
		// Token-derived resources (kv/vectorize/service/do) need a
		// concrete name to resolve a cell; a wildcard cannot resolve.
		writeErr(w, http.StatusForbidden, "scope_name_required", "this binding kind requires a concrete resource name")
		return false
	}
	if claims.Iss == "" && s.Control != nil {
		// Internal tokens are bounded by the registered-binding ACL;
		// delegated tokens are authorized by their ns/scope (ADR-181).
		var ok bool
		var cerr error
		if claims.Resource != "" {
			ok, cerr = s.pinnedBindingAllowed(r.Context(), claims, false)
		} else {
			ok, _, cerr = s.bindingInfo(r.Context(), claims.Namespace, kind, claims.Name)
		}
		if cerr != nil {
			writeErr(w, http.StatusInternalServerError, "scope_check_failed", cerr.Error())
			return false
		}
		if !ok {
			writeErr(w, http.StatusForbidden, "binding_not_registered", "binding is not declared for this namespace")
			return false
		}
	}
	if claims.Resource != "" {
		s.notePinnedAuth(claims)
	}
	return true
}
