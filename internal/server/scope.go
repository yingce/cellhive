package server

import (
	"context"
	"net/http"

	"cellhive/internal/telemetry"
	"strings"
	"time"

	"cellhive/internal/scopedtoken"
)

type ctxKey int

const (
	ctxScopeNS ctxKey = iota
	ctxScopeName
)

// scopeSecret is the HMAC key for binding scoped tokens (ADR-074). Required.
func (s *Server) scopeSecret() []byte { return []byte(s.Cfg.ScopeSecret) }

// scopeAuth enforces the per-binding scoped token on tenant binding endpoints
// (ADR-029 layers 3+4, ADR-074). It always requires a valid token: it verifies
// the signature, expiry, binding kind and that the binding is declared for the
// namespace. A missing token fails closed (403).
func (s *Server) scopeAuth(kind string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w}
			w = rec
			defer func() { s.recordBindingCall(kind, rec.code()) }()
			tok := r.Header.Get("x-cellhive-scope-token")
			if tok == "" {
				writeErr(w, http.StatusForbidden, "scope_required", "a scoped token is required for binding calls")
				return
			}
			claims, err := scopedtoken.Verify(s.scopeSecret(), tok, time.Now())
			if err != nil {
				writeErr(w, http.StatusForbidden, "scope_invalid", err.Error())
				return
			}
			if claims.Kind != kind {
				writeErr(w, http.StatusForbidden, "scope_kind", "token is not valid for a "+kind+" binding")
				return
			}
			if q := r.URL.Query().Get("ns"); q != "" && q != claims.Namespace {
				writeErr(w, http.StatusForbidden, "scope_mismatch", "request namespace does not match the token")
				return
			}
			if s.Control != nil {
				ok, _, cerr := s.bindingInfo(r.Context(), claims.Namespace, kind, claims.Name)
				if cerr != nil {
					writeErr(w, http.StatusInternalServerError, "scope_check_failed", cerr.Error())
					return
				}
				if !ok {
					writeErr(w, http.StatusForbidden, "binding_not_registered", "binding is not declared for this namespace")
					return
				}
			}
			telemetry.SpanFromContext(r.Context()).SetAttributes(
				telemetry.ScopeAttrs(claims.Namespace, kind, claims.Name, "")...)
			if s.Store != nil {
				end := s.Store.BeginRequest(s.gateKey(r.Context(), kind, claims.Namespace, claims.Name, r.URL.Query()))
				defer end()
			}
			ctx := context.WithValue(r.Context(), ctxScopeNS, claims.Namespace)
			ctx = context.WithValue(ctx, ctxScopeName, claims.Name)
			next(w, r.WithContext(ctx))
		}
	}
}

// admit enforces per-namespace write-rate admission (ADR-035). It must run after
// scopeAuth so the namespace is available; nil/disabled admits everything.
func (s *Server) admit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Admission != nil && s.Admission.Enabled() {
			if !s.Admission.Allow(s.scopeNS(r)) {
				writeErr(w, http.StatusTooManyRequests, "rate_limited", "namespace write rate exceeded")
				return
			}
		}
		next(w, r)
	}
}

// bindingInfo resolves a binding declaration through a short-TTL cache so the
// write hot path does not read the control cell on every request. It returns the
// declaration's resource id alongside ok. Negative results are cached too;
// control-plane writes invalidate the namespace.
func (s *Server) bindingInfo(ctx context.Context, ns, kind, name string) (bool, string, error) {
	ttl := s.Cfg.BindingCacheTTL
	if ttl <= 0 {
		id, ok, err := s.Control.Binding(ctx, ns, kind, name)
		return ok, id, err
	}
	key := ns + "|" + kind + "|" + name
	now := time.Now()
	s.bindMu.Lock()
	if e, ok := s.bindCache[key]; ok && now.Before(e.exp) {
		s.bindMu.Unlock()
		return e.ok, e.id, nil
	}
	s.bindMu.Unlock()

	id, ok, err := s.Control.Binding(ctx, ns, kind, name)
	if err != nil {
		return false, "", err
	}
	s.bindMu.Lock()
	if s.bindCache == nil || len(s.bindCache) > 100000 {
		s.bindCache = map[string]bindingEntry{}
	}
	s.bindCache[key] = bindingEntry{ok: ok, id: id, exp: now.Add(ttl)}
	s.bindMu.Unlock()
	return ok, id, nil
}

// bindingAllowed reports whether the binding is declared (cached).
func (s *Server) bindingAllowed(ctx context.Context, ns, kind, name string) (bool, error) {
	ok, _, err := s.bindingInfo(ctx, ns, kind, name)
	return ok, err
}

// bindingID returns the binding's resolved resource id (cached).
func (s *Server) bindingID(ctx context.Context, ns, kind, name string) (string, error) {
	_, id, err := s.bindingInfo(ctx, ns, kind, name)
	return id, err
}

// invalidateBindings drops cached declarations for a namespace after a
// control-plane change so a new binding is visible immediately.
func (s *Server) invalidateBindings(ns string) {
	if s.Cfg.BindingCacheTTL <= 0 {
		return
	}
	prefix := ns + "|"
	s.bindMu.Lock()
	for k := range s.bindCache {
		if strings.HasPrefix(k, prefix) {
			delete(s.bindCache, k)
		}
	}
	s.bindMu.Unlock()
}

// scopeNS returns the namespace authorized by the scoped-token middleware.
func (s *Server) scopeNS(r *http.Request) string {
	v, _ := r.Context().Value(ctxScopeNS).(string)
	return v
}

// scopeName returns the binding name authorized by the middleware.
func (s *Server) scopeName(r *http.Request) string {
	v, _ := r.Context().Value(ctxScopeName).(string)
	return v
}
