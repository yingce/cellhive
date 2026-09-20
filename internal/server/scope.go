package server

import (
	"context"
	"net/http"

	"strings"
	"time"

	"cellhive/internal/scopedtoken"
	"cellhive/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type ctxKey int

const (
	ctxScopeNS ctxKey = iota
	ctxScopeName
)

// scopeSecret is the HMAC key for binding scoped tokens (ADR-074). Required.
func (s *Server) scopeSecret() []byte { return []byte(s.Cfg.ScopeSecret) }

// scopeResourceParam maps a binding kind to the query parameter that carries
// the resource name. The middleware compares it against the token's Name so a
// scoped token can only touch its own resource, even within one namespace.
var scopeResourceParam = map[string]string{
	"d1":       "db",
	"r2":       "bucket",
	"queue":    "queue",
	"workflow": "workflow",
}

// scopeAuth enforces the per-binding scoped token on tenant binding endpoints
// (ADR-029 layers 3+4, ADR-074). It always requires a valid token: it verifies
// the signature, expiry, binding kind (segment-glob, ADR-181) and either the
// registered-binding ACL (platform tokens) or the ns/scope itself (delegated
// issuer tokens, which must expire). A missing token fails closed (403).
func (s *Server) scopeAuth(kind string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w}
			w = rec
			// claims is verified below; the deferred metric reads the resolved
			// namespace (empty until then, which maps to ns="platform").
			nsForMetrics := ""
			defer func() { s.recordBindingCall(nsForMetrics, kind, rec.code()) }()
			tok := r.Header.Get("x-cellhive-scope-token")
			if tok == "" {
				writeErr(w, http.StatusForbidden, "scope_required", "a scoped token is required for binding calls")
				return
			}
			claims, err := scopedtoken.VerifyIssuer(s.scopeSecret(), tok, time.Now())
			if err != nil {
				writeErr(w, http.StatusForbidden, "scope_invalid", err.Error())
				return
			}
			delegated := claims.Iss != ""
			// A delegated (external) token must expire: without an expiry it
			// cannot be bounded by the issuer's own lifetime (ADR-181).
			if delegated && claims.ExpiresMs == 0 {
				writeErr(w, http.StatusForbidden, "scope_exp_required", "a delegated token must carry an expiry")
				return
			}
			if !scopedtoken.MatchKind(claims, kind) {
				writeErr(w, http.StatusForbidden, "scope_kind", "token is not valid for a "+kind+" binding")
				return
			}
			// The namespace is exact and authoritative (isolation boundary).
			if q := r.URL.Query().Get("ns"); q != "" && q != claims.Namespace {
				writeErr(w, http.StatusForbidden, "scope_mismatch", "request namespace does not match the token")
				return
			}
			if param, ok := scopeResourceParam[kind]; ok {
				// The resource name comes from the request; a glob token matches it.
				got := r.URL.Query().Get(param)
				if got == "" || !scopedtoken.MatchName(claims, got) {
					writeErr(w, http.StatusForbidden, "scope_resource_mismatch", "request resource does not match the token")
					return
				}
			} else if scopedtoken.HasWildcard(claims.Name) {
				// Token-derived resources (kv/vectorize/service/do) need a
				// concrete name to resolve a cell; a wildcard cannot resolve.
				writeErr(w, http.StatusForbidden, "scope_name_required", "this binding kind requires a concrete resource name")
				return
			}
			if !delegated && s.Control != nil {
				// Internal tokens are bounded by the registered-binding ACL;
				// delegated tokens are authorized by their ns/scope (ADR-181).
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
				// Before gating in-flight requests, drop a local cell this node
				// does not own (a stale foreign lineage after a restart or
				// takeover). Done here, outside BeginRequest, so Forget never
				// waits on this request's own eviction gate.
				if sc, ok := s.gateScope(r.Context(), kind, claims.Namespace, claims.Name, r.URL.Query()); ok {
					s.invalidateStaleLocal(r.Context(), sc)
				}
				end := s.Store.BeginRequest(s.gateKey(r.Context(), kind, claims.Namespace, claims.Name, r.URL.Query()))
				defer end()
			}
			nsForMetrics = claims.Namespace
			// Attribute the in-flight http.server span to the tenant so traces
			// are namespaced like the ADR-179 metrics (no-op when tracing is off).
			if span := oteltrace.SpanFromContext(r.Context()); span.IsRecording() {
				span.SetAttributes(
					attribute.String("cellhive.namespace", claims.Namespace),
					attribute.String("cellhive.binding", kind),
				)
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
