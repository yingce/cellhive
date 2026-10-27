package server

import (
	"cellhive/internal/scopedtoken"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"strings"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/owner"
	"cellhive/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type ctxKey int

const (
	ctxScopeNS ctxKey = iota
	ctxScopeName
	ctxScopePreInvalidated
	ctxPinnedKV
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
			claims, ok := s.authenticateBindingToken(w, r)
			if !ok || !s.authorizeBinding(w, r, kind, claims) {
				return
			}
			if claims.Resource != "" {
				r = r.WithContext(context.WithValue(r.Context(), ctxPinnedKV, claims))
			}
			preInvalidated, release, ok := s.beginBindingAccess(w, r, kind, claims)
			if !ok {
				return
			}
			if release != nil {
				defer release()
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
			if preInvalidated != "" {
				ctx = context.WithValue(ctx, ctxScopePreInvalidated, preInvalidated)
			}
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
	ctx, span := telemetry.Tracer().Start(ctx, "cell.binding_lookup")
	defer span.End()
	ttl := s.Cfg.BindingCacheTTL
	if ttl <= 0 {
		id, ok, err := s.bindingFromOwner(ctx, ns, kind, name)
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
	return s.refreshBindingInfo(ctx, ns, kind, name)
}

// refreshBindingInfo bypasses the TTL for proactive refresh. A control-plane
// invalidation during the owner read must not be undone by its stale result.
func (s *Server) refreshBindingInfo(ctx context.Context, ns, kind, name string) (bool, string, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "cell.binding_refresh")
	defer span.End()
	key := ns + "|" + kind + "|" + name
	return s.refreshBinding(ctx, key, func() (string, bool, error) { return s.bindingFromOwner(ctx, ns, kind, name) })
}

func (s *Server) refreshBinding(ctx context.Context, key string, read func() (string, bool, error)) (bool, string, error) {
	for {
		if err := ctx.Err(); err != nil {
			return false, "", err
		}
		s.bindMu.Lock()
		rev := s.bindRev
		if f := s.bindFlights[key]; f != nil && f.rev == rev {
			s.bindMu.Unlock()
			select {
			case <-ctx.Done():
				return false, "", ctx.Err()
			case <-f.done:
			}
			s.bindMu.Lock()
			current := s.bindRev == f.rev
			s.bindMu.Unlock()
			if !current || errors.Is(f.err, context.Canceled) || errors.Is(f.err, context.DeadlineExceeded) {
				continue
			}
			return f.ok, f.id, f.err
		}
		f := &bindingFlight{rev: rev, done: make(chan struct{})}
		if s.bindFlights == nil {
			s.bindFlights = make(map[string]*bindingFlight)
		}
		s.bindFlights[key] = f
		s.bindMu.Unlock()
		issued := time.Now()
		f.id, f.ok, f.err = read()
		s.bindMu.Lock()
		current := s.bindRev == rev
		if current && f.err == nil && s.Cfg.BindingCacheTTL > 0 {
			if s.bindCache == nil || len(s.bindCache) > 100000 {
				s.bindCache = map[string]bindingEntry{}
			}
			s.bindCache[key] = bindingEntry{ok: f.ok, id: f.id, exp: issued.Add(s.Cfg.BindingCacheTTL)}
		}
		if s.bindFlights[key] == f {
			delete(s.bindFlights, key)
		}
		close(f.done)
		s.bindMu.Unlock()
		return f.ok, f.id, f.err
	}
}

type bindingFlight struct {
	done chan struct{}
	rev  uint64
	id   string
	ok   bool
	err  error
}

func (s *Server) bindingFromOwner(ctx context.Context, ns, kind, name string, pinned ...scopedtoken.Claims) (string, bool, error) {
	return s.bindingFromOwnerAttempt(ctx, ns, kind, name, true, pinned...)
}

func (s *Server) bindingFromOwnerAttempt(ctx context.Context, ns, kind, name string, mayClaim bool, pinned ...scopedtoken.Claims) (string, bool, error) {
	q := url.Values{"ns": {ns}, "kind": {kind}, "name": {name}}
	if len(pinned) > 0 {
		p := pinned[0]
		q.Set("worker", p.Worker)
		q.Set("version", strconv.Itoa(p.Version))
		q.Set("resource", p.Resource)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://cellhive.internal/v1/control/binding?"+q.Encode(), http.NoBody)
	if err != nil {
		return "", false, err
	}
	telemetry.Inject(ctx, req.Header)
	resp, forwarded, err := s.forwardTo(req, control.Scope(), []byte{})
	if err != nil {
		return "", false, err
	}
	if forwarded {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return "", false, fmt.Errorf("control binding owner returned %d", resp.StatusCode)
		}
		var answer struct {
			ID string `json:"id"`
			OK bool   `json:"ok"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&answer); err != nil {
			return "", false, err
		}
		return answer.ID, answer.OK, nil
	}
	id, allowed, err := s.localBinding(ctx, ns, kind, name, pinned...)
	if mayClaim && (errors.Is(err, owner.ErrUnowned) || errors.Is(err, owner.ErrNotOwner)) {
		// After restart the control cell may still name the previous session.
		// Use normal fenced recovery now, instead of waiting for a timer loop
		// to claim it. Re-route once because a peer can win the claim.
		if err := s.claimForRead(ctx, control.Scope()); err != nil {
			return "", false, err
		}
		return s.bindingFromOwnerAttempt(ctx, ns, kind, name, false, pinned...)
	}
	return id, allowed, err
}

func (s *Server) localBinding(ctx context.Context, ns, kind, name string, pinned ...scopedtoken.Claims) (id string, allowed bool, readErr error) {
	if s.Owner != nil {
		fence, ok := s.Owner.ReadFence(control.Scope())
		if !ok {
			return "", false, owner.ErrNotOwner
		}
		defer func() {
			if !fence() {
				id = ""
				allowed = false
				readErr = owner.ErrNotOwner
			}
		}()
		if check, ok := s.Owner.LocalReadGuard(control.Scope()); ok {
			defer func() {
				if !check() {
					id = ""
					allowed = false
					readErr = owner.ErrNotOwner
				}
			}()
			return s.readLocalBinding(ctx, ns, kind, name, pinned...)
		}
	}

	// forwardTo is only a routing helper: errors/unowned routes may fall through.
	// Before reading local authorization state, require authoritative ownership.
	if s.Owner != nil {
		o, _, err := s.Owner.Resolve(ctx, control.Scope())
		if err != nil {
			return "", false, err
		}
		if o.Node != s.Cfg.NodeID || o.Session != s.Owner.Session || !s.ownerLive(ctx, o, time.Now()) {
			s.Owner.Invalidate(control.Scope())
			return "", false, owner.ErrNotOwner
		}
	}
	return s.readLocalBinding(ctx, ns, kind, name, pinned...)
}

func (s *Server) readLocalBinding(ctx context.Context, ns, kind, name string, pinned ...scopedtoken.Claims) (string, bool, error) {
	if len(pinned) > 0 {
		p := pinned[0]
		if !p.Valid() || p.Kind != "kv" || p.Resource == "" {
			return "", false, scopedtoken.ErrIncomplete
		}
		ok, err := s.Control.PinnedBindingAllowed(ctx, ns, p.Worker, p.Version, name, p.Resource)
		return p.Resource, ok, err
	}
	return s.Control.Binding(ctx, ns, kind, name)
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
	s.bindRev++
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
