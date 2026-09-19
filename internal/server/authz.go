package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"cellhive/internal/auth"
	"cellhive/internal/control"
)

type principalCtxKey struct{}

// adminPrincipal resolves the caller's principal from the admin credential. The
// configured AdminAuth wins; otherwise the static admin token is used (full
// access, kind=static).
func (s *Server) adminPrincipal(r *http.Request) (auth.Principal, bool) {
	a := s.adminAuthenticator()
	if a == nil {
		return auth.Principal{}, false
	}
	if ra, ok := a.(auth.RequestAuthenticator); ok {
		return ra.AuthenticateRequest(r)
	}
	if a.Authenticate(r) {
		return auth.Principal{Sub: "static-admin", Kind: "static", All: true}, true
	}
	return auth.Principal{}, false
}

// principalFrom returns the authenticated principal attached by adminAuth.
func principalFrom(ctx context.Context) (auth.Principal, bool) {
	p, ok := ctx.Value(principalCtxKey{}).(auth.Principal)
	return p, ok
}

func requestID(r *http.Request) string {
	if v := r.Header.Get("x-request-id"); v != "" {
		return v
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// withPrincipal attaches the principal to the request context, both for the
// namespace checks and for the audit trail (control actor).
func withPrincipal(r *http.Request, p auth.Principal) *http.Request {
	p.RequestID = requestID(r)
	ctx := context.WithValue(r.Context(), principalCtxKey{}, p)
	ctx = control.WithActor(ctx, control.Actor{
		Sub: p.Sub, Kind: p.Kind, OnBehalfOf: p.OnBehalfOf, RequestID: p.RequestID,
	})
	return r.WithContext(ctx)
}

// nslessListPaths are admin control paths that may legitimately run without a
// namespace parameter: the handler filters the result with allowedNS.
var nslessListPaths = map[string]bool{
	"/v1/control/apps": true,
}

// controlTargetNS resolves the namespace a control request targets, preferring
// the query parameter and falling back to a JSON body field. When it has to read
// the body it returns the raw bytes so the caller can replay them.
func controlTargetNS(r *http.Request) (ns string, body []byte, err error) {
	if ns = r.URL.Query().Get("namespace"); ns == "" {
		ns = r.URL.Query().Get("ns")
	}
	if ns != "" {
		return ns, nil, nil
	}
	if r.Body == nil || r.Method == http.MethodGet || r.Method == http.MethodHead {
		return "", nil, nil
	}
	b, rerr := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if rerr != nil {
		return "", nil, rerr
	}
	body = b
	if len(b) == 0 {
		return "", body, nil
	}
	var probe struct {
		Namespace string `json:"namespace"`
	}
	if jerr := json.Unmarshal(b, &probe); jerr == nil {
		ns = probe.Namespace
	}
	return ns, body, nil
}

// authorizeControlPath enforces the caller's namespace grants for a control
// request BEFORE the handler runs — and therefore before any forwarding to the
// control-cell owner. Without this, a request that a non-owner node forwards
// would be served by the owner's internal plane, which carries no principal and
// skips scoping (ADR-131 follow-up).
func (s *Server) authorizeControlPath(w http.ResponseWriter, r *http.Request) bool {
	p, ok := principalFrom(r.Context())
	if !ok {
		// Internal plane (no admin principal): the internal token is the
		// credential and the network is trusted.
		return true
	}
	ns, body, err := controlTargetNS(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "cannot read request body: "+err.Error())
		return false
	}
	if body != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	switch {
	case ns != "":
		if !p.Allows(ns) {
			writeErr(w, http.StatusForbidden, "namespace_forbidden", "credential is not granted namespace "+ns)
			return false
		}
		return true
	case nslessListPaths[r.URL.Path]:
		return true // the handler filters with allowedNS
	default:
		if !p.All {
			writeErr(w, http.StatusForbidden, "platform_forbidden", "credential needs platform-wide (*) access")
			return false
		}
		return true
	}
}

// authorizeNS allows the request when the caller may manage ns. Endpoints whose
// target namespace is required call this before touching the store; list
// endpoints are filtered instead (see allowedNS).
func (s *Server) authorizeNS(w http.ResponseWriter, r *http.Request, ns string) bool {
	p, ok := principalFrom(r.Context())
	if !ok {
		// No principal (e.g. internal-token path): the internal plane is trusted.
		return true
	}
	if ns == "" {
		if p.All {
			return true
		}
		writeErr(w, http.StatusForbidden, "namespace_required", "an explicit namespace is required for this credential")
		return false
	}
	if !p.Allows(ns) {
		writeErr(w, http.StatusForbidden, "namespace_forbidden", "credential is not granted namespace "+ns)
		return false
	}
	return true
}

// requireAll gates platform-wide actions (GC, status, capacity, blob upload)
// which cannot be scoped to a namespace: they need a "*" grant.
func (s *Server) requireAll(w http.ResponseWriter, r *http.Request) bool {
	p, ok := principalFrom(r.Context())
	if !ok || p.All {
		return true
	}
	writeErr(w, http.StatusForbidden, "platform_forbidden", "credential needs platform-wide (*) access")
	return false
}

// allowedNS returns nil when the caller may see every namespace, or the set of
// granted namespaces for list endpoints to filter by.
func allowedNS(ctx context.Context) map[string]bool {
	p, ok := principalFrom(ctx)
	if !ok || p.All {
		return nil
	}
	out := map[string]bool{}
	for _, ns := range p.NS {
		if ns != "" && ns != "*" {
			out[ns] = true
		}
	}
	return out
}

// filterByNS keeps only the namespaces in allow (nil = keep all).
func filterByNS[T any](allow map[string]bool, items []T, nsOf func(T) string) []T {
	if allow == nil {
		return items
	}
	out := make([]T, 0, len(items))
	for _, it := range items {
		if allow[nsOf(it)] {
			out = append(out, it)
		}
	}
	return out
}

// actorFor returns the audit actor label for a handler.
// truthy parses a boolean-ish query flag ("1"/"true"/"yes").
func truthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes":
		return true
	}
	return false
}

func actorFor(ctx context.Context) string {
	if p, ok := principalFrom(ctx); ok && p.Sub != "" {
		return p.Sub
	}
	return "admin"
}
