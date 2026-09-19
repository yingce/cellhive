package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/d1"
	"cellhive/internal/queue"
	"cellhive/internal/r2"
	"cellhive/internal/scopedtoken"
)

func newScopeServer(t *testing.T) *Server {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	env, _ := control.NewEnvelope(bytes.Repeat([]byte{0x3}, 32))
	b, _ := bucket.NewFSBucket(t.TempDir())
	return New(Deps{
		Cfg: config.Config{
			NodeID: "node-1", TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok",
			ScopeSecret: "scope-secret",
		},
		Store:   cs,
		Bucket:  b,
		Control: control.New(cs, env),
		D1:      d1.New(cs),
		R2:      r2.New(b),
		Queue:   queue.New(cs),
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

// mint computes a binding token locally (ADR-074); no server round trip.
func mint(t *testing.T, s *Server, ns, kind, name string) string {
	t.Helper()
	tok, err := scopedtoken.Mint([]byte(s.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: ns, Kind: kind, Name: name})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	return tok
}

// kvReq sends a binding request WITHOUT the internal token, proving binding
// endpoints authenticate on the scoped token alone (ADR-074).
func kvReq(t *testing.T, s *Server, method, path, token string, body []byte) int {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if token != "" {
		req.Header.Set("x-cellhive-scope-token", token)
	}
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	return rr.Code
}

func TestScopeAuthKV(t *testing.T) {
	ctx := contextTODO()
	s := newScopeServer(t)
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "main", "acme/__kv__/main", "ops"); err != nil {
		t.Fatalf("resource: %v", err)
	}

	// No token -> 403 (enforcement on).
	if code := kvReq(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=k", "", []byte("v")); code != http.StatusForbidden {
		t.Fatalf("no token put = %d, want 403", code)
	}
	// Token for a registered binding -> 200.
	tok := mint(t, s, "acme", "kv", "main")
	if code := kvReq(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=k", tok, []byte("v")); code != http.StatusOK {
		t.Fatalf("authorized put = %d, want 200", code)
	}
	if code := kvReq(t, s, http.MethodGet, "/v1/kv/get?ns=acme&key=k", tok, nil); code != http.StatusOK {
		t.Fatalf("authorized get = %d, want 200", code)
	}
	// Query ns disagreeing with the token -> 403.
	if code := kvReq(t, s, http.MethodGet, "/v1/kv/get?ns=evil&key=k", tok, nil); code != http.StatusForbidden {
		t.Fatalf("ns mismatch = %d, want 403", code)
	}
	// Tampered token -> 403.
	if code := kvReq(t, s, http.MethodGet, "/v1/kv/get?ns=acme&key=k", "x"+tok, nil); code != http.StatusForbidden {
		t.Fatalf("tampered token = %d, want 403", code)
	}
	// Unregistered binding -> 403.
	tok2 := mint(t, s, "acme", "kv", "nope")
	if code := kvReq(t, s, http.MethodGet, "/v1/kv/get?ns=acme&key=k", tok2, nil); code != http.StatusForbidden {
		t.Fatalf("unregistered binding = %d, want 403", code)
	}
	// Wrong kind in token -> 403.
	tok3 := mint(t, s, "acme", "d1", "main")
	if code := kvReq(t, s, http.MethodGet, "/v1/kv/get?ns=acme&key=k", tok3, nil); code != http.StatusForbidden {
		t.Fatalf("wrong kind = %d, want 403", code)
	}
}

func contextTODO() context.Context { return context.Background() }

// TestScopeAuthEnforcesResourceName is the regression for the middleware only
// checking the token's own name: a token for resource A must not reach B in the
// same namespace via the query resource parameter.
func TestScopeAuthEnforcesResourceName(t *testing.T) {
	ctx := contextTODO()
	s := newScopeServer(t)
	cases := []struct{ kind, name, scope, param, other string }{
		{"d1", "a", "acme/__d1__/a", "db", "b"},
		{"r2", "a", "acme/__r2__/a", "bucket", "b"},
		{"queue", "a", "acme/__queue__/a", "queue", "b"},
		{"workflow", "a", "acme/__workflow__/a", "workflow", "b"},
	}
	for _, tc := range cases {
		if _, err := s.Control.CreateResource(ctx, "acme", tc.kind, tc.name, tc.scope, "ops"); err != nil {
			t.Fatalf("resource %s: %v", tc.kind, err)
		}
	}
	for _, tc := range cases {
		handler := s.scopeAuth(tc.kind)(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		tok := mint(t, s, "acme", tc.kind, tc.name)

		matching := httptest.NewRequest(http.MethodPost, "/x?ns=acme&"+tc.param+"="+tc.name, nil)
		matching.Header.Set("x-cellhive-scope-token", tok)
		if rr := httptest.NewRecorder(); func() int { handler(rr, matching); return rr.Code }() != http.StatusOK {
			t.Fatalf("%s matching resource = %d, want 200", tc.kind, rr.Code)
		}

		cross := httptest.NewRequest(http.MethodPost, "/x?ns=acme&"+tc.param+"="+tc.other, nil)
		cross.Header.Set("x-cellhive-scope-token", tok)
		rr := httptest.NewRecorder()
		handler(rr, cross)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("%s cross-resource = %d, want 403", tc.kind, rr.Code)
		}
	}
}

// TestScopeResourceMismatchDeniedViaHandler exercises the real HTTP handlers
// (not just the middleware) so the cross-resource denial is end-to-end.
func TestScopeResourceMismatchDeniedViaHandler(t *testing.T) {
	ctx := contextTODO()
	s := newScopeServer(t)
	for _, r := range []struct{ kind, name, scope string }{
		{"d1", "a", "acme/__d1__/a"},
		{"d1", "b", "acme/__d1__/b"},
		{"r2", "a", "acme/__r2__/a"},
		{"r2", "b", "acme/__r2__/b"},
	} {
		if _, err := s.Control.CreateResource(ctx, "acme", r.kind, r.name, r.scope, "ops"); err != nil {
			t.Fatalf("resource %s/%s: %v", r.kind, r.name, err)
		}
	}

	d1tok := mint(t, s, "acme", "d1", "a")
	body := []byte(`{"sql":"SELECT 1"}`)
	if code := kvReq(t, s, http.MethodPost, "/v1/d1/query?ns=acme&db=b", d1tok, body); code != http.StatusForbidden {
		t.Fatalf("d1 cross-resource = %d, want 403", code)
	}
	if code := kvReq(t, s, http.MethodPost, "/v1/d1/query?ns=acme&db=a", d1tok, body); code != http.StatusOK {
		t.Fatalf("d1 own resource = %d, want 200", code)
	}

	r2tok := mint(t, s, "acme", "r2", "a")
	if code := kvReq(t, s, http.MethodPut, "/v1/r2/object?ns=acme&bucket=b&key=k", r2tok, []byte("v")); code != http.StatusForbidden {
		t.Fatalf("r2 cross-resource = %d, want 403", code)
	}
	if code := kvReq(t, s, http.MethodPut, "/v1/r2/object?ns=acme&bucket=a&key=k", r2tok, []byte("v")); code != http.StatusOK {
		t.Fatalf("r2 own resource = %d, want 200", code)
	}
}
