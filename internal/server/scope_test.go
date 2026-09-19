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
