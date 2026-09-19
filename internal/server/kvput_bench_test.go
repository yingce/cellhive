package server

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/scopedtoken"
)

func benchKVServer(tb testing.TB, bindingCacheTTL time.Duration) (*Server, string) {
	tb.Helper()
	cs, err := cellstore.New(tb.TempDir())
	if err != nil {
		tb.Fatalf("cellstore: %v", err)
	}
	env, _ := control.NewEnvelope(bytes.Repeat([]byte{0x3}, 32))
	b, _ := bucket.NewFSBucket(tb.TempDir())
	s := New(Deps{
		Cfg: config.Config{
			NodeID: "node-1", ScopeSecret: "scope-secret", BindingCacheTTL: bindingCacheTTL,
		},
		Store:   cs,
		Bucket:  b,
		Control: control.New(cs, env),
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx := tb.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "bench"); err != nil {
		tb.Fatalf("create app: %v", err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "KV", "default", "bench"); err != nil {
		tb.Fatalf("create resource: %v", err)
	}
	tok, err := scopedtoken.Mint([]byte(s.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "kv", Name: "KV"})
	if err != nil {
		tb.Fatalf("mint: %v", err)
	}
	return s, tok
}

// BenchmarkKVPutHandler measures the whole server-side KV put path with no
// network and no client: mux + scoped-token verify + binding check + cellstore
// transaction + JSON response.
func BenchmarkKVPutHandler(b *testing.B) {
	s, tok := benchKVServer(b, 0)
	h := s.Handler()
	body := []byte("v")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest(http.MethodPost, "/v1/kv/put?ns=acme&key=k", bytes.NewReader(body))
			req.Header.Set("x-cellhive-scope-token", tok)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				b.Errorf("status %d: %s", rr.Code, rr.Body.String())
				return
			}
		}
	})
}

// BenchmarkKVPutHandlerNoAuth measures the same path with the binding check
// disabled (scopeAuth bypassed is not possible without a valid token, so this
// uses the cache) to isolate the binding lookup cost.
func BenchmarkKVPutHandlerCachedBinding(b *testing.B) {
	s, tok := benchKVServer(b, time.Minute)
	h := s.Handler()
	body := []byte("v")
	// Warm the cache.
	req := httptest.NewRequest(http.MethodPost, "/v1/kv/put?ns=acme&key=warm", bytes.NewReader(body))
	req.Header.Set("x-cellhive-scope-token", tok)
	h.ServeHTTP(httptest.NewRecorder(), req)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest(http.MethodPost, "/v1/kv/put?ns=acme&key=k", bytes.NewReader(body))
			req.Header.Set("x-cellhive-scope-token", tok)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				b.Errorf("status %d: %s", rr.Code, rr.Body.String())
				return
			}
		}
	})
}
