package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestKVTTLAndMetadataE2E covers ADR-098: metadata round-trips, an expired key
// is invisible, and the timer-driven internal cleanup endpoint deletes it.
func TestKVTTLAndMetadataE2E(t *testing.T) {
	ctx := context.Background()
	s := newScopeServer(t)
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "KV", "default", "test"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	tok := mint(t, s, "acme", "kv", "KV")
	h := s.Handler()
	do := func(method, target, token string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, bytes.NewReader(body))
		if token != "" {
			req.Header.Set("x-cellhive-scope-token", token)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}

	meta := base64.StdEncoding.EncodeToString([]byte(`{"n":1}`))
	if rr := do(http.MethodPost, "/v1/kv/put?ns=acme&key=m&metadata="+url.QueryEscape(meta), tok, []byte("vm")); rr.Code != http.StatusOK {
		t.Fatalf("put m = %d %s", rr.Code, rr.Body.String())
	}
	rr := do(http.MethodGet, "/v1/kv/get?ns=acme&key=m", tok, nil)
	if rr.Code != http.StatusOK || rr.Body.String() != "vm" {
		t.Fatalf("get m = %d %q", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("x-cellhive-kv-metadata"); got != meta {
		t.Fatalf("metadata header = %q, want %q", got, meta)
	}

	// A key past its deadline is invisible from the moment it expires. An
	// absolute expiration in the past is rejected (Cloudflare/celld parity), so
	// arm a near-future one and wait it out.
	soon := time.Now().Add(time.Second).Unix()
	if rr := do(http.MethodPost, "/v1/kv/put?ns=acme&key=e&expiration="+strconv.FormatInt(soon, 10), tok, []byte("ve")); rr.Code != http.StatusOK {
		t.Fatalf("put e = %d %s", rr.Code, rr.Body.String())
	}
	time.Sleep(1200 * time.Millisecond)
	if rr := do(http.MethodGet, "/v1/kv/get?ns=acme&key=e", tok, nil); rr.Code != http.StatusNotFound {
		t.Fatalf("get expired = %d, want 404", rr.Code)
	}

	// The internal cleanup endpoint deletes the expired row (captured write).
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/kv/expire?scope=acme/__kv__/default", nil)
	req.Header.Set("x-cellhive-internal-token", "tok")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expire = %d %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"deleted":1`) {
		t.Fatalf("expire body = %s, want deleted:1", rr.Body.String())
	}
}

// TestKVValidationLimits covers the Cloudflare/celld-aligned KV write limits
// (docs/bindings.md): key <= 512 B, metadata <= 1 KiB, expirationTtl >= 60s,
// and an absolute expiration must be in the future.
func TestKVValidationLimits(t *testing.T) {
	ctx := context.Background()
	s := newScopeServer(t)
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "KV", "default", "test"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	tok := mint(t, s, "acme", "kv", "KV")
	h := s.Handler()
	do := func(target string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body))
		req.Header.Set("x-cellhive-scope-token", tok)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}

	longKey := strings.Repeat("k", 513)
	if rr := do("/v1/kv/put?ns=acme&key="+longKey, []byte("v")); rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "key_too_large") {
		t.Fatalf("long key = %d %s, want 400 key_too_large", rr.Code, rr.Body.String())
	}
	bigMeta := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("m"), 1025))
	if rr := do("/v1/kv/put?ns=acme&key=m&metadata="+url.QueryEscape(bigMeta), []byte("v")); rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "metadata_too_large") {
		t.Fatalf("big metadata = %d %s, want 400 metadata_too_large", rr.Code, rr.Body.String())
	}
	future := time.Now().Add(time.Hour).Unix()
	// ADR-183: any positive TTL is accepted (sub-60s included); expiry is
	// enforced lazily on read, so a 1s key vanishes without waiting for a sweep.
	if rr := do("/v1/kv/put?ns=acme&key=m&expiration_ttl=59", []byte("v")); rr.Code != http.StatusOK {
		t.Fatalf("ttl 59 = %d %s, want 200 (no 60s floor)", rr.Code, rr.Body.String())
	}
	if rr := do("/v1/kv/put?ns=acme&key=m&expiration_ttl=60", []byte("v")); rr.Code != http.StatusOK {
		t.Fatalf("ttl 60 = %d %s, want 200", rr.Code, rr.Body.String())
	}
	zero := time.Now().Add(-time.Minute).Unix()
	if rr := do("/v1/kv/put?ns=acme&key=m&expiration_ttl=0", []byte("v")); rr.Code != http.StatusBadRequest {
		t.Fatalf("ttl 0 = %d %s, want 400", rr.Code, rr.Body.String())
	}
	_ = zero
	past := time.Now().Add(-time.Minute).Unix()
	if rr := do("/v1/kv/put?ns=acme&key=m&expiration="+strconv.FormatInt(past, 10), []byte("v")); rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "future") {
		t.Fatalf("past expiration = %d %s, want 400 expiration in the future", rr.Code, rr.Body.String())
	}
	if rr := do("/v1/kv/put?ns=acme&key=m&expiration="+strconv.FormatInt(future, 10), []byte("v")); rr.Code != http.StatusOK {
		t.Fatalf("future expiration = %d %s, want 200", rr.Code, rr.Body.String())
	}
	// A 1 KiB metadata value (exactly at the limit) is accepted.
	okMeta := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("m"), 1024))
	if rr := do("/v1/kv/put?ns=acme&key=m&metadata="+url.QueryEscape(okMeta), []byte("v")); rr.Code != http.StatusOK {
		t.Fatalf("1KiB metadata = %d %s, want 200", rr.Code, rr.Body.String())
	}
}
