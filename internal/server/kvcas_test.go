package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newBodyReader(body []byte) io.Reader {
	if body == nil {
		return nil
	}
	return bytes.NewReader(body)
}

// TestKVCasAndIncrE2E covers the HTTP surface of the existence-conditioned
// put (if_exists=absent|present, the "onlyIf" semantics) and the atomic
// increment (ADR-182): a failed condition is a clean 200 {applied:false}
// (not an error), a winning condition writes through, unrelated keys never
// interfere, and incr creates/adds/negates atomically with non-integer
// rejection and its own condition-guarded form.
func TestKVCasAndIncrE2E(t *testing.T) {
	ctx := context.Background()
	s := newScopeServer(t)
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "KV", "default", "test"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	tok := mint(t, s, "acme", "kv", "KV")
	h := s.Handler()
	do := func(method, target, token string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, newBodyReader(body))
		req.Header.Set("x-cellhive-scope-token", token)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	type putResT struct {
		OK      bool `json:"ok"`
		Applied bool `json:"applied"`
	}
	var putRes putResT

	// absent on a missing key -> applied:true (create-if-absent).
	rr := do(http.MethodPost, "/v1/kv/put?ns=acme&key=k&if_exists=absent", tok, []byte("v1"))
	if rr.Code != http.StatusOK {
		t.Fatalf("cas create = %d: %s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &putRes); err != nil || !putRes.OK || !putRes.Applied {
		t.Fatalf("cas create body = %s (%v)", rr.Body.String(), err)
	}
	rr = do(http.MethodGet, "/v1/kv/get?ns=acme&key=k", tok, nil)
	if rr.Body.String() != "v1" {
		t.Fatalf("get = %q, want v1", rr.Body.String())
	}

	// absent on an existing key -> 200 {applied:false}, value untouched.
	rr = do(http.MethodPost, "/v1/kv/put?ns=acme&key=k&if_exists=absent", tok, []byte("v2"))
	if rr.Code != http.StatusOK {
		t.Fatalf("failed condition = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &putRes); err != nil || putRes.Applied {
		t.Fatalf("failed condition body = %s (%v)", rr.Body.String(), err)
	}
	rr = do(http.MethodGet, "/v1/kv/get?ns=acme&key=k", tok, nil)
	if rr.Body.String() != "v1" {
		t.Fatalf("value after failed condition = %q, want v1", rr.Body.String())
	}

	// present on an existing key -> applied:true (value swap).
	rr = do(http.MethodPost, "/v1/kv/put?ns=acme&key=k&if_exists=present", tok, []byte("v2"))
	if err := json.Unmarshal(rr.Body.Bytes(), &putRes); err != nil || !putRes.Applied {
		t.Fatalf("present swap body = %s (%v)", rr.Body.String(), err)
	}
	rr = do(http.MethodGet, "/v1/kv/get?ns=acme&key=k", tok, nil)
	if rr.Body.String() != "v2" {
		t.Fatalf("get after swap = %q, want v2", rr.Body.String())
	}

	// Unconditional put still works and reports applied:true.
	rr = do(http.MethodPost, "/v1/kv/put?ns=acme&key=k", tok, []byte("v3"))
	if err := json.Unmarshal(rr.Body.Bytes(), &putRes); err != nil || !putRes.Applied {
		t.Fatalf("unconditional put body = %s (%v)", rr.Body.String(), err)
	}

	// Unrelated key's write does not affect a conditional put on "k".
	if code := kvReq(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=unrelated", tok, []byte("noise")); code != http.StatusOK {
		t.Fatalf("unrelated put = %d", code)
	}
	rr = do(http.MethodPost, "/v1/kv/put?ns=acme&key=k&if_exists=present", tok, []byte("v4"))
	if err := json.Unmarshal(rr.Body.Bytes(), &putRes); err != nil || !putRes.Applied {
		t.Fatalf("condition affected by unrelated write = %s (%v)", rr.Body.String(), err)
	}

	// present on a missing key -> applied:false.
	rr = do(http.MethodPost, "/v1/kv/put?ns=acme&key=gone&if_exists=present", tok, []byte("x"))
	if rr.Code != http.StatusOK {
		t.Fatalf("present on missing = %d, want 200", rr.Code)
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &putRes); err != nil || putRes.Applied {
		t.Fatalf("present on missing body = %s (%v)", rr.Body.String(), err)
	}

	// Bad if_exists -> 400.
	rr = do(http.MethodPost, "/v1/kv/put?ns=acme&key=k&if_exists=sometimes", tok, []byte("v"))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("bad if_exists = %d, want 400", rr.Code)
	}

	// Incr: create at 5, add 2 -> 7; response carries value+applied+txid.
	rr = do(http.MethodPost, "/v1/kv/incr?ns=acme&key=hits&by=5", tok, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("incr create = %d: %s", rr.Code, rr.Body.String())
	}
	var incrRes struct {
		Value   int64 `json:"value"`
		Applied bool  `json:"applied"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &incrRes); err != nil || incrRes.Value != 5 || !incrRes.Applied {
		t.Fatalf("incr body = %s (%v)", rr.Body.String(), err)
	}
	rr = do(http.MethodPost, "/v1/kv/incr?ns=acme&key=hits&by=2", tok, nil)
	if err := json.Unmarshal(rr.Body.Bytes(), &incrRes); err != nil || incrRes.Value != 7 {
		t.Fatalf("incr add body = %s (%v)", rr.Body.String(), err)
	}

	// Negative delta.
	rr = do(http.MethodPost, "/v1/kv/incr?ns=acme&key=hits&by=-7", tok, nil)
	if err := json.Unmarshal(rr.Body.Bytes(), &incrRes); err != nil || incrRes.Value != 0 {
		t.Fatalf("incr negative body = %s (%v)", rr.Body.String(), err)
	}

	// Non-integer target -> 400.
	if code := kvReq(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=text", tok, []byte("abc")); code != http.StatusOK {
		t.Fatalf("put text = %d", code)
	}
	rr = do(http.MethodPost, "/v1/kv/incr?ns=acme&key=text", tok, nil)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("incr non-integer = %d, want 400: %s", rr.Code, rr.Body.String())
	}

	// Condition-guarded increment: absent on a missing key applies; present
	// on a missing key yields {applied:false}.
	rr = do(http.MethodPost, "/v1/kv/incr?ns=acme&key=guarded&by=1&if_exists=absent", tok, nil)
	if err := json.Unmarshal(rr.Body.Bytes(), &incrRes); err != nil || !incrRes.Applied || incrRes.Value != 1 {
		t.Fatalf("cas incr create body = %s (%v)", rr.Body.String(), err)
	}
	rr = do(http.MethodPost, "/v1/kv/incr?ns=acme&key=missing&by=1&if_exists=present", tok, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("cas incr failed condition = %d, want 200", rr.Code)
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &incrRes); err != nil || incrRes.Applied {
		t.Fatalf("cas incr failed condition body = %s (%v)", rr.Body.String(), err)
	}
}
