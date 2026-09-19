package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/r2"
)

// TestR2ListDelimiterAndInclude covers ADR-168: list(delimiter) rolls keys up
// into delimitedPrefixes, and include=httpMetadata,customMetadata opts into the
// per-object sidecar read.
func TestR2ListDelimiterAndInclude(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	store := r2.New(b)
	ctx := context.Background()
	for _, k := range []string{"a/1", "b/1", "c"} {
		if _, err := store.Put(ctx, "acme", "files", k, []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if _, err := store.PutWithMeta(ctx, "acme", "files", "c", []byte("v"), &r2.Metadata{
		HTTP: map[string]string{"contentType": "text/plain"}, Custom: map[string]string{"who": "bob"},
	}); err != nil {
		t.Fatalf("put meta: %v", err)
	}
	s := New(Deps{R2: store})

	list := func(query string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/v1/r2/list?bucket=files&"+query, nil)
		req = req.WithContext(context.WithValue(req.Context(), ctxScopeNS, "acme"))
		rr := httptest.NewRecorder()
		s.handleR2List(rr, req)
		return rr
	}

	rr := list("delimiter=/&include=httpMetadata,customMetadata")
	if rr.Code != http.StatusOK {
		t.Fatalf("list = %d %s", rr.Code, rr.Body.String())
	}
	var out struct {
		Objects           []map[string]any `json:"objects"`
		DelimitedPrefixes []string         `json:"delimitedPrefixes"`
		Truncated         bool             `json:"truncated"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Objects) != 1 || out.Objects[0]["key"] != "c" {
		t.Fatalf("objects = %+v, want only c", out.Objects)
	}
	if len(out.DelimitedPrefixes) != 2 || out.DelimitedPrefixes[0] != "a/" || out.DelimitedPrefixes[1] != "b/" {
		t.Fatalf("delimitedPrefixes = %v", out.DelimitedPrefixes)
	}
	hm, _ := out.Objects[0]["httpMetadata"].(map[string]any)
	cm, _ := out.Objects[0]["customMetadata"].(map[string]any)
	if hm["contentType"] != "text/plain" || cm["who"] != "bob" {
		t.Fatalf("included metadata = %v / %v", hm, cm)
	}

	// include without metadata, and invalid include.
	rr = list("delimiter=/")
	if rr.Code != http.StatusOK {
		t.Fatalf("plain list = %d", rr.Code)
	}
	if rr := list("include=bogus"); rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid include = %d, want 400", rr.Code)
	}
}
