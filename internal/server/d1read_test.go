package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"cellhive/internal/cell"
)

// TestD1ReadSkipsCapture verifies that a read-only D1 statement does not go
// through the capture path (no Ensure/owner check), while a mutation does.
func TestD1ReadSkipsCapture(t *testing.T) {
	srv, _, _ := captureServer(t)
	var calls int64
	srv.Capture.Owner = func(context.Context, cell.Scope) (bool, uint64, error) {
		atomic.AddInt64(&calls, 1)
		return true, 1, nil
	}
	tok := mint(t, srv, "acme", "d1", "DB")
	h := srv.Handler()
	do := func(path, sqlText string) int {
		body, _ := json.Marshal(map[string]any{"sql": sqlText})
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		req.Header.Set("x-cellhive-scope-token", tok)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr.Code
	}
	if code := do("/v1/d1/exec?ns=acme&db=bench", "CREATE TABLE t(a)"); code != http.StatusOK {
		t.Fatalf("exec = %d, want 200", code)
	}
	if atomic.LoadInt64(&calls) == 0 {
		t.Fatalf("mutation did not go through capture")
	}
	before := atomic.LoadInt64(&calls)
	if code := do("/v1/d1/query?ns=acme&db=bench", "SELECT 1"); code != http.StatusOK {
		t.Fatalf("read = %d, want 200", code)
	}
	if after := atomic.LoadInt64(&calls); after != before {
		t.Fatalf("read went through capture: Ensure calls %d -> %d", before, after)
	}
}
