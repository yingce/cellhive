package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/compaction"
)

func gcScopeSnapshot(t *testing.T, epoch, txid uint64) []byte {
	t.Helper()
	return nativeTestFile(t, 1, txid, bytes.Repeat([]byte{byte(epoch)}, 4096))
}

func TestGCScopeSafeRetiresOldEpochOnOwner(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "gc-safe"}
	for epoch := uint64(1); epoch <= 3; epoch++ {
		o, err := s.Owner.Claim(ctx, sc, time.Now())
		if err != nil || o.Epoch != epoch {
			t.Fatalf("claim %+v %v", o, err)
		}
		if _, _, err := s.Replica.Append(ctx, sc, epoch, gcScopeSnapshot(t, epoch, epoch)); err != nil {
			t.Fatal(err)
		}
		if _, err := compaction.Compact(ctx, s.Replica, sc, epoch, compaction.Options{MinSegments: 1}); err != nil {
			t.Fatal(err)
		}
		if epoch < 3 {
			if err := s.Owner.Release(ctx, sc, epoch); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := s.Store.Cell(ctx, sc); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{
		"scope": sc.String(), "safe": true, "keep_previous": 0,
		"grace_seconds": 1,
	})
	call := func() *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		s.handleControlGCScope(rr, httptest.NewRequest(http.MethodPost, "/v1/control/gc/scope", bytes.NewReader(raw)))
		return rr
	}
	if rr := call(); rr.Code != http.StatusOK {
		t.Fatalf("gc status = %d: %s", rr.Code, rr.Body.String())
	}
	time.Sleep(1200 * time.Millisecond) // object upload-age grace
	rr := call()
	if rr.Code != http.StatusOK {
		t.Fatalf("gc status = %d: %s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Retired int `json:"retired"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Retired == 0 {
		t.Fatalf("response = %s", rr.Body.String())
	}
	old, err := s.Replica.ListSegments(ctx, sc, 1)
	if err != nil || len(old) != 0 {
		t.Fatalf("old epoch remains: %v %v", old, err)
	}
	previous, err := s.Replica.ListSegments(ctx, sc, 2)
	if err != nil || len(previous) == 0 {
		t.Fatalf("previous epoch was deleted: %v %v", previous, err)
	}
	ok, err := s.Replica.EpochRetired(ctx, sc, 1)
	if err != nil || !ok {
		t.Fatalf("retired mark missing: %v %v", ok, err)
	}
	keys, err := s.Bucket.List(ctx, sc.Key()+"/ltx/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".ltx") && key != sc.Key()+"/ltx/retired.json" {
			t.Fatalf("private GC file: %s", key)
		}
	}
	if rr := call(); rr.Code != http.StatusOK {
		t.Fatalf("idempotent retry: %d %s", rr.Code, rr.Body.String())
	}
}
