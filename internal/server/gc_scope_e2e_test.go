package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/compaction"
	"cellhive/internal/ltx"
)

func gcScopeSnapshot(t *testing.T, epoch, txid uint64) []byte {
	t.Helper()
	payload, err := ltx.EncodeWALPageMap(64, 1, 1, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{byte(epoch)}, 64)}})
	if err != nil {
		t.Fatal(err)
	}
	return ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: epoch, StartTxID: txid, EndTxID: txid}, payload)
}

func TestGCScopeSafeRetiresOldEpochOnOwner(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "gc-safe"}
	now := time.Now()
	if _, err := s.Owner.Claim(ctx, sc, now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Replica.Append(ctx, sc, 1, gcScopeSnapshot(t, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Replica.MarkSnapshotDone(ctx, sc, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := compaction.Compact(ctx, s.Replica, sc, 1, compaction.Options{MinSegments: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Owner.Release(ctx, sc, 1); err != nil {
		t.Fatal(err)
	}
	owner, err := s.Owner.Claim(ctx, sc, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if owner.Epoch != 2 {
		t.Fatalf("epoch = %d, want 2", owner.Epoch)
	}
	if _, _, err := s.Replica.Append(ctx, sc, owner.Epoch, gcScopeSnapshot(t, owner.Epoch, 2)); err != nil {
		t.Fatal(err)
	}
	if err := s.Replica.MarkSnapshotDone(ctx, sc, owner.Epoch, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := compaction.Compact(ctx, s.Replica, sc, owner.Epoch, compaction.Options{MinSegments: 1}); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{
		"scope": sc.String(), "safe": true, "keep_previous": 0,
		"grace_seconds": 1, "keep_l1_generations": 2,
	})
	call := func() *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		s.handleControlGCScope(rr, httptest.NewRequest(http.MethodPost, "/v1/control/gc/scope", bytes.NewReader(raw)))
		return rr
	}
	if rr := call(); rr.Code != http.StatusOK {
		t.Fatalf("gc status = %d: %s", rr.Code, rr.Body.String())
	}
	time.Sleep(1200 * time.Millisecond) // tombstone grace
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
	ok, err := s.Replica.EpochRetired(ctx, sc, 1)
	if err != nil || !ok {
		t.Fatalf("epoch 1 retired = %v, %v", ok, err)
	}
}
