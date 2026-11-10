package replica

import (
	"bytes"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"errors"
	"testing"
)

func TestNativeContinuationRetryKeepsOriginalAndRejectsDelta(t *testing.T) {
	m := newManager(t)
	ctx := t.Context()
	s := cell.Scope{Namespace: "n", Class: "__kv__", ID: "retry"}
	h := ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 8, MaxTXID: 8, WALOffset: 32, WALSalt1: 4, WALSalt2: 5, Timestamp: 1000}
	key := EpochPrefix(s, 2) + "0000/0000000000000008-0000000000000008.ltx"
	if err := m.CheckContinuation(ctx, s, 2, h); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("missing seed: %v", err)
	}
	if _, _, err := m.resourceObjects().GetVersion(ctx, key); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatal("read-only proof created seed")
	}
	if err := m.AppendContinuation(ctx, s, 2, h); err != nil {
		t.Fatal(err)
	}
	before, _, err := m.resourceObjects().GetVersion(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	h.WALSalt1 = 99    // new local WAL after process restart
	h.Timestamp = 2000 // retry must retain the original publication timestamp
	if err := m.AppendContinuation(ctx, s, 2, h); err != nil {
		t.Fatal(err)
	}
	after, _, err := m.resourceObjects().GetVersion(ctx, key)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("retry overwrote original seed")
	}
	h.Commit = 2
	if err := m.CheckContinuation(ctx, s, 2, h); err == nil {
		t.Fatal("different database shape accepted")
	}
	h.Commit = 1
	delta, err := ltx.EncodeNativePages(h, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.resourceObjects().Put(ctx, key, delta); err != nil {
		t.Fatal(err)
	}
	if err := m.CheckContinuation(ctx, s, 2, h); err == nil {
		t.Fatal("real missing delta accepted as continuation")
	}
}
