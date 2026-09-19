package replica

import (
	"context"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

func newManager(t *testing.T) *Manager {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	return New(b)
}

func TestAppendListRead(t *testing.T) {
	ctx := context.Background()
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}

	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 2, StartTxID: 1, EndTxID: 1}, []byte("payload"))
	key, etag, err := m.Append(ctx, sc, 2, seg)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if etag == "" {
		t.Fatalf("empty etag")
	}
	if key != Prefix(sc, 2)+"1-1.ltx" {
		t.Fatalf("key = %q", key)
	}

	keys, err := m.ListSegments(ctx, sc, 2)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(keys) != 1 || keys[0] != key {
		t.Fatalf("list = %v, want [%s]", keys, key)
	}

	got, err := m.ReadSegment(ctx, key)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if _, payload, err := ltx.Decode(got); err != nil || string(payload) != "payload" {
		t.Fatalf("decoded payload = %q, %v", payload, err)
	}
}

func TestAppendRejectsEpochMismatch(t *testing.T) {
	ctx := context.Background()
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__d1__", ID: "db"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, EndTxID: 1}, []byte("x"))
	if _, _, err := m.Append(ctx, sc, 2, seg); err == nil {
		t.Fatalf("expected epoch mismatch error")
	}
}

func TestAppendRejectsInvalidSegment(t *testing.T) {
	ctx := context.Background()
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__d1__", ID: "db"}
	if _, _, err := m.Append(ctx, sc, 1, []byte("not a segment")); err == nil {
		t.Fatalf("expected invalid segment error")
	}
}

func TestValidateKey(t *testing.T) {
	if err := ValidateKey("cells/a/b/c/ltx/e1/1-1.ltx", "cells/"); err != nil {
		t.Fatalf("valid key rejected: %v", err)
	}
	if err := ValidateKey("nodes/x.json", "cells/"); err == nil {
		t.Fatalf("expected rejection for key outside prefix")
	}
}

func TestAppendBatchAndRestore(t *testing.T) {
	ctx := context.Background()
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__d1__", ID: "batched"}

	seg1 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 7, StartTxID: 1, EndTxID: 1}, []byte("a"))
	seg2 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 7, StartTxID: 2, EndTxID: 2}, []byte("b"))
	seg3 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 7, StartTxID: 3, EndTxID: 3}, []byte("c"))

	key, _, err := m.AppendBatch(ctx, sc, 7, [][]byte{seg1, seg2, seg3})
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}
	keys, err := m.ListSegments(ctx, sc, 7)
	if err != nil || len(keys) != 1 || keys[0] != key {
		t.Fatalf("batch produced %v objects, want 1 (%s): %v", keys, key, err)
	}

	segs, err := m.Restore(ctx, sc, 7)
	if err != nil {
		t.Fatalf("restore batch: %v", err)
	}
	if len(segs) != 3 {
		t.Fatalf("restore returned %d segments, want 3", len(segs))
	}
	if string(segs[0].Raw) != string(seg1) || string(segs[2].Raw) != string(seg3) {
		t.Fatalf("batch segments not restored in order")
	}
}

func TestRestoreOrdersSegments(t *testing.T) {
	ctx := context.Background()
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__d1__", ID: "db"}

	// Append out of order: two deltas then a snapshot.
	_, _, _ = m.Append(ctx, sc, 5, ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 5, StartTxID: 3, EndTxID: 3}, []byte("d3")))
	_, _, _ = m.Append(ctx, sc, 5, ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 5, StartTxID: 2, EndTxID: 2}, []byte("d2")))
	_, _, _ = m.Append(ctx, sc, 5, ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 5, EndTxID: 1}, []byte("snap")))

	segs, err := m.Restore(ctx, sc, 5)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if len(segs) != 3 {
		t.Fatalf("segment count = %d, want 3", len(segs))
	}
	if segs[0].Header.Kind != ltx.KindSnapshot {
		t.Fatalf("first segment kind = %v, want snapshot", segs[0].Header.Kind)
	}
	if segs[1].Header.StartTxID != 2 || segs[2].Header.StartTxID != 3 {
		t.Fatalf("delta order wrong: %d,%d", segs[1].Header.StartTxID, segs[2].Header.StartTxID)
	}
}

func TestManifestAndL1RestorePreference(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "l1"}
	const epoch = uint64(2)

	snap := ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: epoch, StartTxID: 0, EndTxID: 0}, []byte("s"))
	d1 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: epoch, StartTxID: 1, EndTxID: 1}, []byte("d1"))
	d2 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: epoch, StartTxID: 2, EndTxID: 2}, []byte("d2"))
	for _, seg := range [][]byte{snap, d1, d2} {
		if _, _, err := m.Append(ctx, sc, epoch, seg); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	l0, err := m.ListL0Segments(ctx, sc, epoch)
	if err != nil || len(l0) != 3 {
		t.Fatalf("l0 = %v, %v", l0, err)
	}

	// Publish an L1 snapshot covering through txid 1, plus a manifest.
	l1seg := ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: epoch, StartTxID: 1, EndTxID: 1, Flags: ltx.SnapshotFlags(0)}, []byte("l1"))
	k, _, err := m.AppendL1(ctx, sc, epoch, l1seg)
	if err != nil {
		t.Fatalf("append L1: %v", err)
	}
	if err := m.PutManifest(ctx, sc, epoch, Manifest{MinTxID: 0, MaxTxID: 1, Objects: []string{k}}); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	if _, ok, err := m.ReadManifest(ctx, sc, epoch); err != nil || !ok {
		t.Fatalf("read manifest ok=%v err=%v", ok, err)
	}
	l0after, _ := m.ListL0Segments(ctx, sc, epoch)
	if len(l0after) != 3 {
		t.Fatalf("unexpected L0 listing: %v", l0after)
	}
	for _, k := range l0after {
		if strings.HasPrefix(k, L1Prefix(sc, epoch)) {
			t.Fatalf("L1 object leaked into L0 listing: %v", l0after)
		}
	}

	// Restore must use L1 + only the delta newer than MaxTxID.
	segs, err := m.Restore(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if len(segs) != 2 {
		t.Fatalf("restore segments = %d, want 2 (L1 snapshot + d2)", len(segs))
	}
	if segs[0].Header.Kind != ltx.KindSnapshot || string(segs[0].Raw[ltx.HeaderSize:]) != "l1" {
		t.Fatalf("first segment is not the L1 snapshot: %+v", segs[0].Header)
	}
	if string(segs[1].Raw[ltx.HeaderSize:]) != "d2" {
		t.Fatalf("second segment payload = %q, want d2", segs[1].Raw[ltx.HeaderSize:])
	}
}

func TestRestoreDeduplicatesMixedGranularity(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "dedup"}
	const epoch = uint64(1)
	snap := ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: epoch, StartTxID: 0, EndTxID: 0}, []byte("s"))
	d1 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: epoch, StartTxID: 1, EndTxID: 1}, []byte("d1"))
	// Owner uploads a batch (one object), and recovery later re-uploads the
	// follower's single copy of d1.
	if _, _, err := m.AppendBatch(ctx, sc, epoch, [][]byte{snap, d1}); err != nil {
		t.Fatalf("append batch: %v", err)
	}
	if _, _, err := m.Append(ctx, sc, epoch, d1); err != nil {
		t.Fatalf("append single: %v", err)
	}
	segs, err := m.Restore(ctx, sc, epoch)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if len(segs) != 2 {
		t.Fatalf("restore returned %d segments, want 2 (deduplicated)", len(segs))
	}
}

// TestCovers covers the disk-janitor safety predicate (ADR-123): a manifest
// covers local writes up to its MaxTxID, not beyond.
func TestCovers(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	m := New(b)
	sc := cell.Scope{Namespace: "app", Class: "__kv__", ID: "main"}
	if ok, err := m.Covers(ctx, sc, 1, 5); err != nil || ok {
		t.Fatalf("covers without manifest = %v, %v; want false", ok, err)
	}
	if err := m.PutManifest(ctx, sc, 1, Manifest{Epoch: 1, MaxTxID: 10}); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	if ok, err := m.Covers(ctx, sc, 1, 10); err != nil || !ok {
		t.Fatalf("covers at max = %v, %v; want true", ok, err)
	}
	if ok, err := m.Covers(ctx, sc, 1, 11); err != nil || ok {
		t.Fatalf("covers beyond max = %v, %v; want false", ok, err)
	}
}
