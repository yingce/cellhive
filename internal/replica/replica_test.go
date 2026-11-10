package replica

import (
	"bytes"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"os"
	"strings"
	"testing"
)

func newManager(t *testing.T) *Manager {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return New(b)
}
func nativeBlob(t *testing.T, lo, hi uint64, v byte) []byte {
	t.Helper()
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: lo, MaxTXID: hi}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{v}, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestAppendListRead(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	raw := nativeBlob(t, 1, 1, 'a')
	key, etag, err := m.Append(t.Context(), sc, 2, raw)
	if err != nil || etag == "" {
		t.Fatalf("append %q %v", etag, err)
	}
	if key != EpochPrefix(sc, 2)+"0000/0000000000000001-0000000000000001.ltx" {
		t.Fatalf("key=%s", key)
	}
	keys, err := m.ListSegments(t.Context(), sc, 2)
	if err != nil || len(keys) != 1 || keys[0] != key {
		t.Fatalf("keys=%v %v", keys, err)
	}
	got, err := m.ReadSegment(t.Context(), key)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("read differs %v", err)
	}
	h, pages, err := ltx.DecodeNativePages(got)
	if err != nil || h.MinTXID != 1 || len(pages) != 1 || !bytes.Equal(pages[0].Data, bytes.Repeat([]byte{'a'}, 512)) {
		t.Fatalf("native decode %v", err)
	}
}
func TestAppendSnapshotFileUsesCanonicalRangeObject(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "file"}
	raw := nativeBlob(t, 1, 7, 1)
	path := t.TempDir() + "/snapshot"
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	key, _, err := m.AppendSnapshotFile(t.Context(), sc, 1, path, int64(len(raw)))
	if err != nil || key != EpochPrefix(sc, 1)+"0000/0000000000000001-0000000000000007.ltx" {
		t.Fatalf("file=%s %v", key, err)
	}
}
func TestAppendSnapshotReplayKeepsCanonicalRange(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__do__", ID: "host"}
	raw := nativeBlob(t, 1, 1, 1)
	key, _, err := m.Append(t.Context(), sc, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.Append(t.Context(), sc, 1, raw); err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.Append(t.Context(), sc, 1, nativeBlob(t, 1, 1, 2)); err != nil {
		t.Fatalf("overwrite=%v", err)
	}
	got, err := m.ReadSegment(t.Context(), key)
	if err != nil || !bytes.Equal(got, nativeBlob(t, 1, 1, 2)) {
		t.Fatalf("ordinary replay %v", err)
	}
}
func TestAppendRejectsPrivateEpochHeader(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__d1__", ID: "db"}
	raw := []byte("\x4c\x54\x58\x31\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x07\x56\x50\x41\xe6\x70\x72\x69\x76\x61\x74\x65")
	if _, _, err := m.Append(t.Context(), sc, 2, raw); err == nil {
		t.Fatal("private header accepted")
	}
}
func TestAppendRejectsInvalidSegment(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__d1__", ID: "db"}
	if _, _, err := m.Append(t.Context(), sc, 1, []byte("invalid")); err == nil {
		t.Fatal("invalid file accepted")
	}
}
func TestValidateKey(t *testing.T) {
	if err := ValidateKey("cells/a/b/c/ltx/e1/1-1.ltx", "cells/"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateKey("nodes/x.json", "cells/"); err == nil {
		t.Fatal("outside prefix accepted")
	}
}
func TestAppendBatchAndRestore(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__d1__", ID: "batch"}
	key, _, err := m.AppendBatch(t.Context(), sc, 7, [][]byte{nativeBlob(t, 1, 1, 1), nativeBlob(t, 2, 2, 2), nativeBlob(t, 3, 3, 3)})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := m.ListSegments(t.Context(), sc, 7)
	if err != nil || len(keys) != 1 || keys[0] != key {
		t.Fatalf("batch keys %v %v", keys, err)
	}
	segs, err := m.Restore(t.Context(), sc, 7)
	if err != nil || len(segs) != 1 {
		t.Fatalf("batch restore %d %v", len(segs), err)
	}
	h, pages, err := ltx.DecodeNativePages(segs[0].Raw)
	if err != nil || h.MinTXID != 1 || h.MaxTXID != 3 || len(pages) != 1 || pages[0].Data[0] != 3 {
		t.Fatalf("batch state header=%+v err=%v", h, err)
	}
}
func TestRestoreOrdersSegments(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__d1__", ID: "order"}
	for _, tx := range []uint64{3, 2, 1} {
		if _, _, err := m.Append(t.Context(), sc, 5, nativeBlob(t, tx, tx, byte(tx))); err != nil {
			t.Fatal(err)
		}
	}
	segs, err := m.Restore(t.Context(), sc, 5)
	if err != nil || len(segs) != 3 {
		t.Fatalf("restore %d %v", len(segs), err)
	}
	for i, s := range segs {
		h, err := ltx.ParseNativeHeader(s.Raw)
		if err != nil || h.MinTXID != uint64(i+1) {
			t.Fatalf("order %+v %v", h, err)
		}
	}
}
func TestNativeL1RestorePreferenceWithoutManifest(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "l1"}
	for tx := uint64(1); tx <= 3; tx++ {
		if _, _, err := m.Append(t.Context(), sc, 2, nativeBlob(t, tx, tx, byte(tx))); err != nil {
			t.Fatal(err)
		}
	}
	key, _, err := m.AppendL1(t.Context(), sc, 2, nativeBlob(t, 1, 2, 2))
	if err != nil {
		t.Fatal(err)
	}
	keys, err := m.ListL0Segments(t.Context(), sc, 2)
	if err != nil || len(keys) != 3 {
		t.Fatalf("L0 %v %v", keys, err)
	}
	for _, key := range keys {
		if strings.HasPrefix(key, L1Prefix(sc, 2)) {
			t.Fatal("L1 leaked into L0")
		}
	}
	segs, err := m.Restore(t.Context(), sc, 2)
	if err != nil || len(segs) != 2 || segs[0].Key != key || segs[1].Header.EndTxID != 3 {
		t.Fatalf("plan=%+v %v", segs, err)
	}
}
func TestRestoreKeepsDeltaCrossingL1Watermark(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__invocations__", ID: "main"}
	if _, _, err := m.AppendL1(t.Context(), sc, 3, nativeBlob(t, 1, 10, 1)); err != nil {
		t.Fatal(err)
	}
	for _, r := range [][2]uint64{{9, 12}, {13, 13}} {
		if _, _, err := m.Append(t.Context(), sc, 3, nativeBlob(t, r[0], r[1], 2)); err != nil {
			t.Fatal(err)
		}
	}
	segs, err := m.Restore(t.Context(), sc, 3)
	if err != nil || len(segs) != 3 || segs[1].Header.StartTxID != 9 || segs[2].Header.StartTxID != 13 {
		t.Fatalf("overlap plan=%+v %v", segs, err)
	}
}
func TestRestoreDeduplicatesMixedGranularity(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "dedup"}
	delta := nativeBlob(t, 2, 2, 2)
	if _, _, err := m.AppendBatch(t.Context(), sc, 1, [][]byte{nativeBlob(t, 1, 1, 1), delta}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.Append(t.Context(), sc, 1, delta); err != nil {
		t.Fatal(err)
	}
	segs, err := m.Restore(t.Context(), sc, 1)
	if err != nil || len(segs) != 1 || segs[0].Header.EndTxID != 2 {
		t.Fatalf("dedup=%+v %v", segs, err)
	}
}
func TestCovers(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "app", Class: "__kv__", ID: "main"}
	if ok, err := m.Covers(t.Context(), sc, 1, 5); err != nil || ok {
		t.Fatalf("empty=%v %v", ok, err)
	}
	if _, _, err := m.AppendL1(t.Context(), sc, 1, nativeBlob(t, 1, 10, 1)); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.Covers(t.Context(), sc, 1, 10); err != nil || !ok {
		t.Fatalf("covered=%v %v", ok, err)
	}
	if ok, err := m.Covers(t.Context(), sc, 1, 11); err != nil || ok {
		t.Fatalf("beyond=%v %v", ok, err)
	}
}
