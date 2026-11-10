package compaction

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

func nativeManager(t *testing.T) (*replica.Manager, cell.Scope) {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return replica.New(b), cell.Scope{Namespace: "native", Class: "__kv__", ID: "level"}
}
func appendNative(t *testing.T, m *replica.Manager, s cell.Scope, level uint16, min, max uint64, numbers ...uint32) string {
	t.Helper()
	pages := make([]ltx.WALPage, len(numbers))
	for i, pg := range numbers {
		pages[i] = ltx.WALPage{PageNo: pg, Data: bytes.Repeat([]byte{byte(max)}, 512)}
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 2, MinTXID: min, MaxTXID: max}, pages)
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := m.AppendLevel(t.Context(), s, 1, level, raw)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestNativeLevelsPromoteIncrementalRangesAndSnapshot(t *testing.T) {
	m, s := nativeManager(t)
	appendNative(t, m, s, 0, 1, 1, 1, 2)
	appendNative(t, m, s, 0, 2, 3, 2)
	r, err := Compact(t.Context(), m, s, 1, Options{})
	if err != nil || !r.Compacted || r.MinTxID != 1 || r.MaxTxID != 3 {
		t.Fatalf("first=%+v err=%v", r, err)
	}
	appendNative(t, m, s, 0, 4, 4, 1)
	r, err = Compact(t.Context(), m, s, 1, Options{})
	if err != nil || !r.Compacted || r.MinTxID != 4 || r.MaxTxID != 4 {
		t.Fatalf("tail=%+v err=%v", r, err)
	}
	for _, level := range []uint16{2, 3} {
		r, err = CompactLevel(t.Context(), m, s, 1, level, Options{})
		if err != nil || !r.Compacted || r.MinTxID != 1 || r.MaxTxID != 4 {
			t.Fatalf("level%d=%+v err=%v", level, r, err)
		}
	}
	r, err = Snapshot(t.Context(), m, s, 1, Options{})
	if err != nil || !r.Compacted {
		t.Fatalf("snapshot=%+v err=%v", r, err)
	}
	chain, err := m.RestoreChain(t.Context(), s, 1)
	if err != nil || len(chain) != 1 {
		t.Fatalf("chain=%d err=%v", len(chain), err)
	}
	if !strings.Contains(chain[0].Key, "/0009/") {
		t.Fatalf("snapshot key=%s", chain[0].Key)
	}
	ix, err := restore.IndexChain([][]byte{chain[0].Raw})
	if err != nil {
		t.Fatal(err)
	}
	for pg, want := range map[uint32]byte{1: 4, 2: 3} {
		data, err := ix.Page(pg)
		if err != nil || !bytes.Equal(data, bytes.Repeat([]byte{want}, 512)) {
			t.Fatalf("page%d err=%v", pg, err)
		}
	}
	keys, err := m.B.List(t.Context(), replica.EpochPrefix(s, 1))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("unexpected sidecar %s", key)
		}
	}
}

type nativeReadGuard struct{ bucket.Bucket }

func (b nativeReadGuard) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasSuffix(key, ".ltx") {
		return nil, "", errors.New("unexpected LTX download")
	}
	return b.Bucket.Get(ctx, key)
}
func TestNativeCompactionSkipsCoveredBodies(t *testing.T) {
	m, s := nativeManager(t)
	appendNative(t, m, s, 0, 1, 1, 1, 2)
	if _, err := Compact(t.Context(), m, s, 1, Options{}); err != nil {
		t.Fatal(err)
	}
	m.B = nativeReadGuard{m.B}
	r, err := Compact(t.Context(), m, s, 1, Options{})
	if err != nil || r.Compacted || r.Skipped != "already compacted" {
		t.Fatalf("result=%+v err=%v", r, err)
	}
}
func TestNativeCompactionClipsOverlappingSource(t *testing.T) {
	m, s := nativeManager(t)
	appendNative(t, m, s, 1, 1, 3, 1, 2)
	appendNative(t, m, s, 0, 1, 6, 1, 2)
	r, err := Compact(t.Context(), m, s, 1, Options{})
	if err != nil || r.MinTxID != 4 || r.MaxTxID != 6 {
		t.Fatalf("result=%+v err=%v", r, err)
	}
}
func TestNativeCompactionVerifyBeforePublication(t *testing.T) {
	m, s := nativeManager(t)
	appendNative(t, m, s, 0, 1, 1, 1, 2)
	calls := 0
	lost := errors.New("owner changed")
	_, err := Compact(t.Context(), m, s, 1, Options{Verify: func(context.Context) error {
		calls++
		if calls == 2 {
			return lost
		}
		return nil
	}})
	if !errors.Is(err, lost) {
		t.Fatalf("err=%v", err)
	}
	keys, err := m.ListLevel(t.Context(), s, 1, 1)
	if err != nil || len(keys) != 0 {
		t.Fatalf("published after owner loss: %v %v", keys, err)
	}
}

func TestNativeDryRunUsesSamePlanWithoutReadingBodies(t *testing.T) {
	m, s := nativeManager(t)
	appendNative(t, m, s, 1, 1, 3, 1, 2)
	appendNative(t, m, s, 0, 1, 6, 1, 2)
	underlying := m.B
	before, err := underlying.List(t.Context(), s.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	m.B = nativeReadGuard{underlying}
	dry, err := DryRun(t.Context(), m, s, 1, Options{})
	if err != nil || dry.Compacted || dry.Inputs != 1 || dry.MinTxID != 4 || dry.MaxTxID != 6 {
		t.Fatalf("dry=%+v err=%v", dry, err)
	}
	after, err := underlying.List(t.Context(), s.Key()+"/")
	if err != nil || strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatalf("dry run mutated objects: %v", err)
	}
	m.B = underlying
	actual, err := Compact(t.Context(), m, s, 1, Options{})
	if err != nil || actual.Inputs != dry.Inputs || actual.MinTxID != dry.MinTxID || actual.MaxTxID != dry.MaxTxID {
		t.Fatalf("actual=%+v dry=%+v err=%v", actual, dry, err)
	}
}

func TestNativeAdditiveCompactionKeepsActiveReaderAtOriginalCut(t *testing.T) {
	m, s := nativeManager(t)
	base := appendNative(t, m, s, 0, 1, 1, 1, 2)
	if _, err := Compact(t.Context(), m, s, 1, Options{}); err != nil {
		t.Fatal(err)
	}
	old, err := m.NewPageFetcher(t.Context(), s, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	appendNative(t, m, s, 0, 2, 2, 2)
	for _, level := range []uint16{1, 2, 3} {
		result, err := CompactLevel(t.Context(), m, s, 1, level, Options{})
		if err != nil || !result.Compacted {
			t.Fatalf("level %d result=%+v err=%v", level, result, err)
		}
	}
	if _, err := Snapshot(t.Context(), m, s, 1, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.B.Get(t.Context(), base); err != nil {
		t.Fatalf("additive compaction removed source: %v", err)
	}
	// First access is AFTER all promotions; no page cache can hide deletion.
	got, err := old.Page(t.Context(), 2)
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{1}, 512)) {
		t.Fatalf("original cut: %v", err)
	}
	current, err := m.NewPageFetcher(t.Context(), s, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	got, err = current.Page(t.Context(), 2)
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{2}, 512)) {
		t.Fatalf("new cut: %v", err)
	}
	keys, err := m.B.List(t.Context(), s.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("custom object: %s", key)
		}
	}
}

func (b nativeReadGuard) OpenReader(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	if strings.HasSuffix(key, ".ltx") {
		return nil, 0, "", errors.New("unexpected LTX stream")
	}
	return b.Bucket.(bucket.StreamReader).OpenReader(ctx, key)
}
