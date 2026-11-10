package replica

import (
	"cellhive/internal/bucket"
	"context"
	"fmt"
	"strings"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

func putNativeEpochFile(t *testing.T, m *Manager, sc cell.Scope, epoch uint64, level int, min, max uint64, empty bool) string {
	t.Helper()
	var pages []ltx.WALPage
	if !empty {
		pages = []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}}
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: min, MaxTXID: max}, pages)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("%s%04x/%016x-%016x.ltx", EpochPrefix(sc, epoch), level, min, max)
	if _, err := m.resourceObjects().ConditionalCreate(t.Context(), key, raw); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestNativeEpochChainClipsLateTailAndSkipsWrongPredecessor(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "native-chain"}
	base := putNativeEpochFile(t, m, sc, 1, 9, 1, 3, false)
	tail := putNativeEpochFile(t, m, sc, 1, 1, 4, 5, false)
	putNativeEpochFile(t, m, sc, 1, 0, 6, 99, false) // fenced tail
	putNativeEpochFile(t, m, sc, 2, 9, 1, 4, false)  // whole but too old: cannot supply cut5
	continuation := putNativeEpochFile(t, m, sc, 3, 0, 6, 6, true)
	delta := putNativeEpochFile(t, m, sc, 3, 0, 7, 7, false)
	plan, err := m.nativeRestorePlan(t.Context(), sc, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{base, tail, continuation, delta}
	if len(plan) != len(want) {
		t.Fatalf("plan=%+v", plan)
	}
	for i, f := range plan {
		if f.key != want[i] {
			t.Fatalf("plan[%d]=%s want %s", i, f.key, want[i])
		}
	}
	wm, err := m.DurableWatermarkChain(t.Context(), sc, 3)
	if err != nil || wm != 7 {
		t.Fatalf("watermark=%d err=%v", wm, err)
	}
	segs, err := m.RestoreChain(context.Background(), sc, 3)
	if err != nil || len(segs) != 4 {
		t.Fatalf("segments=%d err=%v", len(segs), err)
	}
	for _, seg := range segs {
		if _, err := ltx.ValidateNative(seg.Raw); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNativeEpochChainRejectsMissingPredecessor(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "missing-base"}
	putNativeEpochFile(t, m, sc, 1, 9, 1, 4, false)
	putNativeEpochFile(t, m, sc, 3, 0, 6, 6, true)
	if _, err := m.nativeRestorePlan(t.Context(), sc, 3); err == nil {
		t.Fatal("accepted cut5 without predecessor ending5")
	}
}

func TestNativeRestorePlanChoosesLongestCoverageAcrossLevels(t *testing.T) {
	files := []epochFile{
		{key: "snapshot", level: 9, min: 1, max: 5},
		{key: "l0", level: 0, min: 6, max: 6},
		{key: "l1", level: 1, min: 4, max: 9},
		{key: "l2", level: 2, min: 4, max: 9},
		{key: "tail", level: 0, min: 10, max: 10},
	}
	plan, err := planEpochFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 3 || plan[1].key != "l2" || plan[2].key != "tail" {
		t.Fatalf("plan=%+v", plan)
	}
	files = append(files, epochFile{key: "gap", min: 12, max: 12})
	if _, err := planEpochFiles(files); err == nil {
		t.Fatal("accepted missing txid11")
	}
}

func TestNativeRestoreChecksListedRangeAgainstHeader(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "range-mismatch"}
	key := putNativeEpochFile(t, m, sc, 1, 0, 1, 5, false)
	if _, err := m.readNativePlan(t.Context(), []epochFile{{key: key, epoch: 1, min: 1, max: 6}}); err == nil {
		t.Fatal("accepted path/header mismatch")
	}
}

func TestNativeEpochChainStopsAtNewestCompleteSnapshot(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "stop-base"}
	// An irrelevant predecessor deliberately has a non-native object name.
	if _, err := m.resourceObjects().ConditionalCreate(t.Context(), EpochPrefix(sc, 1)+"0000/broken.ltx", []byte("broken")); err != nil {
		t.Fatal(err)
	}
	key := putNativeEpochFile(t, m, sc, 2, 9, 1, 12, false)
	plan, err := m.nativeRestorePlan(t.Context(), sc, 2)
	if err != nil || len(plan) != 1 || plan[0].key != key {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
}

type rejectLTXBodyReads struct{ bucket.Bucket }

func (b rejectLTXBodyReads) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasSuffix(key, ".ltx") {
		return nil, "", fmt.Errorf("unexpected LTX body GET: %s", key)
	}
	return b.Bucket.Get(ctx, key)
}
func TestNativeWatermarkNeedsNoLTXBodyReads(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "names-only"}
	putNativeEpochFile(t, m, sc, 1, 9, 1, 12, false)
	putNativeEpochFile(t, m, sc, 2, 0, 13, 13, true)
	putNativeEpochFile(t, m, sc, 2, 1, 14, 25, false)
	m.B = rejectLTXBodyReads{m.B}
	wm, err := m.DurableWatermarkChain(t.Context(), sc, 2)
	if err != nil || wm != 25 {
		t.Fatalf("watermark=%d err=%v", wm, err)
	}
}

func TestNativeSnapshotProofRejectsMissingPagesAndPredecessorDependency(t *testing.T) {
	m := newManager(t)
	ctx := t.Context()
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "snapshot-proof"}
	putNativeEpochFile(t, m, sc, 1, 9, 1, 5, false)
	putNativeEpochFile(t, m, sc, 2, 0, 6, 6, true)
	if ok, err := m.HasCompleteSnapshot(ctx, sc, 2); err != nil || ok {
		t.Fatalf("continuation independent=%v %v", ok, err)
	}
	putNativeEpochFile(t, m, sc, 2, 9, 1, 6, true)
	if ok, err := m.HasCompleteSnapshot(ctx, sc, 2); err == nil || ok {
		t.Fatalf("missing page proof=%v %v", ok, err)
	}
	// A newer complete native snapshot is sufficient, with no manifest/index.
	putNativeEpochFile(t, m, sc, 2, 9, 1, 7, false)
	if ok, err := m.HasCompleteSnapshot(ctx, sc, 2); err != nil || !ok {
		t.Fatalf("complete proof=%v %v", ok, err)
	}
	keys, err := m.resourceObjects().ListPrefix(ctx, sc.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("proof wrote sidecar %s", key)
		}
	}
}
