package compaction

import (
	"bytes"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
	"context"
	"testing"
)

func TestCompactUsesRetainedPredecessorForOverlappingBaseline(t *testing.T) {
	ctx := context.Background()
	b, e := bucket.NewFSBucket(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	m := replica.New(b)
	sc := cell.Scope{Namespace: "p0", Class: "__kv__", ID: "compact"}
	payload, e := ltx.EncodeWALPageMap(64, 1, 1, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{7}, 64)}})
	if e != nil {
		t.Fatal(e)
	}
	for _, h := range []ltx.Header{{Kind: ltx.KindSnapshot, Epoch: 22, StartTxID: 3766, EndTxID: 3766}, {Kind: ltx.KindDelta, Epoch: 22, StartTxID: 3767, EndTxID: 3768}, {Kind: ltx.KindMarker, Epoch: 23, StartTxID: 3769, EndTxID: 3768}, {Kind: ltx.KindDelta, Epoch: 23, StartTxID: 3769, EndTxID: 3769}, {Kind: ltx.KindSnapshot, Epoch: 23, StartTxID: 3771, EndTxID: 3771}, {Kind: ltx.KindDelta, Epoch: 23, StartTxID: 3770, EndTxID: 3772}} {
		if _, _, e = m.Append(ctx, sc, h.Epoch, ltx.Encode(h, payload)); e != nil {
			t.Fatal(e)
		}
	}
	res, e := Compact(ctx, m, sc, 23, Options{GC: true})
	if e != nil {
		t.Fatal(e)
	}
	if !res.Compacted || res.MaxTxID != 3772 {
		t.Fatalf("result %+v", res)
	}
	man, ok, e := m.ReadManifest(ctx, sc, 23)
	if e != nil || !ok {
		t.Fatalf("manifest: %v %v", ok, e)
	}
	for _, key := range man.Objects {
		raw, e := m.ReadSegment(ctx, key)
		if e != nil {
			t.Fatal(e)
		}
		h, _, e := ltx.Decode(raw)
		if e != nil || h.Epoch != 23 {
			t.Fatalf("wrong output epoch: %+v %v", h, e)
		}
	}
	segs, e := m.Restore(ctx, sc, 23)
	if e != nil {
		t.Fatal(e)
	}
	var raw [][]byte
	for _, s := range segs {
		raw = append(raw, s.Raw)
	}
	ix, e := restore.IndexChain(raw)
	if e != nil || ix.Watermark != 3772 {
		t.Fatalf("restore %v %v", ix, e)
	}
	old, e := m.ListL0Segments(ctx, sc, 22)
	if e != nil || len(old) == 0 {
		t.Fatal("compaction removed predecessor")
	}
}
