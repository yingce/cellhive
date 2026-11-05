package compaction

import (
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
	"testing"
)

func TestFoldGCPreservesEpochMarker(t *testing.T) {
	ctx, b, m, s, e := newCompactionFixture(t)
	marker, err := m.MarkEpoch(ctx, s, e, 0)
	if err != nil {
		t.Fatal(err)
	}
	appendSegment(t, m, s, e, snapshotSegment(t, e, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	appendSegment(t, m, s, e, deltaSegment(t, e, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
	res, err := Compact(ctx, m, s, e, Options{MinSegments: 1, GC: true})
	if err != nil || !res.Compacted {
		t.Fatalf("fold: %+v %v", res, err)
	}
	if _, _, err := b.Get(ctx, marker); err != nil {
		t.Fatalf("fold deleted epoch marker: %v", err)
	}
	segs, err := m.RestoreChain(ctx, s, e)
	if err != nil {
		t.Fatal(err)
	}
	var raws [][]byte
	for _, seg := range segs {
		raws = append(raws, seg.Raw)
	}
	if err := restore.ValidateChain(raws); err != nil {
		t.Fatal(err)
	}
	if wm, err := m.DurableWatermarkChain(ctx, s, e); err != nil || wm != 1 {
		t.Fatalf("watermark=%d err=%v", wm, err)
	}
	if ok, err := m.HasCompleteSnapshot(ctx, s, e); err != nil || !ok {
		t.Fatalf("complete=%v err=%v", ok, err)
	}
}

func TestFoldGCPreservesUnprovenNames(t *testing.T) {
	for _, name := range []string{"0.link", "legacy.batch", "0-1.m.batch", "unknown.ltx", "00.snapshot"} {
		t.Run(name, func(t *testing.T) {
			ctx, b, m, s, e := newCompactionFixture(t)
			snap := snapshotSegment(t, e, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})
			appendSegment(t, m, s, e, snap)
			appendSegment(t, m, s, e, deltaSegment(t, e, 4096, 1, 1, map[uint32][]byte{1: make([]byte, 4096)}))
			raw := snap
			if name == "0.link" {
				raw = ltx.Encode(ltx.Header{Kind: ltx.KindLink, Epoch: e}, nil)
			}
			key := replica.Prefix(s, e) + name
			if _, err := b.Put(ctx, key, raw); err != nil {
				t.Fatal(err)
			}
			res, err := Compact(ctx, m, s, e, Options{MinSegments: 1, GC: true})
			if err != nil || !res.Compacted {
				t.Fatalf("fold=%+v err=%v", res, err)
			}
			if _, _, err := b.Get(ctx, key); err != nil {
				t.Fatalf("fold removed unproven name: %v", err)
			}
		})
	}
}
