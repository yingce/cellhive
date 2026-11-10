package compaction

import (
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
	"fmt"
	"testing"
)

func TestNativeFoldPreservesContinuationLineage(t *testing.T) {
	ctx, b, m, s, e := newCompactionFixture(t)
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	appendSegment(t, m, s, e, raw)
	next := e + 1
	if err := m.AppendContinuation(ctx, s, next, ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, WALOffset: 32}); err != nil {
		t.Fatal(err)
	}
	raw, err = ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 3, MaxTXID: 3}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	appendSegment(t, m, s, next, raw)
	res, err := Compact(ctx, m, s, next, Options{MinSegments: 1})
	if err != nil || !res.Compacted {
		t.Fatalf("fold=%+v %v", res, err)
	}
	seed := fmt.Sprintf("%s0000/%016x-%016x.ltx", replica.EpochPrefix(s, next), 2, 2)
	if _, _, err := b.Get(ctx, seed); err != nil {
		t.Fatalf("seed missing %v", err)
	}
	segs, err := m.RestoreChain(ctx, s, next)
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
	if wm, err := m.DurableWatermarkChain(ctx, s, next); err != nil || wm != 3 {
		t.Fatalf("watermark=%d %v", wm, err)
	}
	if complete, err := m.HasCompleteSnapshot(ctx, s, next); err != nil || complete {
		t.Fatalf("continuation mistaken for independent snapshot %v %v", complete, err)
	}
}

func TestNativeFoldPreservesUnprovenNames(t *testing.T) {
	for _, name := range []string{"unknown.proof", "unknown.ltx", "other/file"} {
		t.Run(name, func(t *testing.T) {
			ctx, b, m, s, e := newCompactionFixture(t)
			raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 2}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
			if err != nil {
				t.Fatal(err)
			}
			appendSegment(t, m, s, e, raw)
			key := replica.EpochPrefix(s, e) + name
			if _, err := b.Put(ctx, key, []byte("opaque")); err != nil {
				t.Fatal(err)
			}
			res, err := Compact(ctx, m, s, e, Options{MinSegments: 1})
			if err != nil || !res.Compacted {
				t.Fatalf("fold=%+v %v", res, err)
			}
			if raw, _, err := b.Get(ctx, key); err != nil || string(raw) != "opaque" {
				t.Fatalf("unknown object changed %v", err)
			}
		})
	}
}
