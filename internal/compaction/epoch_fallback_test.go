package compaction

import (
	"bytes"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
	"testing"
)

func TestNativeCompactionKeepsExactPredecessorWithOverlappingRanges(t *testing.T) {
	m, sc := nativeManager(t)
	put := func(e, lo, hi uint64, zero bool) {
		t.Helper()
		var pages []ltx.WALPage
		if !zero {
			pages = []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{byte(hi)}, 512)}}
		}
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: lo, MaxTXID: hi}, pages)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = m.Append(t.Context(), sc, e, raw); err != nil {
			t.Fatal(err)
		}
	}
	put(22, 1, 3766, false)
	put(22, 3767, 3768, false)
	put(23, 3769, 3769, true)
	put(23, 3770, 3770, false)
	put(23, 3770, 3772, false)
	result, err := Compact(t.Context(), m, sc, 23, Options{})
	if err != nil || !result.Compacted || result.MinTxID != 3769 || result.MaxTxID != 3772 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	check := func(sc cell.Scope) {
		chain, err := m.RestoreChain(t.Context(), sc, 23)
		if err != nil {
			t.Fatal(err)
		}
		ix, err := restore.IndexChain(raws(chain))
		if err != nil || ix.Watermark != 3772 {
			t.Fatalf("chain=%v err=%v", ix, err)
		}
		if chain[0].Header.Epoch != 22 {
			t.Fatal("predecessor lost")
		}
	}
	check(sc)
	// A late predecessor file crossing the continuation cut cannot replace it.
	put(22, 1, 3771, false)
	check(sc)
}
