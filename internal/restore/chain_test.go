package restore

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cellhive/internal/ltx"
)

func TestRestoreFallsBackToContiguousSnapshot(t *testing.T) {
	page := func(value byte) map[uint32][]byte {
		return map[uint32][]byte{1: bytes.Repeat([]byte{value}, 64)}
	}
	snapshot := func(txid uint64, value byte) []byte {
		return ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 38, StartTxID: txid, EndTxID: txid},
			encodePageMap(t, 64, 1, page(value)))
	}
	delta := func(start, end uint64, value byte) []byte {
		return ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 38, StartTxID: start, EndTxID: end},
			encodePageMap(t, 64, 1, page(value)))
	}
	// The newest snapshot overlaps a batched delta. The older snapshot has an
	// exact suffix, and the whole batch must remain in that suffix.
	s33 := snapshot(33, 'a')
	s34 := snapshot(34, 'b')
	d34 := delta(34, 49, 'c')
	d50 := delta(50, 66, 'd')
	chain := [][]byte{s34, d50, s33, d34}
	dest := filepath.Join(t.TempDir(), "restored.db")
	if _, err := ApplyFile(dest, chain); err != nil {
		t.Fatalf("apply fallback chain: %v", err)
	}
	image, err := os.ReadFile(dest)
	if err != nil || !bytes.Equal(image, bytes.Repeat([]byte{'d'}, 64)) {
		t.Fatalf("restored image = %q, %v", image, err)
	}
	ix, err := IndexChain(chain)
	if err != nil {
		t.Fatalf("index fallback chain: %v", err)
	}
	got, err := ix.Page(1)
	if err != nil || ix.Watermark != 66 || !bytes.Equal(got, image) {
		t.Fatalf("paged image = %q, watermark %d, err %v", got, ix.Watermark, err)
	}
	compact, err := Compact(chain, 0)
	if err != nil {
		t.Fatalf("compact fallback chain: %v", err)
	}
	h, _, err := ltx.Decode(compact[0])
	if err != nil || h.EndTxID != 66 {
		t.Fatalf("compacted watermark = %d, err %v", h.EndTxID, err)
	}
}

func TestRestoreRejectsOverlapWithoutContiguousFallback(t *testing.T) {
	page := map[uint32][]byte{1: bytes.Repeat([]byte{'a'}, 64)}
	snapshot := ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 1, StartTxID: 34, EndTxID: 34},
		encodePageMap(t, 64, 1, page))
	delta := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 34, EndTxID: 49},
		encodePageMap(t, 64, 1, page))
	chain := [][]byte{snapshot, delta}
	if _, err := ApplyFile(filepath.Join(t.TempDir(), "unsafe.db"), chain); err == nil || !strings.Contains(err.Error(), "non-contiguous chain") {
		t.Fatalf("ApplyFile error = %v, want overlap rejection", err)
	}
	if _, err := IndexChain(chain); err == nil || !strings.Contains(err.Error(), "non-contiguous chain") {
		t.Fatalf("IndexChain error = %v, want overlap rejection", err)
	}
}
