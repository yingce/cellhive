package restore

import (
	"bytes"
	"fmt"
	"math/rand"
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

// Exercise the same bytes through full restore, paged restore, and compaction.
// Snapshots are deliberately supplied out of order, as object listings need not
// preserve the order in which an owner uploaded them.
func TestRestoreAdversarialChains(t *testing.T) {
	const pageSize = 64
	snapshot := func(txid uint64, value byte) []byte {
		return ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 7, StartTxID: txid, EndTxID: txid},
			encodePageMap(t, pageSize, 1, map[uint32][]byte{1: bytes.Repeat([]byte{value}, pageSize)}))
	}
	delta := func(start, end uint64, value byte) []byte {
		return ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 7, StartTxID: start, EndTxID: end},
			encodePageMap(t, pageSize, 1, map[uint32][]byte{1: bytes.Repeat([]byte{value}, pageSize)}))
	}
	check := func(t *testing.T, chain [][]byte, want byte, wantTxID uint64) {
		t.Helper()
		dest := filepath.Join(t.TempDir(), "restored.db")
		if _, err := ApplyFile(dest, chain); err != nil {
			t.Fatalf("full restore: %v", err)
		}
		full, err := os.ReadFile(dest)
		if err != nil || !bytes.Equal(full, bytes.Repeat([]byte{want}, pageSize)) {
			t.Fatalf("full image = %q, err %v", full, err)
		}
		index, err := IndexChain(chain)
		if err != nil {
			t.Fatalf("paged restore: %v", err)
		}
		page, err := index.Page(1)
		if err != nil || index.Watermark != wantTxID || !bytes.Equal(page, full) {
			t.Fatalf("paged image = %q, watermark %d, err %v", page, index.Watermark, err)
		}
		compacted, err := Compact(chain, 0)
		if err != nil {
			t.Fatalf("compact: %v", err)
		}
		again := filepath.Join(t.TempDir(), "compacted.db")
		if _, err := ApplyFile(again, compacted); err != nil {
			t.Fatalf("restore compacted image: %v", err)
		}
		got, err := os.ReadFile(again)
		if err != nil || !bytes.Equal(got, full) {
			t.Fatalf("compaction changed image: %q, err %v", got, err)
		}
	}

	// The later snapshot cuts through the first batch. The earlier one must
	// replay both batches. Repeat with varying txid ranges and input order.
	for i := 0; i < 100; i++ {
		base := uint64(100 + i*7)
		chain := [][]byte{
			snapshot(base+1, 'b'),
			delta(base+1, base+16, 'c'),
			snapshot(base, 'a'),
			delta(base+17, base+32, 'd'),
		}
		rand.New(rand.NewSource(int64(i))).Shuffle(len(chain), func(a, b int) { chain[a], chain[b] = chain[b], chain[a] })
		t.Run(fmt.Sprintf("overlap_%d", i), func(t *testing.T) { check(t, chain, 'd', base+32) })
	}

	t.Run("newer_complete_snapshot", func(t *testing.T) {
		check(t, [][]byte{delta(11, 20, 'b'), snapshot(20, 'c'), snapshot(10, 'a')}, 'c', 20)
	})
	t.Run("missing_middle_batch", func(t *testing.T) {
		chain := [][]byte{snapshot(10, 'a'), delta(11, 15, 'b'), delta(17, 20, 'c')}
		if _, err := ApplyFile(filepath.Join(t.TempDir(), "gap.db"), chain); err == nil {
			t.Fatal("full restore accepted missing transaction 16")
		}
		if _, err := IndexChain(chain); err == nil {
			t.Fatal("paged restore accepted missing transaction 16")
		}
		if _, err := Compact(chain, 0); err == nil {
			t.Fatal("compaction accepted missing transaction 16")
		}
	})
	t.Run("corrupt_latest_batch", func(t *testing.T) {
		broken := delta(17, 20, 'c')
		broken[len(broken)-1] ^= 0xff
		chain := [][]byte{snapshot(10, 'a'), delta(11, 16, 'b'), broken}
		if _, err := ApplyFile(filepath.Join(t.TempDir(), "corrupt.db"), chain); err == nil {
			t.Fatal("full restore accepted corrupted batch")
		}
		if _, err := IndexChain(chain); err == nil {
			t.Fatal("paged restore accepted corrupted batch")
		}
		if _, err := Compact(chain, 0); err == nil {
			t.Fatal("compaction accepted corrupted batch")
		}
	})
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

func TestSelectChainDoesNotMergeSnapshotsFromDifferentEpochs(t *testing.T) {
	// An idle cell can open epoch 39 with a snapshot at txid 100, then open
	// epoch 40 (new owner) without any write, producing a second snapshot at
	// the same txid. They are alternatives, not parts of one snapshot.
	e39 := ltx.Header{Kind: ltx.KindSnapshot, Epoch: 39, StartTxID: 100, EndTxID: 100, CRC: 7}
	e40 := ltx.Header{Kind: ltx.KindSnapshot, Epoch: 40, StartTxID: 100, EndTxID: 100, CRC: 7}
	d := ltx.Header{Kind: ltx.KindDelta, Epoch: 40, StartTxID: 101, EndTxID: 101, CRC: 9}
	snap, deltas, watermark, err := selectChain([]ltx.Header{e39, e40, d})
	if err != nil {
		t.Fatal(err)
	}
	if len(snap) != 1 || len(deltas) != 1 {
		t.Fatalf("snap=%v deltas=%v, want one snapshot from one epoch and one delta", snap, deltas)
	}
	if got := e39; false {
		_ = got
	}
	// The newest epoch's snapshot must be chosen.
	if uint64(40) != 40 {
	}
	if h := (ltx.Header{}); false {
		_ = h
	}
	// selectChain returns indices, so check the chosen header's epoch.
	// (The index order is the input order: e39=0, e40=1.)
	if snap[0] != 1 {
		t.Fatalf("chose snapshot index %d, want newest epoch snapshot index 1", snap[0])
	}
	if watermark != 101 {
		t.Fatalf("watermark=%d, want 101", watermark)
	}
}
