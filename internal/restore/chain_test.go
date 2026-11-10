package restore

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/ltx"
)

func TestNativeRestoreChoosesLongestContiguousOverlap(t *testing.T) {
	page := func(value byte) map[uint32][]byte {
		return map[uint32][]byte{1: bytes.Repeat([]byte{value}, 512)}
	}
	snapshot := func(txid uint64, value byte) []byte {
		return nativeTestFile(t, 512, 1, 1, txid, page(value))
	}
	delta := func(start, end uint64, value byte) []byte {
		return nativeTestFile(t, 512, 1, start, end, page(value))
	}
	// The newest snapshot overlaps a compacted delta. Its later pages remain
	// necessary, so the suffix must include that whole delta.
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
	if err != nil || !bytes.Equal(image, bytes.Repeat([]byte{'d'}, 512)) {
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
	compact, err := Compact(chain)
	if err != nil {
		t.Fatalf("compact fallback chain: %v", err)
	}
	h, err := ltx.ValidateNative(compact[0])
	if err != nil || h.MaxTXID != 66 {
		t.Fatalf("compacted watermark = %d, err %v", h.MaxTXID, err)
	}
}

// Exercise the same bytes through full restore, paged restore, and compaction.
// Snapshots are deliberately supplied out of order, as object listings need not
// preserve the order in which an owner uploaded them.
func TestRestoreAdversarialChains(t *testing.T) {
	const pageSize = 512
	snapshot := func(txid uint64, value byte) []byte {
		return nativeTestFile(t, pageSize, 1, 1, txid, map[uint32][]byte{1: bytes.Repeat([]byte{value}, pageSize)})
	}
	delta := func(start, end uint64, value byte) []byte {
		return nativeTestFile(t, pageSize, 1, start, end, map[uint32][]byte{1: bytes.Repeat([]byte{value}, pageSize)})
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
		compacted, err := Compact(chain)
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
		if _, err := Compact(chain); err == nil {
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
		if _, err := Compact(chain); err == nil {
			t.Fatal("compaction accepted corrupted batch")
		}
	})
}

func TestNativeRestoreAcceptsCompactedOverlap(t *testing.T) {
	encode := func(min, max uint64, value byte) []byte {
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: min, MaxTXID: max}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{value}, 512)}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	chain := [][]byte{encode(1, 34, 'a'), encode(34, 49, 'b')}
	dest := filepath.Join(t.TempDir(), "overlap.db")
	if _, err := ApplyFile(dest, chain); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(dest)
	if err != nil || !bytes.Equal(raw, bytes.Repeat([]byte{'b'}, 512)) {
		t.Fatalf("restored wrong final image: %v", err)
	}
	ix, err := IndexChain(chain)
	if err != nil {
		t.Fatal(err)
	}
	page, err := ix.Page(1)
	if err != nil || ix.Watermark != 49 || !bytes.Equal(page, raw) {
		t.Fatalf("paged restore disagrees: watermark=%d err=%v", ix.Watermark, err)
	}
}

// Epoch clipping and conflicting predecessor coverage are exercised in
// replica/epoch_chain_test.go. Native file headers carry no epoch or marker.
func TestNativeContinuationRequiresContiguousPredecessor(t *testing.T) {
	page := map[uint32][]byte{1: make([]byte, 512)}
	base := nativeTestFile(t, 512, 1, 1, 5, page)
	continuation := nativeTestFile(t, 512, 1, 7, 7, nil)
	if err := ValidateChain([][]byte{base, continuation}); err == nil {
		t.Fatal("accepted missing predecessor TXID6")
	}
	base = nativeTestFile(t, 512, 1, 1, 6, page)
	if err := ValidateChain([][]byte{base, continuation}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateChain([][]byte{continuation}); err == nil {
		t.Fatal("continuation accepted without snapshot")
	}
}

func TestNativeSnapshotUsesMaxTXIDAsRestoreWatermark(t *testing.T) {
	headers := []ltx.NativeHeader{{MinTXID: 1, MaxTXID: 5}, {MinTXID: 1, MaxTXID: 10}, {MinTXID: 11, MaxTXID: 12}}
	order, watermark, err := selectNativeChain(headers)
	if err != nil || len(order) != 2 || order[0] != 1 || order[1] != 2 || watermark != 12 {
		t.Fatalf("selection=%v watermark=%d err=%v", order, watermark, err)
	}
	headers[2].MinTXID = 12
	if _, _, err := selectNativeChain(headers); err == nil {
		t.Fatal("accepted missing transaction11")
	}
}

func TestNativeValidateChainRejectsMissingSnapshotPage(t *testing.T) {
	// This file has a valid native index/trailer/CRC, but no page1. A TXID-only
	// continuity proof must not authorize deleting its predecessor epoch.
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateChain([][]byte{raw}); err == nil {
		t.Fatal("incomplete snapshot accepted as independently recoverable")
	}
}

func TestNativeValidateChainChecksPageCoverageAndShape(t *testing.T) {
	encode := func(ps, commit uint32, lo, hi uint64, pages ...ltx.WALPage) []byte {
		t.Helper()
		raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: ps, Commit: commit, MinTXID: lo, MaxTXID: hi}, pages)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	p := func(n uint32) ltx.WALPage { return ltx.WALPage{PageNo: n, Data: make([]byte, 512)} }
	base := encode(512, 2, 1, 5, p(1), p(2))
	continuation := encode(512, 2, 6, 6)
	if err := ValidateChain([][]byte{base, continuation}); err != nil {
		t.Fatal(err)
	}
	wrongSize := encode(1024, 2, 6, 6)
	if err := ValidateChain([][]byte{base, wrongSize}); err == nil {
		t.Fatal("page-size mismatch accepted")
	}
	corrupt := append([]byte(nil), continuation...)
	corrupt[len(corrupt)-1] ^= 1
	if err := ValidateChain([][]byte{base, corrupt}); err == nil {
		t.Fatal("bad file CRC accepted")
	}
	shrink := encode(512, 1, 6, 6)
	growWithoutPage := encode(512, 2, 7, 7)
	if err := ValidateChain([][]byte{base, shrink, growWithoutPage}); err == nil {
		t.Fatal("truncated page resurrected")
	}
	growWithPage := encode(512, 2, 7, 7, p(2))
	if err := ValidateChain([][]byte{base, shrink, growWithPage}); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreRejectsPrivateSegmentFormat(t *testing.T) {
	raw := []byte("\x4c\x54\x58\x31\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x07\x56\x50\x41\xe6\x70\x72\x69\x76\x61\x74\x65")
	if _, err := IndexChain([][]byte{raw}); err == nil {
		t.Error("paged restore accepted private 44-byte segment")
	}
	if _, err := ApplyFile(filepath.Join(t.TempDir(), "legacy.db"), [][]byte{raw}); err == nil {
		t.Error("full restore accepted private 44-byte segment")
	}
}
