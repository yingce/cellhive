package compaction

import (
	"encoding/binary"
	"os"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

// TestCompactWholeObjectsBelowAndAbove1GiB compacts two real L0 chains.
// Eight objects stay under 1GiB. Eight more, skipping SQLite's 1GiB lock
// page, store more than 1GiB. Each chain becomes one L1.
func TestCompactWholeObjectsBelowAndAbove1GiB(t *testing.T) {
	const pageSize = 4096
	const objects = 8
	cases := []struct {
		name  string
		pages int
		above bool
	}{
		{name: "below-1GiB", pages: 8 * 8192, above: false},
		{name: "above-1GiB", pages: 262144 + 2048, above: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, s := nativeManager(t)
			s.ID = tc.name
			per := tc.pages / objects
			var stored int64
			for n := 0; n < objects; n++ {
				from := n * per
				count := per
				if n == objects-1 {
					count = tc.pages - from
				}
				nBytes := writeBulkL0(t, m, s, uint64(n+1), pageSize, from, count, tc.pages)
				stored += nBytes
				t.Logf("l0[%d] stored=%d pages=%d", n+1, nBytes, count)
			}
			const gib = int64(1 << 30)
			if tc.above && stored <= gib {
				t.Fatalf("above case stored %d, want > 1GiB", stored)
			}
			if !tc.above && stored >= gib {
				t.Fatalf("below case stored %d, want < 1GiB", stored)
			}
			res, err := CompactLevel(t.Context(), m, s, 1, 1, Options{MaxInputBytes: -1, MinObjects: objects})
			if err != nil {
				t.Fatal(err)
			}
			if !res.Compacted || res.Inputs != objects || res.MinTxID != 1 || res.MaxTxID != uint64(objects) {
				t.Fatalf("result=%+v", res)
			}
			infos, err := m.ListLevelInfos(t.Context(), s, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(infos) != 1 {
				t.Fatalf("l1 objects=%d, want 1", len(infos))
			}
			out := infos[0].Size
			t.Logf("input_bytes=%d output_bytes=%d inputs=%d range=%d-%d", stored, out, res.Inputs, res.MinTxID, res.MaxTxID)
			if tc.above && (res.InputBytes <= gib || out <= gib) {
				t.Fatalf("above input=%d output=%d, want both > 1GiB", res.InputBytes, out)
			}
			if !tc.above && (res.InputBytes >= gib || out >= gib) {
				t.Fatalf("below input=%d output=%d, want both < 1GiB", res.InputBytes, out)
			}
		})
	}
}

// TestCompactOneGiBSnapshot times one 1GiB snapshot (262144 x 4096 pages,
// entirely before SQLite's lock page) folded into one L1.
func TestCompactOneGiBSnapshot(t *testing.T) {
	const pageSize = 4096
	const pages = 262144
	m, s := nativeManager(t)
	s.ID = "one-gib-snapshot"
	writeStart := time.Now()
	stored := writeBulkL0(t, m, s, 1, pageSize, 0, pages, pages)
	t.Logf("write stored=%d elapsed=%s", stored, time.Since(writeStart).Round(time.Millisecond))
	start := time.Now()
	res, err := CompactLevel(t.Context(), m, s, 1, 1, Options{MaxInputBytes: -1})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("compact input_bytes=%d elapsed=%s inputs=%d range=%d-%d", res.InputBytes, elapsed.Round(time.Millisecond), res.Inputs, res.MinTxID, res.MaxTxID)
	if !res.Compacted || res.Inputs != 1 || res.MinTxID != 1 || res.MaxTxID != 1 {
		t.Fatalf("result=%+v", res)
	}
	infos, err := m.ListLevelInfos(t.Context(), s, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 {
		t.Fatalf("l1 objects=%d, want 1", len(infos))
	}
	t.Logf("output_bytes=%d elapsed=%s", infos[0].Size, elapsed.Round(time.Millisecond))
}

func writeBulkL0(t *testing.T, m *replica.Manager, s cell.Scope, tx uint64, pageSize, from, count, total int) int64 {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "l0-*.ltx")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	defer os.Remove(path)
	last := bulkPageNo(total-1, pageSize)
	enc, err := ltx.NewNativeEncoder(f, ltx.NativeHeader{
		PageSize: uint32(pageSize),
		Commit:   last,
		MinTXID:  tx,
		MaxTXID:  tx,
	})
	if err != nil {
		t.Fatal(err)
	}
	page := make([]byte, pageSize)
	state := tx * 0x9E3779B97F4A7C15
	for i := 0; i < count; i++ {
		fillPage(&state, page)
		if err := enc.WritePage(ltx.WALPage{PageNo: bulkPageNo(from+i, pageSize), Data: page}); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.AppendLevelFile(t.Context(), s, 1, 0, path, info.Size()); err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func bulkPageNo(index, pageSize int) uint32 {
	lock := uint32(0x40000000)/uint32(pageSize) + 1
	n := uint32(index + 1)
	if n >= lock {
		n++
	}
	return n
}

func fillPage(state *uint64, dst []byte) {
	x := *state | 1
	for i := 0; i < len(dst); i += 8 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		binary.LittleEndian.PutUint64(dst[i:], x)
	}
	*state = x
}
