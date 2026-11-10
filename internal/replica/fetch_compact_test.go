package replica

import (
	"bytes"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"math/rand"
	"runtime"
	"testing"
	"unsafe"
)

// Exercise sparse blocks, overwrites, and repeated truncation against a simple
// reference, including page numbers near uint32's upper limit.
func TestCompactPageIndexMatchesSparseReference(t *testing.T) {
	var idx pageIndex
	want := make(map[uint32]pageLocation)
	rng := rand.New(rand.NewSource(1))
	candidates := []uint32{1, 1023, 1024, 1025, 2047, 2048, 1<<32 - 2, 1<<32 - 1}
	for step := 0; step < 2000; step++ {
		pg := candidates[rng.Intn(len(candidates))]
		if rng.Intn(4) == 0 {
			idx.truncate(pg)
			for n := range want {
				if n > pg {
					delete(want, n)
				}
			}
		} else {
			loc := pageLocation{Offset: uint64(step)*70000 + 1<<33, Size: uint32(step) + 10, object: uint32(step)}
			idx.set(pg, loc)
			want[pg] = loc
		}
		if idx.count != uint64(len(want)) {
			t.Fatalf("step %d: count=%d want=%d", step, idx.count, len(want))
		}
		for _, n := range candidates {
			got, ok := idx.get(n)
			expected, exists := want[n]
			if ok != exists || got != expected {
				t.Fatalf("step %d page %d: got %v/%v want %v/%v", step, n, got, ok, expected, exists)
			}
		}
	}
}

func TestCompactPageIndexDoesNotAllocateDeclaredImage(t *testing.T) {
	var idx pageIndex
	idx.set(1<<32-1, pageLocation{Offset: 100, Size: 10})
	if len(idx.blocks) != 1 || idx.count != 1 {
		t.Fatalf("sparse allocation: blocks=%d count=%d", len(idx.blocks), idx.count)
	}
	if unsafe.Sizeof(pageLocation{}) != 16 {
		t.Fatalf("page location grew to %d bytes", unsafe.Sizeof(pageLocation{}))
	}
	idx.truncate(0)
	if len(idx.blocks) != 0 || idx.count != 0 {
		t.Fatal("truncation retained high block")
	}
}

// Isolates retained index storage for a 10 GiB SQLite image with 4 KiB pages;
// excludes downloading/parsing native indexes and object/page payload buffers.
func BenchmarkCompactPageIndex10GiB(b *testing.B) {
	const pages = 10 << 30 / 4096
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		var idx pageIndex
		for pg := uint32(1); pg <= pages; pg++ {
			idx.set(pg, pageLocation{Offset: uint64(pg) * 4096, Size: 4106, object: pg / 100000})
		}
		if idx.count != pages {
			b.Fatal(idx.count)
		}
		b.ReportMetric(float64(len(idx.blocks)*pageIndexBlockSize)*float64(unsafe.Sizeof(pageLocation{}))/pages, "index-bytes/page")
		runtime.KeepAlive(idx)
	}
}

func TestCompactPageFetcherReadRunAcrossBlockBoundary(t *testing.T) {
	m := newManager(t)
	scope := cell.Scope{Namespace: "n", Class: "c", ID: "compact-boundary"}
	pages := make([]ltx.WALPage, 1030)
	for i := range pages {
		pages[i] = ltx.WALPage{PageNo: uint32(i + 1), Data: bytes.Repeat([]byte{byte(i)}, 512)}
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1030, MinTXID: 1, MaxTXID: 1}, pages)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.AppendL1(t.Context(), scope, 1, raw); err != nil {
		t.Fatal(err)
	}
	pf, err := m.NewPageFetcher(t.Context(), scope, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	got, err := pf.ReadRun(t.Context(), 1022, 6)
	if err != nil || len(got) != 6 {
		t.Fatalf("cross-block run: pages=%d err=%v", len(got), err)
	}
	for i, data := range got {
		if !bytes.Equal(data, pages[1021+i].Data) {
			t.Fatalf("wrong page %d", 1022+i)
		}
	}
}
