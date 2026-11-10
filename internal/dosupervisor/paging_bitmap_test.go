package dosupervisor

import "testing"

func TestHydratedPagesSparseBoundariesAndDuplicateAccounting(t *testing.T) {
	var pages hydratedPages
	input := []uint32{1, 64, 65, 1024, 1025, ^uint32(0)}
	for _, page := range input {
		if pages.has(page) {
			t.Fatalf("unexpected present page %d", page)
		}
		pages.mark(page)
		pages.mark(page)
	}
	if pages.count != uint64(len(input)) {
		t.Fatalf("count=%d", pages.count)
	}
	if len(pages.blocks) != 3 {
		t.Fatalf("sparse blocks=%d", len(pages.blocks))
	}
	for _, page := range input {
		if !pages.has(page) {
			t.Fatalf("missing page %d", page)
		}
	}
	for _, page := range []uint32{0, 2, 63, 66, 1023, 1026, ^uint32(0) - 1} {
		if pages.has(page) {
			t.Fatalf("false positive page %d", page)
		}
	}
	pages.mark(0)
	if pages.count != uint64(len(input)) {
		t.Fatal("invalid page changed accounting")
	}
}

// A fully touched 10 GiB database at 4 KiB/page; both representations perform
// duplicate marks and membership checks as repeated reads would. This measures
// in-memory bookkeeping only, not SQLite, network or COS throughput.
func BenchmarkHydratedPages10GiB(b *testing.B) {
	const count = 10 << 30 / 4096
	b.Run("per-page-map", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			pages := map[uint32]bool{}
			for page := uint32(1); page <= count; page++ {
				pages[page] = true
			}
			for page := uint32(1); page <= count; page++ {
				pages[page] = true
				if !pages[page] {
					b.Fatal(page)
				}
			}
		}
	})
	b.Run("sparse-bitmap", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var pages hydratedPages
			for page := uint32(1); page <= count; page++ {
				pages.mark(page)
			}
			for page := uint32(1); page <= count; page++ {
				pages.mark(page)
				if !pages.has(page) {
					b.Fatal(page)
				}
			}
			if pages.count != count {
				b.Fatal(pages.count)
			}
		}
	})
}

// Worst sparse occupancy: one touched page per bitmap block across 10 GiB.
func BenchmarkHydratedPagesSparse10GiB(b *testing.B) {
	const count = (10 << 30) / 4096
	b.Run("per-page-map", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			pages := map[uint32]bool{}
			for page := uint32(1); page <= count; page += 1024 {
				pages[page] = true
			}
			if len(pages) != count/1024 {
				b.Fatal(len(pages))
			}
		}
	})
	b.Run("sparse-bitmap", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var pages hydratedPages
			for page := uint32(1); page <= count; page += 1024 {
				pages.mark(page)
			}
			if pages.count != count/1024 {
				b.Fatal(pages.count)
			}
		}
	})
}
