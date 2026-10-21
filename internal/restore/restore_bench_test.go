package restore

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkApplyFileStreaming(b *testing.B) {
	const ps, commit = uint32(4096), uint32(4096) // 16 MiB image, one page per write
	page := make([]byte, ps)
	for i := range page {
		page[i] = byte(i)
	}
	pages := make(map[uint32][]byte, commit)
	for i := uint32(1); i <= commit; i++ {
		pages[i] = page
	}
	seg := snapshotSegment(b, ps, commit, pages)
	dir := b.TempDir()
	b.SetBytes(int64(ps) * int64(commit))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dest := filepath.Join(dir, "bench.db")
		if _, err := ApplyFile(dest, [][]byte{seg}); err != nil {
			b.Fatal(err)
		}
		_ = os.Remove(dest)
	}
}
