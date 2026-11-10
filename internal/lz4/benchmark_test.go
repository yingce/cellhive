package lz4

import "testing"

func BenchmarkEncodeSQLitePage(b *testing.B) {
	p := make([]byte, 4096)
	for i := range p {
		p[i] = byte(i % 251)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(p)))
	for b.Loop() {
		EncodeBlock(p)
	}
}
