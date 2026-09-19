package lz4

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	cases := map[string][]byte{
		"empty":       {},
		"one":         {0x42},
		"short":       []byte("hello world"),
		"zeros":       make([]byte, 4096),
		"repeat":      bytes.Repeat([]byte("SQLite format 3\x00 page payload "), 400),
		"sqlite-like": append([]byte("SQLite format 3\x00"), bytes.Repeat([]byte{0x0d, 0x00, 0x03}, 1300)...),
	}
	rnd := rand.New(rand.NewSource(7))
	random := make([]byte, 8192)
	rnd.Read(random)
	cases["random"] = random

	for name, src := range cases {
		enc := EncodeBlock(src)
		got, err := DecodeBlock(enc, len(src))
		if err != nil {
			t.Fatalf("%s: decode: %v", name, err)
		}
		if !bytes.Equal(got, src) {
			t.Fatalf("%s: round trip mismatch (%d bytes)", name, len(src))
		}
		if len(src) >= 16 {
			t.Logf("%s: %d -> %d bytes (%.0f%%)", name, len(src), len(enc), 100*float64(len(enc))/float64(len(src)))
		}
	}
}

// TestCompressesRepetitiveData: LZ4 must actually shrink page-like data.
func TestCompressesRepetitiveData(t *testing.T) {
	src := bytes.Repeat([]byte{0x00}, 4096)
	enc := EncodeBlock(src)
	if len(enc) >= len(src)/8 {
		t.Fatalf("all-zero page compressed to %d bytes (want < %d)", len(enc), len(src)/8)
	}
	enc = bytes.Repeat([]byte("page-0001\x00"), 409)
	if len(EncodeBlock(enc)) >= len(enc)/2 {
		t.Fatalf("repetitive page did not compress: %d -> %d", len(enc), len(EncodeBlock(enc)))
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	if _, err := DecodeBlock([]byte{0xF0}, 100); err == nil {
		t.Fatal("truncated literal run accepted")
	}
	// offset 0
	if _, err := DecodeBlock([]byte{0x40, 'a', 'b', 'c', 'd', 0, 0}, 12); err == nil {
		t.Fatal("zero offset accepted")
	}
	// output longer than expected
	src := bytes.Repeat([]byte("abcd"), 10)
	enc := EncodeBlock(src)
	if _, err := DecodeBlock(enc, 8); err == nil {
		t.Fatal("over-long output accepted")
	}
	if _, err := DecodeBlock(nil, 4); err == nil {
		t.Fatal("empty input accepted for a non-empty page")
	}
}

func BenchmarkEncodeBlock(b *testing.B) {
	page := append([]byte("SQLite format 3\x00"), bytes.Repeat([]byte{0x0d, 0x00, 0x03, 0x07}, 1022)...)
	b.SetBytes(int64(len(page)))
	for i := 0; i < b.N; i++ {
		_ = EncodeBlock(page)
	}
}

func BenchmarkDecodeBlock(b *testing.B) {
	page := append([]byte("SQLite format 3\x00"), bytes.Repeat([]byte{0x0d, 0x00, 0x03, 0x07}, 1022)...)
	enc := EncodeBlock(page)
	b.SetBytes(int64(len(page)))
	for i := 0; i < b.N; i++ {
		if _, err := DecodeBlock(enc, len(page)); err != nil {
			b.Fatal(err)
		}
	}
}
