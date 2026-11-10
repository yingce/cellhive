package ltx

import (
	"bytes"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"
)

func nativeCompactInput(t *testing.T, min, max uint64, commit uint32, numbers ...uint32) []byte {
	t.Helper()
	pages := make([]WALPage, len(numbers))
	for i, pg := range numbers {
		pages[i] = WALPage{PageNo: pg, Data: bytes.Repeat([]byte{byte(max)}, 512)}
	}
	raw, err := EncodeNativePages(NativeHeader{PageSize: 512, Commit: commit, MinTXID: min, MaxTXID: max}, pages)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestNativeCompactorLatestPagesAndTruncation(t *testing.T) {
	raw := [][]byte{nativeCompactInput(t, 1, 3, 3, 1, 2, 3), nativeCompactInput(t, 4, 5, 3, 2), nativeCompactInput(t, 6, 6, 2, 1)}
	readers := make([]io.Reader, len(raw))
	for i, b := range raw {
		readers[i] = bytes.NewReader(b)
	}
	var out bytes.Buffer
	h, err := CompactNative(&out, readers)
	if err != nil {
		t.Fatal(err)
	}
	if h.MinTXID != 1 || h.MaxTXID != 6 || h.Commit != 2 {
		t.Fatalf("header=%+v", h)
	}
	_, pages, err := DecodeNativePages(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 2 || pages[0].PageNo != 1 || pages[1].PageNo != 2 || pages[0].Data[0] != 6 || pages[1].Data[0] != 5 {
		t.Fatalf("wrong merged pages: %v", pages)
	}
}

func TestNativeCompactorIncrementalRangeAndContinuation(t *testing.T) {
	var out bytes.Buffer
	h, err := CompactNative(&out, []io.Reader{bytes.NewReader(nativeCompactInput(t, 8, 8, 3)), bytes.NewReader(nativeCompactInput(t, 9, 12, 3, 2))})
	if err != nil {
		t.Fatal(err)
	}
	if h.IsSnapshot() || h.MinTXID != 8 || h.MaxTXID != 12 {
		t.Fatalf("header=%+v", h)
	}
	_, pages, err := DecodeNativePages(out.Bytes())
	if err != nil || len(pages) != 1 || pages[0].PageNo != 2 {
		t.Fatalf("pages=%v err=%v", pages, err)
	}
}

func TestNativeCompactorRejectsGapAndCorruptInput(t *testing.T) {
	base := nativeCompactInput(t, 1, 3, 1, 1)
	for _, tail := range [][]byte{nativeCompactInput(t, 5, 5, 1, 1), nativeCompactInput(t, 2, 3, 1, 1)} {
		var out bytes.Buffer
		if _, err := CompactNative(&out, []io.Reader{bytes.NewReader(base), bytes.NewReader(tail)}); err == nil {
			t.Fatal("accepted gap or non-extending input")
		}
	}
	corrupt := nativeCompactInput(t, 4, 4, 1, 1)
	corrupt[len(corrupt)-1] ^= 1
	var out bytes.Buffer
	if _, err := CompactNative(&out, []io.Reader{bytes.NewReader(base), bytes.NewReader(corrupt)}); !errors.Is(err, ErrChecksum) {
		t.Fatalf("corruption err=%v", err)
	}
	if _, _, err := DecodeNativePages(out.Bytes()); err == nil {
		t.Fatal("failed compaction finalized output")
	}
}

func TestNativeDecoderRejectsTruncationAndTrailingData(t *testing.T) {
	raw := nativeCompactInput(t, 1, 1, 1, 1)
	for _, n := range []int{0, 99, len(raw) - 1} {
		if _, _, err := DecodeNativePages(raw[:n]); err == nil {
			t.Fatalf("accepted truncation %d", n)
		}
	}
	if _, _, err := DecodeNativePages(append(raw, 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
}

func TestNativeCompactorMatchesCelldGolden(t *testing.T) {
	first := nativeCompactInput(t, 1, 3, 3, 1, 2, 3)
	last := nativeCompactInput(t, 4, 5, 2, 1)
	var out bytes.Buffer
	if _, err := CompactNative(&out, []io.Reader{bytes.NewReader(first), bytes.NewReader(last)}); err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile("testdata/native-compacted.ltx")
	if err != nil {
		t.Fatal(err)
	}
	want, wp, err := DecodeNativePages(golden)
	if err != nil {
		t.Fatal(err)
	}
	got, gp, err := DecodeNativePages(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got != want || !reflect.DeepEqual(gp, wp) {
		t.Fatal("native compaction differs from celld header/pages")
	}
}
