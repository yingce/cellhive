package ltx

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type shortNativeWriter struct{}

func (shortNativeWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestNativeEncoderRejectsShortWrite(t *testing.T) {
	_, err := NewNativeEncoder(shortNativeWriter{}, NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("err=%v", err)
	}
}

func TestNativeEncoderSnapshotPageSequence(t *testing.T) {
	h := NativeHeader{PageSize: 512, Commit: 3, MinTXID: 1, MaxTXID: 9}
	for _, sequence := range [][]uint32{{2}, {1, 3}, {1, 1}} {
		var b bytes.Buffer
		e, err := NewNativeEncoder(&b, h)
		if err != nil {
			t.Fatal(err)
		}
		for _, pg := range sequence {
			err = e.WritePage(WALPage{PageNo: pg, Data: make([]byte, 512)})
			if err != nil {
				break
			}
		}
		if err == nil {
			t.Fatalf("accepted %v", sequence)
		}
		if e.Close() == nil {
			t.Fatal("finalized failed encoder")
		}
	}
}

func TestNativeEncoderStreamingMatchesBuffered(t *testing.T) {
	h := NativeHeader{PageSize: 512, Commit: 3, MinTXID: 1, MaxTXID: 9}
	pages := []WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{1}, 512)}, {PageNo: 2, Data: bytes.Repeat([]byte{2}, 512)}, {PageNo: 3, Data: bytes.Repeat([]byte{3}, 512)}}
	expected, err := EncodeNativePages(h, pages)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	e, err := NewNativeEncoder(&b, h)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pages {
		if err := e.WritePage(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, b.Bytes()) || e.BytesWritten() != int64(len(expected)) {
		t.Fatal("streamed output differs")
	}
	if e.Close() == nil || e.WritePage(pages[0]) == nil {
		t.Fatal("closed encoder reused")
	}
}
