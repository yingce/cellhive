package ltx

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	h := Header{Kind: KindDelta, Epoch: 3, StartTxID: 10, EndTxID: 12}
	payload := []byte("hello-ltx-payload")
	seg := Encode(h, payload)

	got, gotPayload, err := Decode(seg)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Epoch != 3 || got.StartTxID != 10 || got.EndTxID != 12 || got.Kind != KindDelta {
		t.Fatalf("header mismatch: %+v", got)
	}
	if string(gotPayload) != string(payload) {
		t.Fatalf("payload mismatch: %q", gotPayload)
	}
	if got.Version != Version {
		t.Fatalf("version = %d, want %d", got.Version, Version)
	}
}

func TestDecodeRejectsBadMagic(t *testing.T) {
	seg := Encode(Header{Kind: KindDelta}, []byte("x"))
	seg[0] = 'X'
	if _, _, err := Decode(seg); !errors.Is(err, ErrMagic) {
		t.Fatalf("bad magic: got %v, want ErrMagic", err)
	}
}

func TestDecodeRejectsCorruption(t *testing.T) {
	seg := Encode(Header{Kind: KindDelta}, []byte("payload"))
	seg[HeaderSize] ^= 0xff
	if _, _, err := Decode(seg); !errors.Is(err, ErrChecksum) {
		t.Fatalf("corruption: got %v, want ErrChecksum", err)
	}
}

func TestDecodeRejectsShort(t *testing.T) {
	if _, _, err := Decode([]byte("short")); !errors.Is(err, ErrShort) {
		t.Fatalf("short: got %v, want ErrShort", err)
	}
}

func TestSplitRejectsOverflowingLength(t *testing.T) {
	for _, length := range []uint64{1 << 63, math.MaxUint64, math.MaxUint64 - 43} {
		seg := make([]byte, HeaderSize)
		copy(seg[0:4], Magic[:])
		seg[4] = Version
		binary.BigEndian.PutUint64(seg[32:40], length)
		if _, err := Split(seg); !errors.Is(err, ErrShort) {
			t.Fatalf("length %d: got %v, want ErrShort", length, err)
		}
	}
}

func TestSplitConcatenated(t *testing.T) {
	a := Encode(Header{Kind: KindDelta, StartTxID: 1, EndTxID: 1}, []byte("a"))
	b := Encode(Header{Kind: KindDelta, StartTxID: 2, EndTxID: 2}, []byte("bb"))
	segs, err := Split(append(append([]byte{}, a...), b...))
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if len(segs) != 2 || len(segs[0]) != len(a) || len(segs[1]) != len(b) {
		t.Fatalf("split segments = %d", len(segs))
	}
}

func TestSegmentName(t *testing.T) {
	if got := SegmentName(Header{Kind: KindDelta, StartTxID: 1, EndTxID: 5}); got != "1-5.ltx" {
		t.Fatalf("delta name = %q", got)
	}
	if got := SegmentName(Header{Kind: KindSnapshot, EndTxID: 9}); got != "9.snapshot" {
		t.Fatalf("snapshot name = %q", got)
	}
	if got := SegmentName(Header{Kind: KindLink, StartTxID: 7}); got != "7.link" {
		t.Fatalf("link name = %q", got)
	}
}
