package ltx

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
)

func TestNativeEmbeddedIndexCelldGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/native-snapshot.ltx")
	if err != nil {
		t.Fatal(err)
	}
	h, pages, err := DecodeNativePages(raw)
	if err != nil {
		t.Fatal(err)
	}
	n := int(binary.BigEndian.Uint64(raw[len(raw)-24:]))
	start := len(raw) - 24 - n
	locs, err := ParseNativePageIndex(raw[start:len(raw)-24], uint64(start-6), h)
	if err != nil {
		t.Fatal(err)
	}
	if len(locs) != len(pages) {
		t.Fatal("missing pages")
	}
	for i, loc := range locs {
		frame := raw[loc.Offset : loc.Offset+loc.Size]
		data, err := DecodeNativePageFrame(frame, h.PageSize, loc.PageNo)
		if err != nil || !bytes.Equal(data, pages[i].Data) {
			t.Fatalf("page %d: %v", loc.PageNo, err)
		}
		if _, err := DecodeNativePageFrame(frame, h.PageSize, loc.PageNo+1); err == nil {
			t.Fatal("wrong page identity accepted")
		}
		if _, err := DecodeNativePageFrame(frame[:len(frame)-1], h.PageSize, loc.PageNo); err == nil {
			t.Fatal("short frame accepted")
		}
	}
	for _, tc := range []struct {
		name  string
		index []byte
		end   uint64
	}{
		{"empty", nil, 100},
		{"overflow", bytes.Repeat([]byte{255}, 11), 100},
		{"outside-frame-region", []byte{1, 100, 11, 0}, 105},
		{"wrong-offset", []byte{1, 99, 11, 0}, 110},
		{"missing-terminator", []byte{1, 100, 11}, 111},
		{"trailing-data", []byte{0, 1}, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseNativePageIndex(tc.index, tc.end, h); err == nil {
				t.Fatal("malformed index accepted")
			}
		})
	}
}
