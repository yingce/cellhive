package ltx

import (
	"bytes"
	"encoding/binary"
	"hash/crc64"
	"os"
	"testing"
)

func TestNativeRoundTripAndCelldHeader(t *testing.T) {
	page := bytes.Repeat([]byte{'x'}, 4096)
	raw, err := EncodeNativePages(NativeHeader{PageSize: 4096, Commit: 1, MinTXID: 1, MaxTXID: 1}, []WALPage{{PageNo: 1, Data: page}})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) < NativeHeaderSize || !bytes.Equal(raw[:4], Magic[:]) {
		t.Fatalf("native prefix is not celld LTX: len=%d", len(raw))
	}
	if got := binaryU32(raw[8:12]); got != 4096 {
		t.Fatalf("page size=%d", got)
	}
	h, pages, err := DecodeNativePages(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !h.IsSnapshot() || h.MinTXID != 1 || h.MaxTXID != 1 {
		t.Fatalf("decoded header=%+v", h)
	}
	if err != nil || len(pages) != 1 || !bytes.Equal(pages[0].Data, page) {
		t.Fatalf("decoded page map: pages=%d err=%v", len(pages), err)
	}
}

func binaryU32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// NoChecksum skips database rolling checksums only. Every native file still
// carries a CRC64 file checksum, including the index and header.
func TestNativeFileIntegrityWithNoDatabaseChecksum(t *testing.T) {
	page := make([]byte, 4096)
	var x uint32 = 1234567
	for i := range page {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		page[i] = byte(x)
	}
	raw, err := EncodeNativePages(NativeHeader{PageSize: 4096, Commit: 1, MinTXID: 1, MaxTXID: 1}, []WALPage{{PageNo: 1, Data: page}})
	if err != nil {
		t.Fatal(err)
	}
	_, pages, err := DecodeNativePages(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err != nil || len(pages) != 1 || !bytes.Equal(pages[0].Data, page) {
		t.Fatalf("incompressible page mismatch: %v", err)
	}
	for _, off := range []int{32, len(raw) - 25, len(raw) - 1} {
		corrupt := append([]byte(nil), raw...)
		corrupt[off] ^= 1
		if _, _, err := DecodeNativePages(corrupt); err == nil {
			t.Fatalf("corruption at %d accepted", off)
		}
	}
}

func TestNativeHeaderRejectsNonNativeCoordinates(t *testing.T) {
	valid := NativeHeader{PageSize: 4096, Commit: 1, MinTXID: 2, MaxTXID: 2, Flags: nativeNoChecksum}
	for name, change := range map[string]func(*NativeHeader){
		"zero-txid":                  func(h *NativeHeader) { h.MinTXID = 0 },
		"reversed-marker":            func(h *NativeHeader) { h.MaxTXID = 1 },
		"unknown-flags":              func(h *NativeHeader) { h.Flags |= 1 },
		"invalid-page-size":          func(h *NativeHeader) { h.PageSize = 64 },
		"salt-without-offset":        func(h *NativeHeader) { h.WALSalt1 = 1 },
		"negative-offset":            func(h *NativeHeader) { h.WALOffset = -1 },
		"disabled-checksum-with-pre": func(h *NativeHeader) { h.PreApplyChecksum = nativeChecksumFlag },
	} {
		t.Run(name, func(t *testing.T) {
			h := valid
			change(&h)
			if _, err := EncodeNativePages(h, nil); err == nil {
				t.Fatal("accepted invalid native header")
			}
		})
	}
	raw, err := EncodeNativePages(valid, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := DecodeNativePages(raw); err != nil {
		t.Fatalf("native no-page continuation: %v", err)
	}
}

func TestNativeCelldGoldenFiles(t *testing.T) {
	for _, kind := range []string{"snapshot", "delta"} {
		t.Run(kind, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/native-" + kind + ".ltx")
			if err != nil {
				t.Fatal(err)
			}
			h, pages, err := DecodeNativePages(raw)
			if err != nil {
				t.Fatal(err)
			}
			commit := h.Commit
			if err != nil || commit != 2 {
				t.Fatalf("commit=%d err=%v", commit, err)
			}
			if kind == "snapshot" {
				if h.MinTXID != 1 || h.MaxTXID != 19 || len(pages) != 2 || !bytes.Equal(pages[0].Data, bytes.Repeat([]byte{23}, 4096)) {
					t.Fatalf("snapshot mismatch: %+v", h)
				}
			} else if h.MinTXID != 20 || h.MaxTXID != 21 || len(pages) != 1 {
				t.Fatalf("delta mismatch: %+v", h)
			}
			last := pages[len(pages)-1]
			if last.PageNo != 2 || !bytes.Equal(last.Data, bytes.Repeat([]byte{42}, 4096)) {
				t.Fatal("page content mismatch")
			}
		})
	}
}

// A valid CRC alone does not make a fabricated page index valid.
func TestNativeRejectsInvalidIndexWithValidChecksum(t *testing.T) {
	raw, err := EncodeNativePages(NativeHeader{PageSize: 4096, Commit: 1, MinTXID: 2, MaxTXID: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw[NativeHeaderSize+nativePageHdrSize] = 1 // Missing offset/size and terminator.
	sum := crc64.Checksum(raw[:len(raw)-8], crc64.MakeTable(crc64.ISO)) | nativeChecksumFlag
	binary.BigEndian.PutUint64(raw[len(raw)-8:], sum)
	if _, _, err := DecodeNativePages(raw); err == nil {
		t.Fatal("accepted fabricated page index")
	}
}
