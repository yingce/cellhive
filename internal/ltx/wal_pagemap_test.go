package ltx

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func pagemapTx(pageNo uint32, data byte, dbSize uint32) WALTransaction {
	return WALTransaction{Frames: []WALFrame{{
		PageNo: pageNo, DBSize: dbSize, Data: bytes.Repeat([]byte{data}, 4096),
	}}}
}

func TestPageMapKeepsLastWritePerPage(t *testing.T) {
	txs := []WALTransaction{
		pagemapTx(5, 0x01, 5),
		pagemapTx(5, 0x02, 6),
		pagemapTx(5, 0x03, 7), // final write to page 5, commit
		pagemapTx(9, 0x04, 7),
		pagemapTx(9, 0x05, 8), // final write to page 9
	}
	commit, pages, err := PageMapFromTransactions(4096, txs)
	if err != nil {
		t.Fatalf("page map: %v", err)
	}
	if commit != 8 {
		t.Fatalf("commit = %d, want 8", commit)
	}
	if len(pages) != 2 {
		t.Fatalf("pages = %d, want 2 (deduplicated)", len(pages))
	}
	if pages[0].PageNo != 5 || !bytes.Equal(pages[0].Data, bytes.Repeat([]byte{0x03}, 4096)) {
		t.Fatalf("page 5 did not keep its last write")
	}
	if pages[1].PageNo != 9 || !bytes.Equal(pages[1].Data, bytes.Repeat([]byte{0x05}, 4096)) {
		t.Fatalf("page 9 did not keep its last write")
	}
}

func TestWALPageMapRoundTrip(t *testing.T) {
	_, pages, err := PageMapFromTransactions(4096, []WALTransaction{
		pagemapTx(3, 0xaa, 42),
	})
	if err != nil {
		t.Fatalf("page map: %v", err)
	}
	payload, err := EncodeWALPageMap(4096, 42, 1, pages)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	pageSize, commit, txCount, got, err := DecodeWALPageMap(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if pageSize != 4096 || commit != 42 || txCount != 1 {
		t.Fatalf("header = pageSize %d commit %d txCount %d", pageSize, commit, txCount)
	}
	if len(got) != 1 || got[0].PageNo != 3 || !bytes.Equal(got[0].Data, pages[0].Data) {
		t.Fatalf("round trip mismatch: %#v", got)
	}
}

func TestWALPageMapRejectsMalformed(t *testing.T) {
	payload, err := EncodeWALPageMap(4096, 1, 1, []WALPage{{PageNo: 3, Data: bytes.Repeat([]byte{1}, 4096)}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	badMagic := append([]byte(nil), payload...)
	copy(badMagic[:4], "BAD!")
	if _, _, _, _, err := DecodeWALPageMap(badMagic); err == nil {
		t.Fatalf("accepted bad magic")
	}
	if _, _, _, _, err := DecodeWALPageMap(payload[:len(payload)-1]); err == nil {
		t.Fatalf("accepted truncated payload")
	}
	if _, err := EncodeWALPageMap(4096, 1, 1, []WALPage{{PageNo: 3, Data: []byte{1, 2, 3}}}); err == nil {
		t.Fatalf("accepted wrong page length")
	}
}

// TestPageMapV2Compression: v2 payloads compress repetitive pages, keep the
// locs index accurate, and round-trip through the readers.
func TestPageMapV2Compression(t *testing.T) {
	const pageSize = 4096
	var pages []WALPage
	for p := uint32(1); p <= 16; p++ {
		data := make([]byte, pageSize)
		if p%4 == 0 {
			// A pseudo-random page: incompressible, must stay raw.
			for i := range data {
				data[i] = byte(i*31 + int(p)*7)
			}
		} else {
			copy(data, []byte("SQLite format 3\x00"))
			for i := 16; i < pageSize; i++ {
				data[i] = byte(i % 251)
			}
		}
		pages = append(pages, WALPage{PageNo: p, Data: data})
	}
	payload, err := EncodeWALPageMap(pageSize, 16, 1, pages)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) >= 16*pageSize/2 {
		t.Fatalf("v2 payload = %d bytes for %d raw (compression not effective)", len(payload), 16*pageSize)
	}
	locs, err := PageLocs(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(locs) != 16 {
		t.Fatalf("locs = %d, want 16", len(locs))
	}
	compressed := 0
	for _, l := range locs {
		if l.Codec == CodecLZ4 {
			compressed++
		}
		if l.Size != pageSize {
			t.Fatalf("loc size = %d", l.Size)
		}
	}
	if compressed == 0 {
		t.Fatal("no frame was compressed")
	}
	// Readers see identical pages.
	ps, commit, _, got, err := DecodeWALPageMap(payload)
	if err != nil || ps != pageSize || commit != 16 || len(got) != 16 {
		t.Fatalf("decode = %d/%d/%d, %v", ps, commit, len(got), err)
	}
	for i := range pages {
		if got[i].PageNo != pages[i].PageNo || !bytes.Equal(got[i].Data, pages[i].Data) {
			t.Fatalf("page %d mismatch after v2 round trip", pages[i].PageNo)
		}
	}
	one, ok, err := PageMapLookup(payload, 9)
	if err != nil || !ok || !bytes.Equal(one, pages[8].Data) {
		t.Fatalf("lookup page 9: ok=%v err=%v", ok, err)
	}
	nums, err := PageMapNumbers(payload)
	if err != nil || len(nums) != 16 || nums[14] != 15 {
		t.Fatalf("numbers = %v, %v", nums, err)
	}
	// Every frame must decode to exactly one page; a corrupt stored length fails.
	bad := append([]byte(nil), payload...)
	binary.BigEndian.PutUint32(bad[pageMapV2HeaderSize+8:], uint32(1))
	if _, _, _, _, err := DecodeWALPageMap(bad[:pageMapV2HeaderSize+pageMapV2IndexStride+2]); err == nil {
		t.Fatal("truncated v2 frame accepted")
	}
}

// TestPageMapCompressionDisabledWritesV1 pins the CELLHIVE_LTX_COMPRESSION=off
// escape hatch: the writer must emit the legacy v1 (WAL2) layout, which readers
// and PageLocs already understand.
func TestPageMapCompressionDisabledWritesV1(t *testing.T) {
	const pageSize = 256
	pages := []WALPage{
		{PageNo: 2, Data: bytes.Repeat([]byte{0x11}, pageSize)},
		{PageNo: 5, Data: bytes.Repeat([]byte{0x22}, pageSize)},
	}
	prev := CompressionEnabled
	CompressionEnabled = false
	t.Cleanup(func() { CompressionEnabled = prev })
	payload, err := EncodeWALPageMap(pageSize, 7, 2, pages)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload[:4], walPageMapMagic) {
		t.Fatalf("magic = %q, want WAL2", payload[:4])
	}
	if len(payload) != PageMapHeaderSize+2*(4+pageSize) {
		t.Fatalf("payload = %d, want v1 fixed frames", len(payload))
	}
	locs, err := PageLocs(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(locs) != 2 || locs[0].Codec != CodecRaw || locs[0].Off != PageMapHeaderSize+4 {
		t.Fatalf("locs = %+v", locs)
	}
	ps, commit, _, got, err := DecodeWALPageMap(payload)
	if err != nil || ps != pageSize || commit != 7 || len(got) != 2 {
		t.Fatalf("decode = %d/%d/%d, %v", ps, commit, len(got), err)
	}
	if got[1].PageNo != 5 || !bytes.Equal(got[1].Data, pages[1].Data) {
		t.Fatal("page 5 mismatch after v1 round trip")
	}
}

// TestPageMapV1StillReadable keeps backward compatibility for segments written
// before v2 (the v1 layout is fixed-size frames).
func TestPageMapV1StillReadable(t *testing.T) {
	const pageSize = 128
	pages := []WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{7}, pageSize)}, {PageNo: 4, Data: bytes.Repeat([]byte{9}, pageSize)}}
	// Hand-build a WAL2 payload.
	payload := make([]byte, 0, 20+2*(4+pageSize))
	payload = append(payload, []byte("WAL2")...)
	var tmp [4]byte
	for _, v := range []uint32{pageSize, 4, 1, 2} {
		binary.BigEndian.PutUint32(tmp[:], v)
		payload = append(payload, tmp[:]...)
	}
	for _, p := range pages {
		binary.BigEndian.PutUint32(tmp[:], p.PageNo)
		payload = append(payload, tmp[:]...)
		payload = append(payload, p.Data...)
	}
	ps, commit, _, got, err := DecodeWALPageMap(payload)
	if err != nil || ps != pageSize || commit != 4 || len(got) != 2 {
		t.Fatalf("v1 decode = %d/%d/%d, %v", ps, commit, len(got), err)
	}
	if got[1].PageNo != 4 || !bytes.Equal(got[1].Data, pages[1].Data) {
		t.Fatalf("v1 page mismatch: %+v", got[1])
	}
	one, ok, err := PageMapLookup(payload, 4)
	if err != nil || !ok || !bytes.Equal(one, pages[1].Data) {
		t.Fatalf("v1 lookup: ok=%v err=%v", ok, err)
	}
}

// BenchmarkFrameDecode measures the per-page decompression cost a paged read
// pays (LZ4 vs a raw frame).
func BenchmarkFrameDecode(b *testing.B) {
	const pageSize = 4096
	page := make([]byte, pageSize)
	copy(page, "SQLite format 3\x00")
	for i := 16; i < pageSize; i++ {
		page[i] = byte(i % 251)
	}
	payload, err := EncodeWALPageMap(pageSize, 1, 1, []WALPage{{PageNo: 1, Data: page}})
	if err != nil {
		b.Fatal(err)
	}
	locs, err := PageLocs(payload)
	if err != nil {
		b.Fatal(err)
	}
	if locs[0].Codec != CodecLZ4 {
		b.Fatal("page did not compress")
	}
	b.SetBytes(pageSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := FrameData(payload, locs[0]); err != nil {
			b.Fatal(err)
		}
	}
}
