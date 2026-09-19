package ltx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"cellhive/internal/lz4"
)

var (
	walPageMapMagic   = []byte("WAL2") // v1: fixed-size frames
	walPageMapV2Magic = []byte("WAL3") // v2: per-frame LZ4 + offset index
)

// PageMapHeaderSize is the fixed v1 (WAL2) payload header size.
const PageMapHeaderSize = 20

// v2 (WAL3) header and index geometry.
const (
	pageMapV2HeaderSize  = 24
	pageMapV2IndexStride = 12 // pgno u32 | off u32 | stored u32 (top bit = codec)
)

// Frame codecs in a v2 index entry.
const (
	CodecRaw uint32 = 0 // frame is exactly PageSize bytes
	CodecLZ4 uint32 = 1 // frame is an LZ4 block decoding to PageSize bytes
)

// CodecBit marks an LZ4 frame inside the packed stored|codec index word; stored
// lengths are far below 2^31.
const CodecBit = uint32(1) << 31

// PageLoc locates one page's frame inside a page-map payload. For v1 payloads
// the frame carries a 4-byte page-number prefix; for v2 frames the page number
// lives only in the index, so Off/Stored point directly at the data.
type PageLoc struct {
	Pgno   uint32
	Off    int64 // offset of the frame within the payload
	Stored int   // frame size in the payload
	Size   uint32
	Codec  uint32
}

// CompressionEnabled makes EncodeWALPageMap write the compressed v2 (WAL3)
// payload. When false it writes the legacy v1 (WAL2) fixed-frame payload, which
// is useful to A/B the CPU cost of LZ4 encoding on the capture path. It is set
// once at process startup (CELLHIVE_LTX_COMPRESSION) before any writer runs and
// must not be changed concurrently. Readers always accept both formats.
var CompressionEnabled = true

// PageEntryOffset returns the byte offset, within a v1 (WAL2) payload, of the
// entry for the pos-th page (0-based, pages ascending). v2 payloads carry an
// explicit offset index instead; use PageLocs.
func PageEntryOffset(pageSize uint32, pos int) int {
	return PageMapHeaderSize + pos*(4+int(pageSize))
}

// WALPage is one deduplicated SQLite page in a page-map payload. Within a
// capture range only the final version of a page is kept, mirroring celld's
// WalReader page_map (last write per page).
type WALPage struct {
	PageNo uint32
	Data   []byte
}

// PageMapFromTransactions collapses committed WAL transactions into the final
// page image set. commit is the last nonzero commit page count, which callers
// use for the LTX commit field.
func PageMapFromTransactions(pageSize uint32, txs []WALTransaction) (uint32, []WALPage, error) {
	if pageSize == 0 {
		return 0, nil, fmt.Errorf("ltx: zero WAL page size")
	}
	if len(txs) == 0 {
		return 0, nil, fmt.Errorf("ltx: empty WAL transaction list")
	}
	index := map[uint32]int{}
	var pages []WALPage
	var commit uint32
	for ti, tx := range txs {
		if len(tx.Frames) == 0 {
			return 0, nil, fmt.Errorf("ltx: transaction %d has no frames", ti)
		}
		if tx.Frames[len(tx.Frames)-1].DBSize == 0 {
			return 0, nil, fmt.Errorf("ltx: transaction %d has no commit frame", ti)
		}
		for _, frame := range tx.Frames {
			if len(frame.Data) != int(pageSize) {
				return 0, nil, fmt.Errorf("ltx: transaction %d page %d length %d, want %d", ti, frame.PageNo, len(frame.Data), pageSize)
			}
			if pos, ok := index[frame.PageNo]; ok {
				pages[pos].Data = frame.Data
			} else {
				index[frame.PageNo] = len(pages)
				pages = append(pages, WALPage{PageNo: frame.PageNo, Data: frame.Data})
			}
			if frame.DBSize != 0 {
				commit = frame.DBSize
			}
		}
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].PageNo < pages[j].PageNo })
	return commit, pages, nil
}

// EncodeWALPageMap encodes a deduplicated page set as a v2 (WAL3) payload:
// every page frame is LZ4-compressed when that is smaller, and an explicit
// offset index lets a reader ranged-fetch and decompress a single frame without
// scanning the payload (ADR-160: L1 snapshots shrink substantially for
// repetitive SQLite pages).
//
// Layout: "WAL3" | page_size u32 | commit u32 | tx_count u32 | page_count u32 |
// reserved u32 | (pgno u32 | off u32 | stored u32 | codec u32)* | frames.
func EncodeWALPageMap(pageSize, commit uint32, txCount int, pages []WALPage) ([]byte, error) {
	if !CompressionEnabled {
		return encodeWALPageMapV1(pageSize, commit, txCount, pages)
	}
	if pageSize == 0 {
		return nil, fmt.Errorf("ltx: zero WAL page size")
	}
	if txCount <= 0 {
		return nil, fmt.Errorf("ltx: invalid transaction count %d", txCount)
	}
	indexEnd := pageMapV2HeaderSize + len(pages)*pageMapV2IndexStride
	frames := make([]byte, 0, len(pages)*int(pageSize))
	locs := make([]PageLoc, 0, len(pages))
	var seen uint32
	for i, page := range pages {
		if len(page.Data) != int(pageSize) {
			return nil, fmt.Errorf("ltx: page %d length %d, want %d", page.PageNo, len(page.Data), pageSize)
		}
		if i > 0 && page.PageNo <= seen {
			return nil, fmt.Errorf("ltx: pages must be strictly ascending by page number")
		}
		seen = page.PageNo
		frame := page.Data
		codec := CodecRaw
		if enc := lz4.EncodeBlock(page.Data); len(enc) < len(page.Data) {
			frame, codec = enc, CodecLZ4
		}
		locs = append(locs, PageLoc{
			Pgno: page.PageNo, Off: int64(indexEnd + len(frames)),
			Stored: len(frame), Size: pageSize, Codec: codec,
		})
		frames = append(frames, frame...)
	}
	buf := make([]byte, indexEnd, indexEnd+len(frames))
	copy(buf, walPageMapV2Magic)
	binary.BigEndian.PutUint32(buf[4:], pageSize)
	binary.BigEndian.PutUint32(buf[8:], commit)
	binary.BigEndian.PutUint32(buf[12:], uint32(txCount))
	binary.BigEndian.PutUint32(buf[16:], uint32(len(pages)))
	binary.BigEndian.PutUint32(buf[20:], 0)
	for i, l := range locs {
		at := pageMapV2HeaderSize + i*pageMapV2IndexStride
		stored := uint32(l.Stored)
		if l.Codec == CodecLZ4 {
			stored |= CodecBit
		}
		binary.BigEndian.PutUint32(buf[at:], l.Pgno)
		binary.BigEndian.PutUint32(buf[at+4:], uint32(l.Off))
		binary.BigEndian.PutUint32(buf[at+8:], stored)
	}
	return append(buf, frames...), nil
}

// encodeWALPageMapV1 writes the legacy WAL2 payload (fixed-size frames, no
// compression), used when CompressionEnabled is false and by older writers.
func encodeWALPageMapV1(pageSize, commit uint32, txCount int, pages []WALPage) ([]byte, error) {
	if pageSize == 0 {
		return nil, fmt.Errorf("ltx: zero WAL page size")
	}
	if txCount <= 0 {
		return nil, fmt.Errorf("ltx: invalid transaction count %d", txCount)
	}
	buf := make([]byte, 0, 20+len(pages)*(4+int(pageSize)))
	buf = append(buf, walPageMapMagic...)
	var tmp [4]byte
	for _, v := range []uint32{pageSize, commit, uint32(txCount), uint32(len(pages))} {
		binary.BigEndian.PutUint32(tmp[:], v)
		buf = append(buf, tmp[:]...)
	}
	var seen uint32
	for i, page := range pages {
		if len(page.Data) != int(pageSize) {
			return nil, fmt.Errorf("ltx: page %d length %d, want %d", page.PageNo, len(page.Data), pageSize)
		}
		if i > 0 && page.PageNo <= seen {
			return nil, fmt.Errorf("ltx: pages must be strictly ascending by page number")
		}
		seen = page.PageNo
		binary.BigEndian.PutUint32(tmp[:], page.PageNo)
		buf = append(buf, tmp[:]...)
		buf = append(buf, page.Data...)
	}
	return buf, nil
}

// pageMapDesc describes a v1 or v2 page-map payload.
type pageMapDesc struct {
	pageSize  uint32
	commit    uint32
	txCount   int
	pageCount int
	v2        bool
}

// describePageMap validates a payload header and returns its dimensions.
func describePageMap(payload []byte) (pageMapDesc, error) {
	switch {
	case len(payload) >= 20 && bytes.Equal(payload[:4], walPageMapMagic):
		d := pageMapDesc{
			pageSize:  binary.BigEndian.Uint32(payload[4:]),
			commit:    binary.BigEndian.Uint32(payload[8:]),
			txCount:   int(binary.BigEndian.Uint32(payload[12:])),
			pageCount: int(binary.BigEndian.Uint32(payload[16:])),
		}
		if d.pageSize == 0 || d.txCount <= 0 || d.pageCount < 0 {
			return d, fmt.Errorf("ltx: invalid page-map dimensions")
		}
		if need := 20 + d.pageCount*(4+int(d.pageSize)); need != len(payload) {
			return d, fmt.Errorf("ltx: page-map length %d, want %d", len(payload), need)
		}
		return d, nil
	case len(payload) >= pageMapV2HeaderSize && bytes.Equal(payload[:4], walPageMapV2Magic):
		d := pageMapDesc{
			pageSize:  binary.BigEndian.Uint32(payload[4:]),
			commit:    binary.BigEndian.Uint32(payload[8:]),
			txCount:   int(binary.BigEndian.Uint32(payload[12:])),
			pageCount: int(binary.BigEndian.Uint32(payload[16:])),
			v2:        true,
		}
		if d.pageSize == 0 || d.txCount <= 0 || d.pageCount < 0 {
			return d, fmt.Errorf("ltx: invalid page-map dimensions")
		}
		if d.pageCount > (1<<31)/pageMapV2IndexStride {
			return d, fmt.Errorf("ltx: page-map page count too large")
		}
		if indexEnd := pageMapV2HeaderSize + d.pageCount*pageMapV2IndexStride; indexEnd > len(payload) {
			return d, fmt.Errorf("ltx: truncated page-map index")
		}
		return d, nil
	default:
		return pageMapDesc{}, fmt.Errorf("ltx: invalid page-map header")
	}
}

// PageLocs returns the ordered frame locations of a payload (v1 arithmetic or
// the v2 offset index) with strict bounds validation.
func PageLocs(payload []byte) ([]PageLoc, error) {
	d, err := describePageMap(payload)
	if err != nil {
		return nil, err
	}
	locs := make([]PageLoc, 0, d.pageCount)
	var prev uint32
	if !d.v2 {
		off := PageMapHeaderSize
		for i := 0; i < d.pageCount; i++ {
			if off+4+int(d.pageSize) > len(payload) {
				return nil, fmt.Errorf("ltx: truncated page-map entry")
			}
			pgno := binary.BigEndian.Uint32(payload[off:])
			if i > 0 && pgno <= prev {
				return nil, fmt.Errorf("ltx: page numbers not ascending")
			}
			prev = pgno
			locs = append(locs, PageLoc{Pgno: pgno, Off: int64(off + 4), Stored: int(d.pageSize), Size: d.pageSize, Codec: CodecRaw})
			off += 4 + int(d.pageSize)
		}
		return locs, nil
	}
	indexEnd := pageMapV2HeaderSize + d.pageCount*pageMapV2IndexStride
	for i := 0; i < d.pageCount; i++ {
		at := pageMapV2HeaderSize + i*pageMapV2IndexStride
		pgno := binary.BigEndian.Uint32(payload[at:])
		off := int64(binary.BigEndian.Uint32(payload[at+4:]))
		word := binary.BigEndian.Uint32(payload[at+8:])
		stored := int(word &^ CodecBit)
		codec := CodecRaw
		if word&CodecBit != 0 {
			codec = CodecLZ4
		}
		if i > 0 && pgno <= prev {
			return nil, fmt.Errorf("ltx: page numbers not ascending")
		}
		prev = pgno
		if stored <= 0 || codec > CodecLZ4 {
			return nil, fmt.Errorf("ltx: invalid page frame at index %d", i)
		}
		if codec == CodecRaw && stored != int(d.pageSize) {
			return nil, fmt.Errorf("ltx: raw frame %d size %d, want %d", i, stored, d.pageSize)
		}
		if off < int64(indexEnd) || off+int64(stored) > int64(len(payload)) {
			return nil, fmt.Errorf("ltx: frame %d out of bounds", i)
		}
		locs = append(locs, PageLoc{Pgno: pgno, Off: off, Stored: stored, Size: d.pageSize, Codec: codec})
	}
	return locs, nil
}

// FrameData returns a page's bytes from a payload loc (decompressing v2 frames).
func FrameData(payload []byte, loc PageLoc) ([]byte, error) {
	if loc.Off < 0 || loc.Off+int64(loc.Stored) > int64(len(payload)) {
		return nil, fmt.Errorf("ltx: frame out of bounds")
	}
	frame := payload[loc.Off : loc.Off+int64(loc.Stored)]
	switch loc.Codec {
	case CodecRaw:
		if len(frame) != int(loc.Size) {
			return nil, fmt.Errorf("ltx: raw frame length %d, want %d", len(frame), loc.Size)
		}
		out := make([]byte, loc.Size)
		copy(out, frame)
		return out, nil
	case CodecLZ4:
		return lz4.DecodeBlock(frame, int(loc.Size))
	default:
		return nil, fmt.Errorf("ltx: unknown frame codec %d", loc.Codec)
	}
}

// DecodeWALPageMap decodes a page-map payload (v1 or v2) with validation.
func DecodeWALPageMap(payload []byte) (pageSize, commit uint32, txCount int, pages []WALPage, err error) {
	d, err := describePageMap(payload)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	locs, err := PageLocs(payload)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	pages = make([]WALPage, 0, len(locs))
	for _, loc := range locs {
		data, derr := FrameData(payload, loc)
		if derr != nil {
			return 0, 0, 0, nil, derr
		}
		pages = append(pages, WALPage{PageNo: loc.Pgno, Data: data})
	}
	return d.pageSize, d.commit, d.txCount, pages, nil
}

// PageMapNumbers scans a page-map payload and returns its page numbers in order,
// without copying or decompressing page data. It is the cheap primitive used to
// build a page index for on-demand paging.
func PageMapNumbers(payload []byte) ([]uint32, error) {
	locs, err := PageLocs(payload)
	if err != nil {
		return nil, err
	}
	out := make([]uint32, 0, len(locs))
	for _, l := range locs {
		out = append(out, l.Pgno)
	}
	return out, nil
}

// PageMapLookup returns a copy of the final version of pgno in a page-map
// payload (decompressing it when needed).
func PageMapLookup(payload []byte, pgno uint32) ([]byte, bool, error) {
	locs, err := PageLocs(payload)
	if err != nil {
		return nil, false, err
	}
	for _, loc := range locs {
		if loc.Pgno == pgno {
			data, derr := FrameData(payload, loc)
			if derr != nil {
				return nil, false, derr
			}
			return data, true, nil
		}
	}
	return nil, false, nil
}
