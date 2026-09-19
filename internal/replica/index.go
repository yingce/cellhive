package replica

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

var (
	indexMagic   = []byte("CIDX") // v1: page numbers only (fixed-size frames)
	indexMagicV2 = []byte("CID2") // v2: per-page frame locations (compressed L1)
)

// PageIndexEntry locates the final version of every page held by one compacted
// L1 object: the page size, the object's commit page count, and the ascending
// page numbers it defines. It is enough to compute a page's byte offset in the
// object (see ltx.PageEntryOffset) and fetch just that page with a ranged read.
type PageIndexEntry struct {
	Key      string
	PageSize uint32
	Commit   uint32
	Pages    []uint32
	// Locs locates each page's frame inside the L1 object (parallel to Pages).
	// Present for v2 indexes; empty for v1 indexes, where frames are fixed-size
	// and the offset is computed arithmetically.
	Locs []ltx.PageLoc
}

// PageIndex is the persisted on-demand paging index for a scope/epoch's L1
// snapshot. Deltas newer than Watermark are not folded yet and must be read
// separately.
type PageIndex struct {
	Watermark uint64
	Entries   []PageIndexEntry
}

// IndexName is the object key of the page index for a scope/epoch.
func IndexName(s cell.Scope, epoch uint64) string {
	return L1Prefix(s, epoch) + "index.bin"
}

// EncodeIndex serializes a page index.
//
// Layout (v2, used whenever an entry carries frame locations):
//
//	"CID2" | watermark u64 | entryCount u32 |
//	(keyLen u16 | key | pageSize u32 | commit u32 | pageCount u32 |
//	 (pageNo u32 | off u32 | stored u32)*)*  (stored's top bit = LZ4 codec)
//
// v1 ("CIDX") omits the per-page locations.
func EncodeIndex(idx PageIndex) ([]byte, error) {
	v2 := false
	for _, e := range idx.Entries {
		if len(e.Locs) > 0 {
			v2 = true
			break
		}
	}
	buf := make([]byte, 0, 16+len(idx.Entries)*32)
	if v2 {
		buf = append(buf, indexMagicV2...)
	} else {
		buf = append(buf, indexMagic...)
	}
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], idx.Watermark)
	buf = append(buf, tmp[:8]...)
	binary.BigEndian.PutUint32(tmp[:4], uint32(len(idx.Entries)))
	buf = append(buf, tmp[:4]...)
	for _, e := range idx.Entries {
		if len(e.Key) > 0xffff {
			return nil, fmt.Errorf("replica: index key too long")
		}
		binary.BigEndian.PutUint16(tmp[:2], uint16(len(e.Key)))
		buf = append(buf, tmp[:2]...)
		buf = append(buf, e.Key...)
		binary.BigEndian.PutUint32(tmp[:4], e.PageSize)
		buf = append(buf, tmp[:4]...)
		binary.BigEndian.PutUint32(tmp[:4], e.Commit)
		buf = append(buf, tmp[:4]...)
		binary.BigEndian.PutUint32(tmp[:4], uint32(len(e.Pages)))
		buf = append(buf, tmp[:4]...)
		prev := uint32(0)
		for i, p := range e.Pages {
			if i > 0 && p <= prev {
				return nil, fmt.Errorf("replica: index pages not ascending")
			}
			prev = p
			binary.BigEndian.PutUint32(tmp[:4], p)
			buf = append(buf, tmp[:4]...)
		}
		if len(e.Locs) > 0 {
			if len(e.Locs) != len(e.Pages) {
				return nil, fmt.Errorf("replica: index loc/page mismatch")
			}
			for _, l := range e.Locs {
				stored := uint32(l.Stored)
				if l.Codec == ltx.CodecLZ4 {
					stored |= ltx.CodecBit
				}
				for _, v := range []uint32{l.Pgno, uint32(l.Off), stored} {
					binary.BigEndian.PutUint32(tmp[:4], v)
					buf = append(buf, tmp[:4]...)
				}
			}
		}
	}
	return buf, nil
}

// DecodeIndex parses a page index (v1 or v2) with strict bounds validation.
func DecodeIndex(data []byte) (PageIndex, error) {
	if len(data) < 16 {
		return PageIndex{}, fmt.Errorf("replica: invalid index header")
	}
	v2 := bytes.Equal(data[:4], indexMagicV2)
	if !v2 && !bytes.Equal(data[:4], indexMagic) {
		return PageIndex{}, fmt.Errorf("replica: invalid index header")
	}
	idx := PageIndex{Watermark: binary.BigEndian.Uint64(data[4:12])}
	count := int(binary.BigEndian.Uint32(data[12:16]))
	off := 16
	for i := 0; i < count; i++ {
		if off+2 > len(data) {
			return PageIndex{}, fmt.Errorf("replica: truncated index entry")
		}
		keyLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if off+keyLen+12 > len(data) {
			return PageIndex{}, fmt.Errorf("replica: truncated index entry")
		}
		key := string(data[off : off+keyLen])
		off += keyLen
		pageSize := binary.BigEndian.Uint32(data[off:])
		commit := binary.BigEndian.Uint32(data[off+4:])
		pageCount := int(binary.BigEndian.Uint32(data[off+8:]))
		off += 12
		if pageSize == 0 || pageCount < 0 || off+pageCount*4 > len(data) {
			return PageIndex{}, fmt.Errorf("replica: invalid index entry dimensions")
		}
		pages := make([]uint32, pageCount)
		for j := 0; j < pageCount; j++ {
			pages[j] = binary.BigEndian.Uint32(data[off:])
			off += 4
		}
		entry := PageIndexEntry{Key: key, PageSize: pageSize, Commit: commit, Pages: pages}
		if v2 {
			if off+pageCount*12 > len(data) {
				return PageIndex{}, fmt.Errorf("replica: truncated index locations")
			}
			locs := make([]ltx.PageLoc, pageCount)
			for j := 0; j < pageCount; j++ {
				word := binary.BigEndian.Uint32(data[off+8:])
				codec := uint32(ltx.CodecRaw)
				if word&ltx.CodecBit != 0 {
					codec = ltx.CodecLZ4
				}
				locs[j] = ltx.PageLoc{
					Pgno:   binary.BigEndian.Uint32(data[off:]),
					Off:    int64(binary.BigEndian.Uint32(data[off+4:])),
					Stored: int(word &^ ltx.CodecBit),
					Size:   pageSize,
					Codec:  codec,
				}
				if locs[j].Pgno != pages[j] || locs[j].Stored <= 0 {
					return PageIndex{}, fmt.Errorf("replica: invalid index location %d", j)
				}
				off += 12
			}
			entry.Locs = locs
		}
		idx.Entries = append(idx.Entries, entry)
	}
	return idx, nil
}

// PutIndex writes the page index for a scope/epoch.
func (m *Manager) PutIndex(ctx context.Context, s cell.Scope, epoch uint64, idx PageIndex) error {
	data, err := EncodeIndex(idx)
	if err != nil {
		return err
	}
	_, err = m.B.Put(ctx, IndexName(s, epoch), data)
	return err
}

// ReadIndex returns the page index for a scope/epoch. ok is false when none was
// written (uncompacted cell).
func (m *Manager) ReadIndex(ctx context.Context, s cell.Scope, epoch uint64) (PageIndex, bool, error) {
	data, _, err := m.B.Get(ctx, IndexName(s, epoch))
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) {
			return PageIndex{}, false, nil
		}
		return PageIndex{}, false, err
	}
	idx, err := DecodeIndex(data)
	if err != nil {
		return PageIndex{}, false, err
	}
	return idx, true, nil
}
