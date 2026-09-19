// Package ltx encodes and decodes CellHive replication segments.
//
// A segment is a 44-byte header followed by an opaque payload (see
// docs/protocol-formats.md §4). The header carries the protocol version, the
// cell epoch, the transaction-id range, and a CRC32C over the payload.
package ltx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// HeaderSize is the fixed segment header size.
const HeaderSize = 44

// Magic is the segment magic ("LTX1").
var Magic = [4]byte{'L', 'T', 'X', '1'}

// Version is the current segment format version.
const Version = 1

// Kind identifies the segment type.
type Kind uint8

const (
	KindDelta    Kind = 0 // incremental transactions
	KindSnapshot Kind = 1 // full database image
	KindLink     Kind = 2 // snapshot -> delta link marker
)

// Snapshot paging flags. A snapshot larger than one segment's byte budget is
// split into ordered parts that share the snapshot's txid watermark; each part
// carries the identical full commit page count and a disjoint page range. The
// header Flags field is only interpreted for KindSnapshot: bit 15 marks a paged
// part, bits 0..14 hold the 0-based part index. A Flags value of 0 is the
// original single-segment snapshot (unpaged), so the format is backward
// compatible.
const (
	flagSnapshotPaged uint16 = 1 << 15
	snapshotPartMask  uint16 = flagSnapshotPaged - 1
)

// SnapshotFlags returns the header Flags value for part of a paged snapshot.
// part is clamped to the 15-bit part-index space.
func SnapshotFlags(part int) uint16 {
	if part < 0 {
		part = 0
	}
	return flagSnapshotPaged | uint16(part)&snapshotPartMask
}

// SnapshotPart reports whether h is a paged snapshot part and its part index.
func SnapshotPart(h Header) (part int, paged bool) {
	if h.Kind != KindSnapshot || h.Flags&flagSnapshotPaged == 0 {
		return 0, false
	}
	return int(h.Flags & snapshotPartMask), true
}

var (
	// ErrShort is returned when the buffer is smaller than the header.
	ErrShort = errors.New("ltx: short segment")
	// ErrMagic is returned for a bad magic.
	ErrMagic = errors.New("ltx: bad magic")
	// ErrChecksum is returned when the payload CRC does not match.
	ErrChecksum = errors.New("ltx: checksum mismatch")
)

// Header is the decoded segment header.
type Header struct {
	Version   uint8
	Kind      Kind
	Flags     uint16
	Epoch     uint64
	StartTxID uint64
	EndTxID   uint64
	Length    uint64
	CRC       uint32
}

// Encode builds a segment from a header and payload.
func Encode(h Header, payload []byte) []byte {
	h.Version = Version
	h.Length = uint64(len(payload))
	h.CRC = crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))
	out := make([]byte, HeaderSize+len(payload))
	copy(out[0:4], Magic[:])
	out[4] = h.Version
	out[5] = byte(h.Kind)
	binary.BigEndian.PutUint16(out[6:8], h.Flags)
	binary.BigEndian.PutUint64(out[8:16], h.Epoch)
	binary.BigEndian.PutUint64(out[16:24], h.StartTxID)
	binary.BigEndian.PutUint64(out[24:32], h.EndTxID)
	binary.BigEndian.PutUint64(out[32:40], h.Length)
	binary.BigEndian.PutUint32(out[40:44], h.CRC)
	copy(out[HeaderSize:], payload)
	return out
}

// Decode parses a segment, validating magic, length, and payload CRC.
func Decode(b []byte) (Header, []byte, error) {
	if len(b) < HeaderSize {
		return Header{}, nil, ErrShort
	}
	if string(b[0:4]) != string(Magic[:]) {
		return Header{}, nil, ErrMagic
	}
	h := Header{
		Version:   b[4],
		Kind:      Kind(b[5]),
		Flags:     binary.BigEndian.Uint16(b[6:8]),
		Epoch:     binary.BigEndian.Uint64(b[8:16]),
		StartTxID: binary.BigEndian.Uint64(b[16:24]),
		EndTxID:   binary.BigEndian.Uint64(b[24:32]),
		Length:    binary.BigEndian.Uint64(b[32:40]),
		CRC:       binary.BigEndian.Uint32(b[40:44]),
	}
	if h.Version != Version {
		return Header{}, nil, fmt.Errorf("ltx: unsupported version %d", h.Version)
	}
	if uint64(len(b)-HeaderSize) < h.Length {
		return Header{}, nil, ErrShort
	}
	payload := b[HeaderSize : HeaderSize+int(h.Length)]
	if crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)) != h.CRC {
		return Header{}, nil, ErrChecksum
	}
	return h, payload, nil
}

// ID is a content identity for a segment: two segments with the same epoch,
// kind, txid range and payload CRC are the same logical segment, regardless of
// which object (batch or single) carries them. It is used to deduplicate the
// chain when recovery re-uploads a follower's single segments alongside the
// owner's group-committed batches.
func (h Header) ID() string {
	return fmt.Sprintf("%d:%d:%d:%d:%d", h.Epoch, h.Kind, h.StartTxID, h.EndTxID, h.CRC)
}

// SegmentName returns the object key suffix for a segment. Paged snapshot parts
// get a distinct key per part so they never collide in object storage.
func SegmentName(h Header) string {
	switch h.Kind {
	case KindSnapshot:
		if part, paged := SnapshotPart(h); paged {
			return fmt.Sprintf("%d.p%d.snapshot", h.EndTxID, part)
		}
		return fmt.Sprintf("%d.snapshot", h.EndTxID)
	case KindLink:
		return fmt.Sprintf("%d.link", h.StartTxID)
	default:
		return fmt.Sprintf("%d-%d.ltx", h.StartTxID, h.EndTxID)
	}
}

// Split splits a buffer containing one or more concatenated LTX segments into
// individual segments (group-commit batches). It returns ErrShort/ErrChecksum on
// malformed input.
func Split(b []byte) ([][]byte, error) {
	var out [][]byte
	for len(b) > 0 {
		if len(b) < HeaderSize {
			return nil, ErrShort
		}
		if string(b[0:4]) != string(Magic[:]) {
			return nil, ErrMagic
		}
		length := binary.BigEndian.Uint64(b[32:40])
		total := HeaderSize + int(length)
		if len(b) < total {
			return nil, ErrShort
		}
		seg := b[:total]
		if _, _, err := Decode(seg); err != nil {
			return nil, err
		}
		out = append(out, seg)
		b = b[total:]
	}
	return out, nil
}

// BatchName returns the object key suffix for a batch of segments.
func BatchName(first, last Header) string {
	return fmt.Sprintf("%d-%d.batch", first.StartTxID, last.EndTxID)
}
