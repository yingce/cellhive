package ltx

// This file contains the celld/Litestream v3 wire codec. The wire format is documented in
// /opt/celld/crates/ltx/reference/ltx-format.md.

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	NativeHeaderSize   = 100
	nativePageHdrSize  = 6
	nativeSizeFlag     = uint16(1)
	nativeNoChecksum   = uint32(1 << 1)
	nativeChecksumFlag = uint64(1) << 63
)

// NativeHeader is celld's 100-byte LTX header. Epoch is intentionally not
// encoded: celld scopes epochs outside the file and the object path supplies
// that context during restore.
type NativeHeader struct {
	Flags            uint32
	PageSize         uint32
	Commit           uint32
	MinTXID          uint64
	MaxTXID          uint64
	Timestamp        int64
	PreApplyChecksum uint64
	WALOffset        int64
	WALSize          int64
	WALSalt1         uint32
	WALSalt2         uint32
	NodeID           uint64
}

func (h NativeHeader) IsSnapshot() bool { return h.MinTXID == 1 }

// Validate enforces the native header invariants before any page allocation.
func (h NativeHeader) Validate() error {
	if h.Flags & ^nativeNoChecksum != 0 {
		return fmt.Errorf("ltx: invalid header flags")
	}
	if h.PageSize < 512 || h.PageSize > 65536 || h.PageSize&(h.PageSize-1) != 0 {
		return fmt.Errorf("ltx: invalid page size")
	}
	if h.MinTXID == 0 || h.MaxTXID < h.MinTXID {
		return fmt.Errorf("ltx: invalid transaction range")
	}
	if h.WALOffset < 0 || h.WALSize < 0 || (h.WALOffset == 0 && (h.WALSize != 0 || h.WALSalt1 != 0 || h.WALSalt2 != 0)) {
		return fmt.Errorf("ltx: invalid WAL coordinate")
	}
	if h.IsSnapshot() || h.Flags&nativeNoChecksum != 0 {
		if h.PreApplyChecksum != 0 {
			return fmt.Errorf("ltx: unexpected pre-apply checksum")
		}
	} else if h.PreApplyChecksum&nativeChecksumFlag == 0 {
		return fmt.Errorf("ltx: missing pre-apply checksum")
	}
	return nil
}

// EncodeNativePages writes SQLite pages directly as one native LTX file.
// Page size and final database size belong to the header, not a private payload.
func EncodeNativePages(h NativeHeader, pages []WALPage) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := NewNativeEncoder(&buf, h)
	if err != nil {
		return nil, err
	}
	for _, p := range pages {
		if err := enc.WritePage(p); err != nil {
			return nil, err
		}
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (h NativeHeader) marshal() []byte {
	out := make([]byte, NativeHeaderSize)
	copy(out[:4], Magic[:])
	binary.BigEndian.PutUint32(out[4:8], h.Flags)
	binary.BigEndian.PutUint32(out[8:12], h.PageSize)
	binary.BigEndian.PutUint32(out[12:16], h.Commit)
	binary.BigEndian.PutUint64(out[16:24], h.MinTXID)
	binary.BigEndian.PutUint64(out[24:32], h.MaxTXID)
	binary.BigEndian.PutUint64(out[32:40], uint64(h.Timestamp))
	binary.BigEndian.PutUint64(out[40:48], h.PreApplyChecksum)
	binary.BigEndian.PutUint64(out[48:56], uint64(h.WALOffset))
	binary.BigEndian.PutUint64(out[56:64], uint64(h.WALSize))
	binary.BigEndian.PutUint32(out[64:68], h.WALSalt1)
	binary.BigEndian.PutUint32(out[68:72], h.WALSalt2)
	binary.BigEndian.PutUint64(out[72:80], h.NodeID)

	return out
}

func appendUvarint(dst []byte, v uint64) []byte {
	var b [10]byte
	n := binary.PutUvarint(b[:], v)
	return append(dst, b[:n]...)
}
