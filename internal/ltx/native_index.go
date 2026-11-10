package ltx

import (
	"cellhive/internal/lz4"
	"encoding/binary"
	"fmt"
)

// NativePageLocation addresses a complete frame in the native LTX object.
type NativePageLocation struct {
	PageNo       uint32
	Offset, Size uint64
}

func ParseNativeHeader(raw []byte) (NativeHeader, error) { return parseNativeHeader(raw) }

// ParseNativePageIndex validates ordered, adjacent frame locations against the
// data region ending immediately before the six-byte page terminator.
func ParseNativePageIndex(raw []byte, frameEnd uint64, h NativeHeader) ([]NativePageLocation, error) {
	var out []NativePageLocation
	offset := uint64(NativeHeaderSize)
	var prev uint64
	read := func() (uint64, error) {
		v, n := binary.Uvarint(raw)
		if n <= 0 {
			return 0, ErrShort
		}
		raw = raw[n:]
		return v, nil
	}
	for {
		pg, err := read()
		if err != nil {
			return nil, err
		}
		if pg == 0 {
			if len(raw) != 0 || offset != frameEnd {
				return nil, fmt.Errorf("ltx: invalid index boundary")
			}
			return out, nil
		}
		off, err := read()
		if err != nil {
			return nil, err
		}
		size, err := read()
		if err != nil {
			return nil, err
		}
		if pg <= prev || pg > uint64(h.Commit) || pg == uint64(0x40000000/h.PageSize+1) || off != offset || off > frameEnd || size < 10 || size > frameEnd-off || size > uint64(h.PageSize+h.PageSize/255+26) {
			return nil, fmt.Errorf("ltx: invalid page index entry")
		}
		out = append(out, NativePageLocation{uint32(pg), off, size})
		offset += size
		prev = pg
	}
}

// DecodeNativePageFrame validates the requested page identity before decoding.
// As in celld paged restore, file CRC requires a full read; a ranged fault
// validates frame structure and LZ4, with object identity pinned by the caller.
func DecodeNativePageFrame(raw []byte, pageSize, pgno uint32) ([]byte, error) {
	if len(raw) < 10 || binary.BigEndian.Uint32(raw[:4]) != pgno || binary.BigEndian.Uint16(raw[4:6]) != nativeSizeFlag || uint64(binary.BigEndian.Uint32(raw[6:10])) != uint64(len(raw)-10) {
		return nil, fmt.Errorf("ltx: invalid ranged page frame")
	}
	if len(raw)-10 > int(pageSize+pageSize/255+16) {
		return nil, fmt.Errorf("ltx: compressed page exceeds LZ4 bound")
	}
	return lz4.DecodeBlock(raw[10:], int(pageSize))
}
