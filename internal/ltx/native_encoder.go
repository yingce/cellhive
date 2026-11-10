package ltx

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"

	"cellhive/internal/lz4"
)

// NativeEncoder writes a single native LTX file. Only the compact page index
// remains in memory; page images can be released after WritePage returns.
// Close finalizes the file, but does not close or sync the caller's writer.
type NativeEncoder struct {
	w        io.Writer
	header   NativeHeader
	hash     hash.Hash64
	index    []byte
	bytes    int64
	previous uint32
	closed   bool
	err      error
}

func NewNativeEncoder(w io.Writer, h NativeHeader) (*NativeEncoder, error) {
	h.Flags |= nativeNoChecksum
	if err := h.Validate(); err != nil {
		return nil, err
	}
	e := &NativeEncoder{w: w, header: h, hash: crc64.New(crc64.MakeTable(crc64.ISO))}
	if err := e.write(h.marshal(), true); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *NativeEncoder) write(b []byte, hashed bool) error {
	if e.err != nil {
		return e.err
	}
	n, err := e.w.Write(b)
	e.bytes += int64(n)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	e.err = err
	if err == nil && hashed {
		_, _ = e.hash.Write(b)
	}
	return err
}

func (e *NativeEncoder) WritePage(p WALPage) error {
	if e.err != nil {
		return e.err
	}
	if e.closed {
		return fmt.Errorf("ltx: encoder is closed")
	}
	if p.PageNo == 0 || p.PageNo <= e.previous || p.PageNo > e.header.Commit || len(p.Data) != int(e.header.PageSize) {
		e.err = fmt.Errorf("ltx: invalid page %d", p.PageNo)
		return e.err
	}
	lockPage := uint32(0x40000000)/e.header.PageSize + 1
	expected := e.previous + 1
	if expected == lockPage {
		expected++
	}
	if p.PageNo == lockPage || (e.header.IsSnapshot() && p.PageNo != expected) {
		e.err = fmt.Errorf("ltx: invalid snapshot or lock page %d", p.PageNo)
		return e.err
	}
	stored := lz4.EncodeBlock(p.Data)
	var ph [10]byte
	binary.BigEndian.PutUint32(ph[:4], p.PageNo)
	binary.BigEndian.PutUint16(ph[4:6], nativeSizeFlag)
	binary.BigEndian.PutUint32(ph[6:10], uint32(len(stored)))
	offset := e.bytes
	if err := e.write(ph[:], true); err != nil {
		return err
	}
	if err := e.write(stored, false); err != nil {
		return err
	}
	_, _ = e.hash.Write(p.Data)
	e.index = appendUvarint(e.index, uint64(p.PageNo))
	e.index = appendUvarint(e.index, uint64(offset))
	e.index = appendUvarint(e.index, uint64(e.bytes-offset))
	e.previous = p.PageNo
	return nil
}

func (e *NativeEncoder) Close() error {
	if e.err != nil {
		return e.err
	}
	if e.closed {
		return fmt.Errorf("ltx: encoder already closed")
	}
	e.closed = true
	if err := e.write(make([]byte, 6), true); err != nil {
		return err
	}
	e.index = append(e.index, 0)
	if err := e.write(e.index, true); err != nil {
		return err
	}
	var tail [16]byte
	binary.BigEndian.PutUint64(tail[:8], uint64(len(e.index)))
	// NoChecksum requires post-apply checksum zero, but preserves file CRC64.
	if err := e.write(tail[:], true); err != nil {
		return err
	}
	var sum [8]byte
	binary.BigEndian.PutUint64(sum[:], nativeChecksumFlag|e.hash.Sum64())
	return e.write(sum[:], false)
}

func (e *NativeEncoder) BytesWritten() int64 { return e.bytes }
