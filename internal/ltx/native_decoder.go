package ltx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc64"
	"io"

	"cellhive/internal/lz4"
)

// NativeDecoder reads one native LTX file without retaining database pages.
// EOF is returned only after index, trailer and file checksum verification.
type NativeDecoder struct {
	r        io.Reader
	Header   NativeHeader
	hash     hash.Hash64
	index    []byte
	offset   uint64
	previous uint32
	rolling  uint64
	done     bool
	err      error
}

func parseNativeHeader(raw []byte) (NativeHeader, error) {
	if len(raw) < NativeHeaderSize {
		return NativeHeader{}, ErrShort
	}
	if !bytes.Equal(raw[:4], Magic[:]) {
		return NativeHeader{}, ErrMagic
	}
	h := NativeHeader{
		Flags: binary.BigEndian.Uint32(raw[4:8]), PageSize: binary.BigEndian.Uint32(raw[8:12]), Commit: binary.BigEndian.Uint32(raw[12:16]),
		MinTXID: binary.BigEndian.Uint64(raw[16:24]), MaxTXID: binary.BigEndian.Uint64(raw[24:32]), Timestamp: int64(binary.BigEndian.Uint64(raw[32:40])),
		PreApplyChecksum: binary.BigEndian.Uint64(raw[40:48]), WALOffset: int64(binary.BigEndian.Uint64(raw[48:56])), WALSize: int64(binary.BigEndian.Uint64(raw[56:64])),
		WALSalt1: binary.BigEndian.Uint32(raw[64:68]), WALSalt2: binary.BigEndian.Uint32(raw[68:72]), NodeID: binary.BigEndian.Uint64(raw[72:80]),
	}
	return h, h.Validate()
}

func NewNativeDecoder(r io.Reader) (*NativeDecoder, error) {
	var raw [NativeHeaderSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return nil, err
	}
	h, err := parseNativeHeader(raw[:])
	if err != nil {
		return nil, err
	}
	d := &NativeDecoder{r: r, Header: h, hash: crc64.New(crc64.MakeTable(crc64.ISO)), offset: NativeHeaderSize, rolling: nativeChecksumFlag}
	d.hash.Write(raw[:])
	return d, nil
}

func (d *NativeDecoder) ReadPage() (p WALPage, err error) {
	if d.err != nil {
		return WALPage{}, d.err
	}
	if d.done {
		return WALPage{}, io.EOF
	}
	defer func() {
		if err == io.EOF && !d.done {
			err = io.ErrUnexpectedEOF
		}
		if err != nil && err != io.EOF {
			d.err = err
		}
	}()
	var ph [10]byte
	if _, err = io.ReadFull(d.r, ph[:6]); err != nil {
		return WALPage{}, err
	}
	d.hash.Write(ph[:6])
	pg := binary.BigEndian.Uint32(ph[:4])
	flags := binary.BigEndian.Uint16(ph[4:6])
	if pg == 0 {
		if flags != 0 {
			return WALPage{}, fmt.Errorf("ltx: invalid page terminator")
		}
		if err = d.finish(); err != nil {
			return WALPage{}, err
		}
		d.done = true
		return WALPage{}, io.EOF
	}
	if flags != nativeSizeFlag || pg <= d.previous || pg > d.Header.Commit || pg == uint32(0x40000000)/d.Header.PageSize+1 {
		return WALPage{}, fmt.Errorf("ltx: invalid native page header")
	}
	if _, err = io.ReadFull(d.r, ph[6:]); err != nil {
		return WALPage{}, err
	}
	d.hash.Write(ph[6:])
	size := binary.BigEndian.Uint32(ph[6:])
	if size > d.Header.PageSize+d.Header.PageSize/255+16 {
		return WALPage{}, fmt.Errorf("ltx: compressed page exceeds LZ4 bound")
	}
	compressed := make([]byte, size)
	if _, err = io.ReadFull(d.r, compressed); err != nil {
		return WALPage{}, err
	}
	data, err := lz4.DecodeBlock(compressed, int(d.Header.PageSize))
	if err != nil {
		return WALPage{}, err
	}
	d.hash.Write(data)
	if d.Header.IsSnapshot() && d.Header.Flags&nativeNoChecksum == 0 {
		h := crc64.New(crc64.MakeTable(crc64.ISO))
		h.Write(ph[:4])
		h.Write(data)
		d.rolling = nativeChecksumFlag | (d.rolling ^ h.Sum64())
	}
	d.index = appendUvarint(d.index, uint64(pg))
	d.index = appendUvarint(d.index, d.offset)
	d.index = appendUvarint(d.index, uint64(10+size))
	d.offset += uint64(10 + size)
	d.previous = pg
	return WALPage{PageNo: pg, Data: data}, nil
}

func (d *NativeDecoder) finish() error {
	d.index = append(d.index, 0)
	// Compare and hash the embedded index in bounded chunks instead of
	// allocating a second full copy of a large cell's index and trailer.
	indexLen := len(d.index)
	chunk := make([]byte, min(64<<10, indexLen))
	for offset := 0; offset < indexLen; {
		n := min(len(chunk), indexLen-offset)
		if _, err := io.ReadFull(d.r, chunk[:n]); err != nil {
			return err
		}
		if !bytes.Equal(chunk[:n], d.index[offset:offset+n]) {
			return fmt.Errorf("ltx: page index does not match frames")
		}
		d.hash.Write(chunk[:n])
		offset += n
	}
	d.index = nil
	var tail [24]byte
	if _, err := io.ReadFull(d.r, tail[:]); err != nil {
		return err
	}
	if binary.BigEndian.Uint64(tail[:8]) != uint64(indexLen) {
		return fmt.Errorf("ltx: page index length mismatch")
	}
	post := binary.BigEndian.Uint64(tail[8:16])
	if d.Header.Flags&nativeNoChecksum != 0 {
		if post != 0 {
			return fmt.Errorf("ltx: unexpected post-apply checksum")
		}
	} else if post&nativeChecksumFlag == 0 || (d.Header.IsSnapshot() && post != d.rolling) {
		return ErrChecksum
	}
	d.hash.Write(tail[:16])
	if nativeChecksumFlag|d.hash.Sum64() != binary.BigEndian.Uint64(tail[16:]) {
		return ErrChecksum
	}
	var extra [1]byte
	n, err := io.ReadFull(d.r, extra[:])
	if n != 0 || err != io.EOF {
		return fmt.Errorf("ltx: trailing data or incomplete file boundary: %w", err)
	}
	return nil
}

func DecodeNativePages(raw []byte) (NativeHeader, []WALPage, error) {
	d, err := NewNativeDecoder(bytes.NewReader(raw))
	if err != nil {
		return NativeHeader{}, nil, err
	}
	var pages []WALPage
	for {
		p, err := d.ReadPage()
		if err == io.EOF {
			return d.Header, pages, nil
		}
		if err != nil {
			return NativeHeader{}, nil, err
		}
		pages = append(pages, p)
	}
}

// ValidateNative verifies framing, embedded index and file checksum without
// retaining decoded pages or converting to any intermediate payload format.
func ValidateNative(raw []byte) (NativeHeader, error) {
	d, err := NewNativeDecoder(bytes.NewReader(raw))
	if err != nil {
		return NativeHeader{}, err
	}
	for {
		_, err := d.ReadPage()
		if err == io.EOF {
			return d.Header, nil
		}
		if err != nil {
			return NativeHeader{}, err
		}
	}
}
