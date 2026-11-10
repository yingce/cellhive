// Package ltx reads and writes the native 100-byte-header LTX format.
package ltx

import "errors"

var Magic = [4]byte{'L', 'T', 'X', '1'}

// Kind is an in-memory classification, never a serialized file discriminator.
type Kind uint8

const (
	KindDelta Kind = iota
	KindSnapshot
)

var (
	// ErrShort is returned when the buffer is smaller than the header.
	ErrShort = errors.New("ltx: short segment")
	// ErrMagic is returned for a bad magic.
	ErrMagic = errors.New("ltx: bad magic")
	// ErrChecksum is returned when a native file or database checksum differs.
	ErrChecksum = errors.New("ltx: checksum mismatch")
)

// Header describes a restored range together with the epoch from its object
// path. The on-disk header is NativeHeader; it contains no epoch or kind field.
type Header struct {
	Kind      Kind
	Epoch     uint64
	StartTxID uint64
	EndTxID   uint64
}
