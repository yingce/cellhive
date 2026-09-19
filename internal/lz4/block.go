// Package lz4 implements the LZ4 block format (compression and decompression)
// in pure Go. It exists so LTX page frames can be compressed without adding a
// dependency (ADR-160 follow-up: celld uses pierrec/lz4 for its L1 files).
//
// Only the raw block format is implemented — no frame, no checksum — because the
// LTX segment already carries a length and CRC over its payload. Decoded sizes
// are always known by the caller (a SQLite page is exactly page_size bytes), so
// DecodeBlock enforces an exact output length instead of trusting the input.
package lz4

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Errors returned by DecodeBlock.
var (
	ErrTruncated   = errors.New("lz4: truncated input")
	ErrBadOffset   = errors.New("lz4: invalid match offset")
	ErrBadLength   = errors.New("lz4: invalid match length")
	ErrTooLarge    = errors.New("lz4: output exceeds the expected size")
	ErrShortOutput = errors.New("lz4: output shorter than the expected size")
)

const (
	minMatch = 4
	// lastLiterals bounds the literal run per the format (no match after it).
	maxOffset = 65535
)

// EncodeBlock compresses src as one LZ4 block. The result is not necessarily
// smaller; callers compare lengths and store raw frames when compression does
// not help.
func EncodeBlock(src []byte) []byte {
	// Worst case: every 255 literals cost one extra length byte, plus a token.
	dst := make([]byte, 0, len(src)+len(src)/255+16)
	if len(src) == 0 {
		return dst
	}
	const hashBits = 16
	table := make([]int, 1<<hashBits)
	for i := range table {
		table[i] = -1
	}
	hash := func(i int) uint32 {
		v := binary.LittleEndian.Uint32(src[i:])
		return (v * 2654435761) >> (32 - hashBits)
	}

	anchor := 0
	i := 0
	for i+minMatch <= len(src) {
		h := hash(i)
		cand := table[h]
		table[h] = i
		if cand < 0 || i-cand > maxOffset || cand+minMatch > len(src) ||
			binary.LittleEndian.Uint32(src[cand:]) != binary.LittleEndian.Uint32(src[i:]) {
			i++
			continue
		}
		// Extend the match.
		start := i
		i += minMatch
		for i < len(src) && src[cand+(i-start)] == src[i] {
			i++
		}
		matchLen := i - start
		// Emit the pending literals + this match.
		litLen := start - anchor
		token := byte(0)
		if litLen >= 15 {
			token = 0xF0
		} else {
			token = byte(litLen) << 4
		}
		ml := matchLen - minMatch
		if ml >= 15 {
			token |= 0x0F
		} else {
			token |= byte(ml)
		}
		dst = append(dst, token)
		if litLen >= 15 {
			dst = appendLengthBytes(dst, litLen-15)
		}
		dst = append(dst, src[anchor:start]...)
		offset := start - cand
		dst = append(dst, byte(offset), byte(offset>>8))
		if ml >= 15 {
			dst = appendLengthBytes(dst, ml-15)
		}
		anchor = i
	}
	// Final literals (the last sequence has no match).
	litLen := len(src) - anchor
	if litLen > 0 {
		token := byte(0)
		if litLen >= 15 {
			token = 0xF0
		} else {
			token = byte(litLen) << 4
		}
		dst = append(dst, token)
		if litLen >= 15 {
			dst = appendLengthBytes(dst, litLen-15)
		}
		dst = append(dst, src[anchor:]...)
	}
	return dst
}

func appendLengthBytes(dst []byte, n int) []byte {
	for n >= 255 {
		dst = append(dst, 255)
		n -= 255
	}
	return append(dst, byte(n))
}

// DecodeBlock decompresses one LZ4 block into exactly expected bytes. A block
// that decodes to a different length is an error, so a corrupt frame can never
// silently shorten a SQLite page.
func DecodeBlock(src []byte, expected int) ([]byte, error) {
	out := make([]byte, 0, expected)
	i := 0
	for i < len(src) {
		token := src[i]
		i++
		litLen := int(token >> 4)
		if litLen == 15 {
			n, err := readLength(src, &i)
			if err != nil {
				return nil, err
			}
			litLen += n
		}
		if i+litLen > len(src) {
			return nil, fmt.Errorf("%w: literals %d at %d", ErrTruncated, litLen, i)
		}
		out = append(out, src[i:i+litLen]...)
		i += litLen
		if i == len(src) {
			break // last sequence: literals only
		}
		if i+2 > len(src) {
			return nil, ErrTruncated
		}
		offset := int(src[i]) | int(src[i+1])<<8
		i += 2
		if offset == 0 || offset > len(out) {
			return nil, fmt.Errorf("%w: %d", ErrBadOffset, offset)
		}
		matchLen := int(token & 0x0F)
		if matchLen == 15 {
			n, err := readLength(src, &i)
			if err != nil {
				return nil, err
			}
			matchLen += n
		}
		matchLen += minMatch
		if len(out)+matchLen > expected {
			return nil, fmt.Errorf("%w: %d > %d", ErrTooLarge, len(out)+matchLen, expected)
		}
		// Byte-wise copy: LZ4 matches may overlap (offset < matchLen).
		for k := 0; k < matchLen; k++ {
			out = append(out, out[len(out)-offset])
		}
	}
	if len(out) != expected {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrShortOutput, len(out), expected)
	}
	return out, nil
}

func readLength(src []byte, i *int) (int, error) {
	total := 0
	for {
		if *i >= len(src) {
			return 0, ErrTruncated
		}
		b := int(src[*i])
		*i++
		total += b
		if b != 255 {
			return total, nil
		}
		if total > 1<<30 {
			return 0, ErrBadLength
		}
	}
}
