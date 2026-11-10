package lz4

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
)

// Preserve exact encoded bytes across table reuse: immutable LTX retries depend
// on this, beyond merely decoding to the same pages.
func TestEncodeTableReusePreservesBytes(t *testing.T) {
	rng := rand.New(rand.NewSource(91))
	for _, n := range []int{0, 1, 12, 512, 4096, 65536, 131072, 4096} {
		p := make([]byte, n)
		rng.Read(p)
		for mode := 0; mode < 3; mode++ {
			if mode == 1 {
				for i := range p {
					p[i] = byte(i % 251)
				}
			}
			if mode == 2 {
				clear(p)
			}
			want := encodeBlockBeforeTableReuse(p)
			for repeat := 0; repeat < 3; repeat++ {
				if got := EncodeBlock(p); !bytes.Equal(got, want) {
					t.Fatalf("bytes changed size=%d mode=%d", n, mode)
				}
			}
		}
	}
}
func encodeBlockBeforeTableReuse(src []byte) []byte {
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
	for i+matchFindLimit <= len(src) {
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
		for i < len(src)-lastLiterals && src[cand+(i-start)] == src[i] {
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
