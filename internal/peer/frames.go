package peer

import "encoding/binary"

// EncodeFrames serializes segments as [u32 big-endian length][bytes] records.
// Used for the batched peer-append wire body and the spool append-log.
func EncodeFrames(segments [][]byte) []byte {
	total := 0
	for _, s := range segments {
		total += 4 + len(s)
	}
	buf := make([]byte, 0, total)
	var hdr [4]byte
	for _, s := range segments {
		binary.BigEndian.PutUint32(hdr[:], uint32(len(s)))
		buf = append(buf, hdr[:]...)
		buf = append(buf, s...)
	}
	return buf
}

// DecodeFrames parses [u32 len][bytes] records, stopping at a torn tail so a
// crash-truncated batch is read best-effort rather than erroring.
func DecodeFrames(data []byte) [][]byte {
	var out [][]byte
	for i := 0; i+4 <= len(data); {
		n := int(binary.BigEndian.Uint32(data[i:]))
		i += 4
		if n < 0 || i+n > len(data) {
			break
		}
		seg := make([]byte, n)
		copy(seg, data[i:i+n])
		out = append(out, seg)
		i += n
	}
	return out
}
