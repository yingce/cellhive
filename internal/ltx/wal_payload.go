package ltx

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var walPayloadMagic = []byte("WAL1")

// WALFrame is one SQLite WAL page frame in an LTX payload.
type WALFrame struct {
	PageNo uint32
	DBSize uint32
	Data   []byte
}

// WALTransaction preserves one committed SQLite transaction boundary.
type WALTransaction struct {
	Frames []WALFrame
}

// EncodeWALPayload encodes committed SQLite WAL transactions into a strict,
// versioned page-frame payload for an LTX segment.
func EncodeWALPayload(pageSize uint32, txs []WALTransaction) ([]byte, error) {
	if pageSize == 0 {
		return nil, fmt.Errorf("ltx: zero WAL page size")
	}
	if len(txs) == 0 {
		return nil, fmt.Errorf("ltx: empty WAL transaction list")
	}
	total := 12
	for ti, tx := range txs {
		if len(tx.Frames) == 0 {
			return nil, fmt.Errorf("ltx: transaction %d has no frames", ti)
		}
		if tx.Frames[len(tx.Frames)-1].DBSize == 0 {
			return nil, fmt.Errorf("ltx: transaction %d has no commit frame", ti)
		}
		total += 4
		for fi, frame := range tx.Frames {
			if len(frame.Data) != int(pageSize) {
				return nil, fmt.Errorf("ltx: transaction %d frame %d page length %d, want %d", ti, fi, len(frame.Data), pageSize)
			}
			total += 8 + int(pageSize)
		}
	}
	buf := make([]byte, total)
	copy(buf, walPayloadMagic)
	binary.BigEndian.PutUint32(buf[4:], pageSize)
	binary.BigEndian.PutUint32(buf[8:], uint32(len(txs)))
	off := 12
	for _, tx := range txs {
		binary.BigEndian.PutUint32(buf[off:], uint32(len(tx.Frames)))
		off += 4
		for _, frame := range tx.Frames {
			binary.BigEndian.PutUint32(buf[off:], frame.PageNo)
			binary.BigEndian.PutUint32(buf[off+4:], frame.DBSize)
			off += 8
			copy(buf[off:], frame.Data)
			off += int(pageSize)
		}
	}
	return buf, nil
}

// DecodeWALPayload decodes a WAL1 payload with strict boundary validation.
func DecodeWALPayload(payload []byte) (uint32, []WALTransaction, error) {
	if len(payload) < 12 || !bytes.Equal(payload[:4], walPayloadMagic) {
		return 0, nil, fmt.Errorf("ltx: invalid WAL payload header")
	}
	pageSize := binary.BigEndian.Uint32(payload[4:])
	txCount := int(binary.BigEndian.Uint32(payload[8:]))
	if pageSize == 0 || txCount == 0 {
		return 0, nil, fmt.Errorf("ltx: invalid WAL payload dimensions")
	}
	off := 12
	txs := make([]WALTransaction, 0, txCount)
	for ti := 0; ti < txCount; ti++ {
		if off+4 > len(payload) {
			return 0, nil, fmt.Errorf("ltx: truncated transaction %d", ti)
		}
		frameCount := int(binary.BigEndian.Uint32(payload[off:]))
		off += 4
		if frameCount == 0 || frameCount > (len(payload)-off)/(8+int(pageSize)) {
			return 0, nil, fmt.Errorf("ltx: invalid frame count for transaction %d", ti)
		}
		tx := WALTransaction{Frames: make([]WALFrame, 0, frameCount)}
		for fi := 0; fi < frameCount; fi++ {
			need := 8 + int(pageSize)
			if off+need > len(payload) {
				return 0, nil, fmt.Errorf("ltx: truncated transaction %d frame %d", ti, fi)
			}
			frame := WALFrame{
				PageNo: binary.BigEndian.Uint32(payload[off:]),
				DBSize: binary.BigEndian.Uint32(payload[off+4:]),
				Data:   append([]byte(nil), payload[off+8:off+need]...),
			}
			tx.Frames = append(tx.Frames, frame)
			off += need
		}
		if tx.Frames[len(tx.Frames)-1].DBSize == 0 {
			return 0, nil, fmt.Errorf("ltx: transaction %d has no commit frame", ti)
		}
		txs = append(txs, tx)
	}
	if off != len(payload) {
		return 0, nil, fmt.Errorf("ltx: trailing WAL payload bytes")
	}
	return pageSize, txs, nil
}
