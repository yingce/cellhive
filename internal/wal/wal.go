// Package wal reads a SQLite write-ahead log produced by another process
// (e.g. workerd's actor SQLite) without modifying it.
//
// The reader is at the file level: it parses the WAL header and frame headers,
// yields committed frames, and detects checkpoint/truncation via salt rotation.
// It is the basis for the DO replication spike (docs/archive/p0-tasks.md P0.2).
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
)

const (
	walHeaderSize = 32
	frameHdrSize  = 24
	// magic values: the low bit selects the checksum byte order.
	magicLE = 0x377f0682
	magicBE = 0x377f0683
)

// ErrNoWAL indicates the WAL file is absent or too small to contain a header.
var ErrNoWAL = errors.New("wal: no wal file")

// Header is the 32-byte WAL header.
type Header struct {
	Magic         uint32
	Version       uint32
	PageSize      uint32
	CheckpointSeq uint32
	Salt1         uint32
	Salt2         uint32
	Checksum1     uint32
	Checksum2     uint32
}

// Frame is one committed WAL frame.
type Frame struct {
	PageNo    uint32
	DBSize    uint32 // nonzero => commit frame
	Data      []byte
	Committed bool // frame is at/before the last commit
}

// Transaction is one complete SQLite WAL transaction. Its final frame carries
// the nonzero DBSize commit marker.
type Transaction struct {
	Frames []Frame
	DBSize uint32
}

// Snapshot is the parsed committed state of a WAL file.
type Snapshot struct {
	Header      Header
	Frames      []Frame // committed frames in order
	CommitCount int     // number of commit frames
	TotalFrames int     // complete frames present in the file
}

func parseHeader(b []byte) (Header, error) {
	if len(b) < walHeaderSize {
		return Header{}, ErrNoWAL
	}
	h := Header{
		Magic:         binary.BigEndian.Uint32(b[0:4]),
		Version:       binary.BigEndian.Uint32(b[4:8]),
		PageSize:      binary.BigEndian.Uint32(b[8:12]),
		CheckpointSeq: binary.BigEndian.Uint32(b[12:16]),
		Salt1:         binary.BigEndian.Uint32(b[16:20]),
		Salt2:         binary.BigEndian.Uint32(b[20:24]),
		Checksum1:     binary.BigEndian.Uint32(b[24:28]),
		Checksum2:     binary.BigEndian.Uint32(b[28:32]),
	}
	if h.Magic != magicLE && h.Magic != magicBE {
		return Header{}, fmt.Errorf("wal: bad magic 0x%08x", h.Magic)
	}
	if h.PageSize == 0 {
		return Header{}, fmt.Errorf("wal: zero page size")
	}
	return h, nil
}

// Read parses a WAL file and returns its committed frames.
func Read(path string) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoWAL
		}
		return nil, err
	}
	if len(data) < walHeaderSize {
		return nil, ErrNoWAL
	}
	h, err := parseHeader(data[:walHeaderSize])
	if err != nil {
		return nil, err
	}
	frameSize := int(frameHdrSize) + int(h.PageSize)
	if len(data) < walHeaderSize+frameSize {
		return &Snapshot{Header: h}, nil
	}
	total := (len(data) - walHeaderSize) / frameSize
	frames := make([]Frame, 0, total)
	lastCommit := -1
	commits := 0
	for i := 0; i < total; i++ {
		off := walHeaderSize + i*frameSize
		fh := data[off : off+frameHdrSize]
		f := Frame{
			PageNo: binary.BigEndian.Uint32(fh[0:4]),
			DBSize: binary.BigEndian.Uint32(fh[4:8]),
		}
		salt1 := binary.BigEndian.Uint32(fh[8:12])
		salt2 := binary.BigEndian.Uint32(fh[12:16])
		if salt1 != h.Salt1 || salt2 != h.Salt2 {
			break // frame belongs to a different (stale) WAL generation
		}
		page := make([]byte, h.PageSize)
		copy(page, data[off+frameHdrSize:off+frameHdrSize+int(h.PageSize)])
		f.Data = page
		if f.DBSize != 0 {
			lastCommit = i
			commits++
		}
		frames = append(frames, f)
	}
	if lastCommit < 0 {
		return &Snapshot{Header: h, TotalFrames: total}, nil
	}
	committed := frames[:lastCommit+1]
	for i := range committed {
		committed[i].Committed = true
	}
	return &Snapshot{Header: h, Frames: committed, CommitCount: commits, TotalFrames: total}, nil
}

// PollResult is returned by Cursor.Poll.
type PollResult struct {
	Header       Header
	NewCommitted []Frame
	Transactions []Transaction
	Checkpoint   bool // salt rotated or file truncated since last poll
}

// Cursor tracks (salt, frame) across polls and detects checkpoints.
type Cursor struct {
	haveSalt  bool
	salt1     uint32
	salt2     uint32
	frameIdx  int // complete frames already consumed
	file      *os.File
	path      string
	bytesRead atomic.Uint64
}

// NewCursor creates an empty cursor.
func NewCursor() *Cursor { return &Cursor{} }

func (c *Cursor) reset() {
	c.frameIdx = 0
	c.salt1, c.salt2 = 0, 0
	c.haveSalt = false
}

// Reset drops the held file and clears cursor state so the next Poll starts a
// fresh baseline (no checkpoint is reported for the truncation the caller
// already handled).
func (c *Cursor) Reset() {
	c.reset()
	_ = c.Close()
}

// BytesRead reports cumulative bytes transferred from the WAL by Poll.
func (c *Cursor) BytesRead() uint64 { return c.bytesRead.Load() }

// Close releases the held WAL file descriptor.
func (c *Cursor) Close() error {
	if c.file == nil {
		return nil
	}
	err := c.file.Close()
	c.file = nil
	c.path = ""
	return err
}

func (c *Cursor) open(path string) error {
	if c.file != nil && c.path == path {
		return nil
	}
	_ = c.Close()
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	c.file = f
	c.path = path
	return nil
}

func (c *Cursor) readAt(buf []byte, off int64) error {
	n, err := c.file.ReadAt(buf, off)
	c.bytesRead.Add(uint64(n))
	if err != nil {
		return err
	}
	if n != len(buf) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// Poll reads the WAL and returns newly committed frames since the last poll.
// A checkpoint/truncation resets the cursor and sets Checkpoint=true.
func (c *Cursor) Poll(path string) (PollResult, error) {
	if err := c.open(path); err != nil {
		if os.IsNotExist(err) {
			if c.haveSalt {
				c.reset()
				return PollResult{Checkpoint: true}, nil
			}
			return PollResult{}, ErrNoWAL
		}
		return PollResult{}, err
	}
	info, err := c.file.Stat()
	if err != nil {
		return PollResult{}, err
	}
	if info.Size() < walHeaderSize {
		if c.haveSalt {
			c.reset()
			return PollResult{Checkpoint: true}, nil
		}
		return PollResult{}, ErrNoWAL
	}
	var headerBytes [walHeaderSize]byte
	if err := c.readAt(headerBytes[:], 0); err != nil {
		return PollResult{}, err
	}
	h, err := parseHeader(headerBytes[:])
	if err != nil {
		return PollResult{}, err
	}

	res := PollResult{Header: h}
	// Checkpoint detection: salt rotated, or file shrank below our cursor.
	saltChanged := c.haveSalt && (c.salt1 != h.Salt1 || c.salt2 != h.Salt2)
	frameSize := int(frameHdrSize) + int(h.PageSize)
	if saltChanged || info.Size() < int64(walHeaderSize+c.frameIdx*frameSize) {
		c.frameIdx = 0
		c.haveSalt = true
		c.salt1, c.salt2 = h.Salt1, h.Salt2
		res.Checkpoint = true
	} else {
		c.haveSalt = true
		c.salt1, c.salt2 = h.Salt1, h.Salt2
	}

	total := int((info.Size() - walHeaderSize) / int64(frameSize))
	if total <= c.frameIdx {
		return res, nil
	}
	count := total - c.frameIdx
	tail := make([]byte, count*frameSize)
	offset := int64(walHeaderSize + c.frameIdx*frameSize)
	if err := c.readAt(tail, offset); err != nil {
		// The WAL changed after Stat. Reset and let the next poll establish the
		// new generation instead of accepting a torn view.
		c.reset()
		_ = c.Close()
		return PollResult{Checkpoint: true}, nil
	}
	newFrames := make([]Frame, 0, count)
	lastCommit := -1
	for i := 0; i < count; i++ {
		off := i * frameSize
		fh := tail[off : off+frameHdrSize]
		if binary.BigEndian.Uint32(fh[8:12]) != h.Salt1 || binary.BigEndian.Uint32(fh[12:16]) != h.Salt2 {
			break
		}
		f := Frame{
			PageNo: binary.BigEndian.Uint32(fh[0:4]),
			DBSize: binary.BigEndian.Uint32(fh[4:8]),
		}
		f.Data = tail[off+frameHdrSize : off+frameHdrSize+int(h.PageSize)]
		if f.DBSize != 0 {
			lastCommit = len(newFrames)
		}
		newFrames = append(newFrames, f)
	}
	if lastCommit >= 0 {
		committed := newFrames[:lastCommit+1]
		for i := range committed {
			committed[i].Committed = true
		}
		res.NewCommitted = committed
		start := 0
		for i, frame := range committed {
			if frame.DBSize == 0 {
				continue
			}
			frames := append([]Frame(nil), committed[start:i+1]...)
			res.Transactions = append(res.Transactions, Transaction{Frames: frames, DBSize: frame.DBSize})
			start = i + 1
		}
		c.frameIdx += lastCommit + 1
	}
	return res, nil
}
