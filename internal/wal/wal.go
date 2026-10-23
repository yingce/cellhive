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

// ErrChecksum indicates that the WAL header or a frame failed SQLite's
// checksum. Capture must not turn a torn/invalid frame into committed data.
var ErrChecksum = errors.New("wal: checksum mismatch")

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

// checksumBytes ports SQLite's walChecksumBytes. big selects the checksum
// word byte order, which is encoded in the low bit of the WAL magic.
func checksumBytes(b []byte, s1, s2 uint32, big bool) (uint32, uint32) {
	for len(b) >= 8 {
		var x, y uint32
		if big {
			x = binary.BigEndian.Uint32(b[0:4])
			y = binary.BigEndian.Uint32(b[4:8])
		} else {
			x = binary.LittleEndian.Uint32(b[0:4])
			y = binary.LittleEndian.Uint32(b[4:8])
		}
		s1 += x + s2
		s2 += y + s1
		b = b[8:]
	}
	return s1, s2
}

func checksumBig(h Header) bool { return h.Magic&1 == 1 }

func headerChecksum(raw []byte, h Header) (uint32, uint32) {
	return checksumBytes(raw[:walHeaderSize-8], 0, 0, checksumBig(h))
}

func frameChecksum(fh, page []byte, s1, s2 uint32, big bool) (uint32, uint32) {
	s1, s2 = checksumBytes(fh[:8], s1, s2, big)
	return checksumBytes(page, s1, s2, big)
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
	big := checksumBig(h)
	s1, s2 := headerChecksum(data[:walHeaderSize], h)
	if s1 != h.Checksum1 || s2 != h.Checksum2 {
		return nil, fmt.Errorf("%w: header", ErrChecksum)
	}
	frameSize := int(frameHdrSize) + int(h.PageSize)
	if len(data) < walHeaderSize+frameSize {
		return &Snapshot{Header: h}, nil
	}
	total := (len(data) - walHeaderSize) / frameSize
	frames := make([]Frame, 0, total)
	lastCommit := -1
	commits := 0
	var badErr error
	for i := 0; i < total; i++ {
		off := walHeaderSize + i*frameSize
		fh := data[off : off+frameHdrSize]
		salt1 := binary.BigEndian.Uint32(fh[8:12])
		salt2 := binary.BigEndian.Uint32(fh[12:16])
		if salt1 != h.Salt1 || salt2 != h.Salt2 {
			break // frame belongs to a different (stale) WAL generation
		}
		page := data[off+frameHdrSize : off+frameHdrSize+int(h.PageSize)]
		ns1, ns2 := frameChecksum(fh, page, s1, s2, big)
		if ns1 != binary.BigEndian.Uint32(fh[16:20]) || ns2 != binary.BigEndian.Uint32(fh[20:24]) {
			badErr = fmt.Errorf("%w: frame %d", ErrChecksum, i)
			break
		}
		s1, s2 = ns1, ns2
		f := Frame{
			PageNo: binary.BigEndian.Uint32(fh[0:4]),
			DBSize: binary.BigEndian.Uint32(fh[4:8]),
			Data:   append([]byte(nil), page...),
		}
		if f.DBSize != 0 {
			lastCommit = i
			commits++
		}
		frames = append(frames, f)
	}
	if lastCommit < 0 {
		return &Snapshot{Header: h, TotalFrames: total}, badErr
	}
	committed := frames[:lastCommit+1]
	for i := range committed {
		committed[i].Committed = true
	}
	snap := &Snapshot{Header: h, Frames: committed, CommitCount: commits, TotalFrames: total}
	return snap, badErr
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
	haveSalt bool
	salt1    uint32
	salt2    uint32
	// cksum1/2 is the rolling SQLite checksum of the last consumed frame
	// (initialized from the WAL header). It is only advanced through frames
	// whose checksum has been verified.
	cksum1    uint32
	cksum2    uint32
	haveCksum bool
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
	c.cksum1, c.cksum2 = 0, 0
	c.haveSalt = false
	c.haveCksum = false
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
		// SQLite may unlink and recreate the WAL on last-connection close. A
		// cached fd then points at the deleted inode and Poll would silently
		// see no transactions forever. Reopen whenever the path now names a
		// different file.
		cur, cerr := c.file.Stat()
		now, serr := os.Stat(path)
		if cerr == nil && serr == nil && os.SameFile(cur, now) {
			return nil
		}
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
	big := checksumBig(h)
	hs1, hs2 := headerChecksum(headerBytes[:], h)
	if hs1 != h.Checksum1 || hs2 != h.Checksum2 {
		return PollResult{Header: h}, fmt.Errorf("%w: header", ErrChecksum)
	}

	res := PollResult{Header: h}
	// Checkpoint detection: salt rotated, or file shrank below our cursor.
	saltChanged := c.haveSalt && (c.salt1 != h.Salt1 || c.salt2 != h.Salt2)
	frameSize := int(frameHdrSize) + int(h.PageSize)
	if saltChanged || info.Size() < int64(walHeaderSize+c.frameIdx*frameSize) {
		c.frameIdx = 0
		c.haveSalt = true
		c.salt1, c.salt2 = h.Salt1, h.Salt2
		c.cksum1, c.cksum2 = hs1, hs2
		c.haveCksum = true
		res.Checkpoint = true
	} else {
		c.haveSalt = true
		c.salt1, c.salt2 = h.Salt1, h.Salt2
		if !c.haveCksum {
			c.cksum1, c.cksum2 = hs1, hs2
			c.haveCksum = true
		}
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
	s1, s2 := c.cksum1, c.cksum2
	lastS1, lastS2 := s1, s2
	bad := false
	for i := 0; i < count; i++ {
		off := i * frameSize
		fh := tail[off : off+frameHdrSize]
		if binary.BigEndian.Uint32(fh[8:12]) != h.Salt1 || binary.BigEndian.Uint32(fh[12:16]) != h.Salt2 {
			break
		}
		page := tail[off+frameHdrSize : off+frameHdrSize+int(h.PageSize)]
		ns1, ns2 := frameChecksum(fh, page, s1, s2, big)
		if ns1 != binary.BigEndian.Uint32(fh[16:20]) || ns2 != binary.BigEndian.Uint32(fh[20:24]) {
			// A torn/invalid frame must not be accepted as a commit. Stop
			// before it; the next Poll retries once SQLite finishes the write.
			bad = true
			break
		}
		s1, s2 = ns1, ns2
		f := Frame{
			PageNo: binary.BigEndian.Uint32(fh[0:4]),
			DBSize: binary.BigEndian.Uint32(fh[4:8]),
		}
		f.Data = append([]byte(nil), page...)
		if f.DBSize != 0 {
			lastCommit = len(newFrames)
			lastS1, lastS2 = s1, s2
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
		c.cksum1, c.cksum2 = lastS1, lastS2
	}
	if bad && lastCommit < 0 {
		return res, fmt.Errorf("%w: frame %d", ErrChecksum, c.frameIdx)
	}
	return res, nil
}
