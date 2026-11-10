package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// ErrOverlayChanged means the WAL generation or a captured frame changed.
var ErrOverlayChanged = errors.New("wal: overlay source changed")

type overlayFrame struct {
	offset         int64
	prior1, prior2 uint32
	header         [frameHdrSize]byte
}

// CommittedOverlay holds frame references, never page payloads. It is a
// captured WAL prefix, not a SQLite read transaction: callers must serialize
// checkpoint/reset and validate their main database view before publishing.
// Appends after the initial file-size horizon are deliberately excluded.
// Methods must not race with Close.
type CommittedOverlay struct {
	PageSize  uint32
	DBSize    uint32 // zero means no committed transaction in this WAL
	file      *os.File
	path      string
	info      os.FileInfo
	rawHeader [walHeaderSize]byte
	header    Header
	pages     map[uint32]overlayFrame
}

// OpenCommittedOverlay scans with one frame buffer and O(distinct pages)
// references (including the pending transaction). Missing/empty WALs return
// an empty overlay whose absence/identity can still be checked by Validate.
func OpenCommittedOverlay(path string) (*CommittedOverlay, error) {
	return OpenCommittedOverlayContext(context.Background(), path)
}

// OpenStableCommittedOverlay retries an observation that intersects a writer's
// append/reset. Every attempt has a fresh file-size horizon and full checksum
// validation. Persistent corruption still fails; no partial prefix is accepted
// merely because retrying ran out of time.
func OpenStableCommittedOverlay(ctx context.Context, path string) (*CommittedOverlay, error) {
	for attempt := 0; ; attempt++ {
		o, err := OpenCommittedOverlayContext(ctx, path)
		if err == nil || attempt == 5 || (!errors.Is(err, io.ErrUnexpectedEOF) &&
			!errors.Is(err, ErrChecksum) && !errors.Is(err, ErrOverlayChanged)) {
			return o, err
		}
		timer := time.NewTimer(time.Millisecond * time.Duration(2<<attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func OpenCommittedOverlayContext(ctx context.Context, path string) (_ *CommittedOverlay, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o := &CommittedOverlay{path: path, pages: make(map[uint32]overlayFrame)}
	o.file, err = os.Open(path)
	if os.IsNotExist(err) {
		return o, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = o.Close()
		}
	}()
	o.info, err = o.file.Stat()
	if err != nil {
		return nil, err
	}
	if o.info.Size() == 0 {
		return o, o.Validate()
	}
	if o.info.Size() < walHeaderSize {
		return nil, fmt.Errorf("wal: incomplete overlay header: %w", io.ErrUnexpectedEOF)
	}
	if _, err = o.file.ReadAt(o.rawHeader[:], 0); err != nil {
		return nil, err
	}
	o.header, err = parseHeader(o.rawHeader[:])
	if err != nil {
		return nil, err
	}
	h := o.header
	if h.Version != 3007000 || h.PageSize < 512 || h.PageSize > 65536 || h.PageSize&(h.PageSize-1) != 0 {
		return nil, fmt.Errorf("wal: invalid overlay version/page size: %d/%d", h.Version, h.PageSize)
	}
	s1, s2 := headerChecksum(o.rawHeader[:], h)
	if s1 != h.Checksum1 || s2 != h.Checksum2 {
		return nil, fmt.Errorf("%w: overlay header", ErrChecksum)
	}
	o.PageSize = h.PageSize
	frameSize := int64(frameHdrSize) + int64(h.PageSize)
	buf := make([]byte, int(frameSize))
	pending := make(map[uint32]overlayFrame)
	for off := int64(walHeaderSize); off < o.info.Size(); off += frameSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remaining := o.info.Size() - off
		if remaining < frameSize {
			// An incomplete current-generation frame cannot be proved uncommitted.
			return nil, fmt.Errorf("wal: incomplete overlay frame: %w", io.ErrUnexpectedEOF)
		}
		if _, err = o.file.ReadAt(buf, off); err != nil {
			return nil, err
		}
		fh := buf[:frameHdrSize]
		if binary.BigEndian.Uint32(fh[8:12]) != h.Salt1 || binary.BigEndian.Uint32(fh[12:16]) != h.Salt2 {
			break
		} // stale generation tail
		pg := binary.BigEndian.Uint32(fh[:4])
		if pg == 0 {
			return nil, fmt.Errorf("wal: zero overlay page number")
		}
		ref := overlayFrame{offset: off, prior1: s1, prior2: s2}
		copy(ref.header[:], fh)
		s1, s2 = frameChecksum(fh, buf[frameHdrSize:], s1, s2, checksumBig(h))
		if s1 != binary.BigEndian.Uint32(fh[16:20]) || s2 != binary.BigEndian.Uint32(fh[20:24]) {
			return nil, fmt.Errorf("%w: overlay frame at %d", ErrChecksum, off)
		}
		pending[pg] = ref
		if size := binary.BigEndian.Uint32(fh[4:8]); size != 0 {
			for pg, ref := range pending {
				if pg <= size {
					o.pages[pg] = ref
				}
			}
			// clear traverses the retained map directory. Do not make thousands
			// of later single-page commits repeatedly clear a huge transaction's
			// allocation while a second copy remains in the committed map.
			if len(pending) > 1024 {
				pending = make(map[uint32]overlayFrame)
			} else {
				clear(pending)
			}
			// A later growth must never resurrect pages removed by a shrink.
			if size < o.DBSize {
				if uint64(o.DBSize-size) < uint64(len(o.pages)) {
					for pg := uint64(size) + 1; pg <= uint64(o.DBSize); pg++ {
						delete(o.pages, uint32(pg))
					}
				} else {
					for pg := range o.pages {
						if pg > size {
							delete(o.pages, pg)
						}
					}
				}
			}
			o.DBSize = size
		}
	}
	if err = o.Validate(); err != nil {
		return nil, err
	}
	return o, nil
}

// ReadPage reads a captured committed page into dst, which must be exactly
// PageSize bytes. It rechecks the original frame header and rolling checksum;
// same-offset payload replacement cannot silently enter the snapshot.
func (o *CommittedOverlay) ReadPage(pg uint32, dst []byte) (bool, error) {
	ref, ok := o.pages[pg]
	if !ok {
		return false, nil
	}
	if len(dst) != int(o.PageSize) {
		return false, fmt.Errorf("wal: overlay destination size %d, want %d", len(dst), o.PageSize)
	}
	if o.file == nil {
		return false, ErrOverlayChanged
	}
	var fh [frameHdrSize]byte
	if _, err := o.file.ReadAt(fh[:], ref.offset); err != nil {
		return false, fmt.Errorf("%w: %v", ErrOverlayChanged, err)
	}
	if fh != ref.header {
		return false, ErrOverlayChanged
	}
	if _, err := o.file.ReadAt(dst, ref.offset+frameHdrSize); err != nil {
		return false, fmt.Errorf("%w: %v", ErrOverlayChanged, err)
	}
	s1, s2 := frameChecksum(fh[:], dst, ref.prior1, ref.prior2, checksumBig(o.header))
	if s1 != binary.BigEndian.Uint32(ref.header[16:20]) || s2 != binary.BigEndian.Uint32(ref.header[20:24]) {
		return false, fmt.Errorf("%w: %w", ErrOverlayChanged, ErrChecksum)
	}
	return true, nil
}

// Validate must succeed after encoding and before accepting the snapshot.
// It detects unlink/replacement, truncation, and generation/header changes.
// It does not lock SQLite or prove the main database stayed unchanged.
func (o *CommittedOverlay) Validate() error {
	now, err := os.Stat(o.path)
	if o.info == nil {
		if os.IsNotExist(err) {
			return nil
		}
		return ErrOverlayChanged
	}
	if err != nil || o.file == nil || !os.SameFile(o.info, now) || now.Size() < o.info.Size() {
		return ErrOverlayChanged
	}
	if o.info.Size() == 0 {
		if now.Size() != 0 {
			return ErrOverlayChanged
		}
		return nil
	}
	var raw [walHeaderSize]byte
	if _, err = o.file.ReadAt(raw[:], 0); err != nil {
		return fmt.Errorf("%w: %v", ErrOverlayChanged, err)
	}
	if !bytes.Equal(raw[:], o.rawHeader[:]) {
		return ErrOverlayChanged
	}
	return nil
}

// Close releases the retained descriptor.
func (o *CommittedOverlay) Close() error {
	if o.file == nil {
		return nil
	}
	err := o.file.Close()
	o.file = nil
	return err
}
