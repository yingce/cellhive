package restore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"cellhive/internal/ltx"
)

// PageIndex is a lazily-resolvable view of a segment chain. It records, for every
// page 1..Commit, which chain part holds that page's final version, without
// materializing page data. It is the on-demand paging primitive: a caller can
// fetch one page (or a range) and write it into a sparse file instead of loading
// the whole database.
type PageIndex struct {
	PageSize  uint32
	Commit    uint32
	Watermark uint64
	refs      map[uint32]pageRef
	objects   [][]byte
	timestamp int64
}

type pageRef struct {
	object       int
	offset, size uint64
}

// IndexChain validates native files and builds locations into their original
// compressed frames. No private WAL map, re-encoding or decoded database copy
// is retained. Epochs have already been resolved by the replica planner.
func IndexChain(segments [][]byte) (*PageIndex, error) {
	headers := make([]ltx.NativeHeader, len(segments))
	for i, raw := range segments {
		h, err := ltx.ParseNativeHeader(raw)
		if err != nil {
			return nil, fmt.Errorf("restore: native header %d: %w", i, err)
		}
		headers[i] = h
	}
	order, watermark, err := selectNativeChain(headers)
	if err != nil {
		return nil, err
	}
	ix := &PageIndex{Watermark: watermark, refs: make(map[uint32]pageRef)}
	for _, i := range order {
		raw := segments[i]
		d, err := ltx.NewNativeDecoder(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		if ix.PageSize == 0 {
			ix.PageSize = d.Header.PageSize
		} else if ix.PageSize != d.Header.PageSize {
			return nil, fmt.Errorf("restore: page size mismatch")
		}
		for {
			_, err := d.ReadPage()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
		}
		// The streaming decoder has checked this exact tail against every frame.
		size := binary.BigEndian.Uint64(raw[len(raw)-24:])
		start := uint64(len(raw)) - 24 - size
		locations, err := ltx.ParseNativePageIndex(raw[start:uint64(len(raw))-24], start-6, d.Header)
		if err != nil {
			return nil, err
		}
		if d.Header.Commit < ix.Commit {
			for pg := range ix.refs {
				if pg > d.Header.Commit {
					delete(ix.refs, pg)
				}
			}
		}
		ix.Commit = d.Header.Commit
		ix.timestamp = d.Header.Timestamp
		object := len(ix.objects)
		ix.objects = append(ix.objects, raw)
		for _, loc := range locations {
			ix.refs[loc.PageNo] = pageRef{object, loc.Offset, loc.Size}
		}
	}
	if ix.PageSize == 0 {
		return nil, fmt.Errorf("restore: no native snapshot")
	}
	expected := uint64(ix.Commit)
	if uint64(0x40000000)/uint64(ix.PageSize)+1 <= uint64(ix.Commit) {
		expected--
	}
	if uint64(len(ix.refs)) != expected {
		return nil, fmt.Errorf("restore: incomplete page coverage: have %d want %d", len(ix.refs), expected)
	}
	return ix, nil
}

func (ix *PageIndex) Has(pgno uint32) bool {
	if pgno == 0 || pgno > ix.Commit {
		return false
	}
	if pgno == 0x40000000/ix.PageSize+1 {
		return true
	}
	_, ok := ix.refs[pgno]
	return ok
}

func (ix *PageIndex) Page(pgno uint32) ([]byte, error) {
	if pgno > 0 && pgno <= ix.Commit && pgno == 0x40000000/ix.PageSize+1 {
		return make([]byte, ix.PageSize), nil
	}
	ref, ok := ix.refs[pgno]
	if !ok {
		return nil, fmt.Errorf("restore: page %d not defined", pgno)
	}
	return ltx.DecodeNativePageFrame(ix.objects[ref.object][ref.offset:ref.offset+ref.size], ix.PageSize, pgno)
}

// Ranges returns the contiguous runs of defined pages, as [start, end] pairs.
func (ix *PageIndex) Ranges() [][2]uint32 {
	var out [][2]uint32
	start := uint32(0)
	prev := uint32(0)
	for pgno := uint32(1); pgno <= ix.Commit; pgno++ {
		if !ix.Has(pgno) {
			if start != 0 {
				out = append(out, [2]uint32{start, prev})
				start = 0
			}
			continue
		}
		if start == 0 {
			start = pgno
		}
		prev = pgno
	}
	if start != 0 {
		out = append(out, [2]uint32{start, prev})
	}
	return out
}

// SparseFile writes a SQLite database image of the full page count to dest,
// filling only the given page ranges and leaving the rest as holes (a sparse
// file). It returns how many pages were written.
func (ix *PageIndex) SparseFile(dest string, ranges [][2]uint32) (int, error) {
	return SparseFileFunc(dest, ix.PageSize, ix.Commit, ix.Page, ranges)
}

// SparseFileFunc writes a sparse database image of pageSize*commit bytes to dest,
// filling only the given page ranges via fill and leaving the rest as holes. It
// is the shared bounded-memory, on-demand materializer: fill may fetch one page
// at a time (from a chain, or from object storage with a ranged read).
func SparseFileFunc(dest string, pageSize, commit uint32, fill func(pgno uint32) ([]byte, error), ranges [][2]uint32) (int, error) {
	if pageSize == 0 || commit == 0 {
		return 0, fmt.Errorf("restore: empty image")
	}
	if len(ranges) == 0 {
		ranges = [][2]uint32{{1, commit}}
	}
	for _, r := range ranges {
		if r[0] == 0 || r[1] < r[0] || r[1] > commit {
			return 0, fmt.Errorf("restore: invalid page range %d-%d", r[0], r[1])
		}
	}
	if err := os.MkdirAll(dirOf(dest), 0o755); err != nil {
		return 0, err
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		_ = os.Remove(dest + suffix)
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	// Size the file to the full image; the unwritten tail is a hole.
	if err := f.Truncate(int64(commit) * int64(pageSize)); err != nil {
		return 0, err
	}
	written := 0
	for _, r := range ranges {
		for pgno := r[0]; pgno <= r[1]; pgno++ {
			data, err := fill(pgno)
			if err != nil {
				return written, err
			}
			if len(data) != int(pageSize) {
				return written, fmt.Errorf("restore: page %d length %d, want %d", pgno, len(data), pageSize)
			}
			if _, err := f.WriteAt(data, int64(pgno-1)*int64(pageSize)); err != nil {
				return written, err
			}
			written++
		}
	}
	if err := f.Sync(); err != nil {
		return written, err
	}
	return written, nil
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
