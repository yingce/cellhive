package replica

import (
	"context"
	"fmt"
	"sort"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
)

// PageFetcher resolves pages of a scope/epoch on demand. Pages folded into the
// persisted L1 page index are fetched one at a time with a ranged read; pages
// from newer (unfolded) L0 deltas are held in memory (L0 is small by
// construction). It never downloads the whole L1 snapshot to answer a page, so a
// cold start can materialize only the pages it needs.
type PageFetcher struct {
	m        *Manager
	scope    cell.Scope
	epoch    uint64
	idx      PageIndex
	entryOf  map[uint32]int
	l0       map[uint32][]byte
	commit   uint32
	pageSize uint32
}

// NewPageFetcher builds a fetcher from the persisted L1 page index plus newer L0
// deltas. It requires a page index (a compacted cell); an uncompacted cell has no
// index and should be restored with Restore.
func (m *Manager) NewPageFetcher(ctx context.Context, s cell.Scope, epoch uint64) (*PageFetcher, error) {
	idx, ok, err := m.ReadIndex(ctx, s, epoch)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("replica: no page index for %s (cell not compacted)", s.String())
	}
	pf := &PageFetcher{
		m: m, scope: s, epoch: epoch, idx: idx,
		entryOf: map[uint32]int{}, l0: map[uint32][]byte{},
	}
	for ei, e := range idx.Entries {
		if e.PageSize == 0 {
			return nil, fmt.Errorf("replica: index entry %d has zero page size", ei)
		}
		if pf.pageSize == 0 {
			pf.pageSize = e.PageSize
		} else if e.PageSize != pf.pageSize {
			return nil, fmt.Errorf("replica: index page size mismatch")
		}
		if e.Commit > pf.commit {
			pf.commit = e.Commit
		}
		for _, p := range e.Pages {
			pf.entryOf[p] = ei
		}
	}

	l0keys, err := m.ListL0Segments(ctx, s, epoch)
	if err != nil {
		return nil, err
	}
	// Collect the L0 deltas first and require a contiguous chain from the index
	// watermark before applying any of them: a missing (not yet uploaded) delta
	// must fail closed instead of silently producing a stale database.
	type l0delta struct {
		start, end uint64
		pageSize   uint32
		commit     uint32
		pages      []ltx.WALPage
	}
	var deltas []l0delta
	for _, k := range l0keys {
		raw, err := m.ReadSegment(ctx, k)
		if err != nil {
			return nil, err
		}
		parts, err := ltx.Split(raw)
		if err != nil {
			return nil, fmt.Errorf("replica: corrupt L0 object %s: %w", k, err)
		}
		for _, part := range parts {
			h, payload, err := ltx.Decode(part)
			if err != nil {
				return nil, err
			}
			if h.Kind != ltx.KindDelta || h.StartTxID <= idx.Watermark {
				continue
			}
			ps, commit, _, pages, err := ltx.DecodeWALPageMap(payload)
			if err != nil {
				return nil, fmt.Errorf("replica: decode L0 delta: %w", err)
			}
			deltas = append(deltas, l0delta{start: h.StartTxID, end: h.EndTxID, pageSize: ps, commit: commit, pages: pages})
		}
	}
	sort.SliceStable(deltas, func(i, j int) bool { return deltas[i].start < deltas[j].start })
	expected := idx.Watermark + 1
	for _, d := range deltas {
		if d.start != expected {
			return nil, fmt.Errorf("replica: non-contiguous L0 chain: expected txid %d, got %d", expected, d.start)
		}
		if d.end < d.start {
			return nil, fmt.Errorf("replica: invalid L0 delta range %d..%d", d.start, d.end)
		}
		expected = d.end + 1
	}
	for _, d := range deltas {
		if pf.pageSize == 0 {
			pf.pageSize = d.pageSize
		} else if d.pageSize != pf.pageSize {
			return nil, fmt.Errorf("replica: L0 page size mismatch")
		}
		if d.commit > pf.commit {
			pf.commit = d.commit
		}
		for _, p := range d.pages {
			pf.l0[p.PageNo] = p.Data
		}
	}
	if pf.pageSize == 0 || pf.commit == 0 {
		return nil, fmt.Errorf("replica: page index has no pages")
	}
	return pf, nil
}

// Commit is the final database page count across the index and newer L0 deltas.
func (pf *PageFetcher) Commit() uint32 { return pf.commit }

// PageSize is the database page size.
func (pf *PageFetcher) PageSize() uint32 { return pf.pageSize }

// Watermark is the highest txid folded into the index.
func (pf *PageFetcher) Watermark() uint64 { return pf.idx.Watermark }

// Page returns the final version of one page. Newer L0 deltas win; otherwise the
// page is fetched from its L1 object with a single ranged read.
func (pf *PageFetcher) Page(ctx context.Context, pgno uint32) ([]byte, error) {
	if data, ok := pf.l0[pgno]; ok {
		return data, nil
	}
	ei, ok := pf.entryOf[pgno]
	if !ok {
		return nil, fmt.Errorf("replica: page %d not defined", pgno)
	}
	e := pf.idx.Entries[ei]
	pos := sort.Search(len(e.Pages), func(i int) bool { return e.Pages[i] >= pgno })
	if pos >= len(e.Pages) || e.Pages[pos] != pgno {
		return nil, fmt.Errorf("replica: page %d missing from index entry", pgno)
	}
	if len(e.Locs) == len(e.Pages) && len(e.Locs) > 0 {
		// v2: the index carries the frame location; the frame may be compressed.
		loc := e.Locs[pos]
		raw, _, err := pf.m.B.RangedGet(ctx, e.Key, int64(ltx.HeaderSize)+loc.Off, int64(loc.Stored))
		if err != nil {
			return nil, fmt.Errorf("replica: ranged read %s page %d: %w", e.Key, pgno, err)
		}
		if len(raw) != loc.Stored {
			return nil, fmt.Errorf("replica: short ranged read for page %d", pgno)
		}
		// raw starts at the frame, so the loc's absolute offset no longer applies.
		sliced := loc
		sliced.Off = 0
		return ltx.FrameData(raw, sliced)
	}
	// v1: fixed-size frames with a 4-byte page-number prefix.
	off := ltx.HeaderSize + ltx.PageEntryOffset(e.PageSize, pos)
	want := 4 + int(e.PageSize)
	raw, _, err := pf.m.B.RangedGet(ctx, e.Key, int64(off), int64(want))
	if err != nil {
		return nil, fmt.Errorf("replica: ranged read %s page %d: %w", e.Key, pgno, err)
	}
	if len(raw) != want {
		return nil, fmt.Errorf("replica: short ranged read for page %d", pgno)
	}
	got := uint32(raw[0])<<24 | uint32(raw[1])<<16 | uint32(raw[2])<<8 | uint32(raw[3])
	if got != pgno {
		return nil, fmt.Errorf("replica: index offset mismatch for page %d (got %d)", pgno, got)
	}
	out := make([]byte, e.PageSize)
	copy(out, raw[4:])
	return out, nil
}

// ReadRun returns the final version of pgno plus up to maxPages-1 pages that
// follow it and are stored contiguously in the same L1 object, fetched with one
// ranged read. It is the paging window (ADR-160): a scan then costs one bucket
// read per window instead of one per page. Pages that fall back to an in-memory
// L0 delta, leave the L1 entry, or exceed the byte budget end the run.
func (pf *PageFetcher) ReadRun(ctx context.Context, pgno uint32, maxPages int) ([][]byte, error) {
	if maxPages < 1 {
		maxPages = 1
	}
	if data, ok := pf.l0[pgno]; ok {
		return [][]byte{data}, nil
	}
	ei, ok := pf.entryOf[pgno]
	if !ok {
		return nil, fmt.Errorf("replica: page %d not defined", pgno)
	}
	e := pf.idx.Entries[ei]
	pos := sort.Search(len(e.Pages), func(i int) bool { return e.Pages[i] >= pgno })
	if pos >= len(e.Pages) || e.Pages[pos] != pgno {
		return nil, fmt.Errorf("replica: page %d missing from index entry", pgno)
	}
	n := 1
	if len(e.Locs) == len(e.Pages) && len(e.Locs) > 0 {
		// v2: extend while pages are consecutive and their frames are adjacent in
		// the object (one ranged read serves the whole span).
		for n < maxPages && pos+n < len(e.Pages) && e.Pages[pos+n] == pgno+uint32(n) {
			next := e.Locs[pos+n]
			prev := e.Locs[pos+n-1]
			if next.Off != prev.Off+int64(prev.Stored) || next.Codec != prev.Codec {
				break
			}
			if int64(n+1)*int64(e.PageSize) > runByteBudget {
				break
			}
			n++
		}
		start := e.Locs[pos]
		span := int64(0)
		for k := 0; k < n; k++ {
			span += int64(e.Locs[pos+k].Stored)
		}
		raw, _, err := pf.m.B.RangedGet(ctx, e.Key, int64(ltx.HeaderSize)+start.Off, span)
		if err != nil {
			return nil, fmt.Errorf("replica: ranged read %s pages %d..%d: %w", e.Key, pgno, pgno+uint32(n)-1, err)
		}
		out := make([][]byte, n)
		at := int64(0)
		for k := 0; k < n; k++ {
			loc := e.Locs[pos+k]
			if at+int64(loc.Stored) > int64(len(raw)) {
				return nil, fmt.Errorf("replica: short ranged read for pages %d..%d", pgno, pgno+uint32(n)-1)
			}
			sliced := loc
			sliced.Off = 0
			page, derr := ltx.FrameData(raw[at:at+int64(loc.Stored)], sliced)
			if derr != nil {
				return nil, derr
			}
			out[k] = page
			at += int64(loc.Stored)
		}
		return out, nil
	}
	// v1: fixed-size frames.
	for n < maxPages && pos+n < len(e.Pages) && e.Pages[pos+n] == pgno+uint32(n) {
		if int64(n+1)*int64(4+e.PageSize) > runByteBudget {
			break
		}
		n++
	}
	off := ltx.HeaderSize + ltx.PageEntryOffset(e.PageSize, pos)
	frame := 4 + int(e.PageSize)
	want := n * frame
	raw, _, err := pf.m.B.RangedGet(ctx, e.Key, int64(off), int64(want))
	if err != nil {
		return nil, fmt.Errorf("replica: ranged read %s pages %d..%d: %w", e.Key, pgno, pgno+uint32(n)-1, err)
	}
	if len(raw) != want {
		return nil, fmt.Errorf("replica: short ranged read for pages %d..%d", pgno, pgno+uint32(n)-1)
	}
	out := make([][]byte, n)
	for k := 0; k < n; k++ {
		at := k * frame
		got := uint32(raw[at])<<24 | uint32(raw[at+1])<<16 | uint32(raw[at+2])<<8 | uint32(raw[at+3])
		if got != pgno+uint32(k) {
			return nil, fmt.Errorf("replica: index offset mismatch for page %d (got %d)", pgno+uint32(k), got)
		}
		page := make([]byte, e.PageSize)
		copy(page, raw[at+4:at+frame])
		out[k] = page
	}
	return out, nil
}

// runByteBudget bounds one window read (decoded pages). A scan is amortized
// without letting a single fault pull a large slice of the database.
const runByteBudget = 256 << 10

// Materialize writes a database image to dest, filling only the given page
// ranges (all pages when ranges is nil) by fetching each page on demand. Unfilled
// bytes remain holes. It returns the number of pages written and never holds more
// than one page (plus the small unfolded L0 set) in memory.
func (pf *PageFetcher) Materialize(ctx context.Context, dest string, ranges [][2]uint32) (int, error) {
	return restore.SparseFileFunc(dest, pf.pageSize, pf.commit, func(pgno uint32) ([]byte, error) {
		return pf.Page(ctx, pgno)
	}, ranges)
}
