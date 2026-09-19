package restore

import (
	"fmt"
	"os"
	"sort"

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

	refs     map[uint32]int
	payloads [][]byte
}

// IndexChain builds a PageIndex from an ordered segment chain (snapshot parts
// plus deltas), applying the same ordering and txid-contiguity rules as
// mergeChain. It reads only page numbers, not page data.
func IndexChain(segments [][]byte) (*PageIndex, error) {
	type part struct {
		h       ltx.Header
		payload []byte
	}
	var parts []part
	for i, raw := range segments {
		split, err := ltx.Split(raw)
		if err != nil {
			return nil, fmt.Errorf("restore: decode segment %d: %w", i, err)
		}
		for _, one := range split {
			h, payload, err := ltx.Decode(one)
			if err != nil {
				return nil, err
			}
			parts = append(parts, part{h: h, payload: payload})
		}
	}

	var snapWatermark uint64
	haveSnap := false
	for _, p := range parts {
		if p.h.Kind != ltx.KindSnapshot {
			continue
		}
		if !haveSnap || p.h.StartTxID > snapWatermark {
			snapWatermark = p.h.StartTxID
		}
		haveSnap = true
	}
	if !haveSnap {
		return nil, fmt.Errorf("restore: no snapshot in chain")
	}

	var order []part
	for _, p := range parts {
		if p.h.Kind == ltx.KindSnapshot && p.h.StartTxID == snapWatermark {
			order = append(order, p)
		}
	}
	sort.SliceStable(order, func(i, j int) bool {
		pi, _ := ltx.SnapshotPart(order[i].h)
		pj, _ := ltx.SnapshotPart(order[j].h)
		return pi < pj
	})
	var deltas []part
	for _, p := range parts {
		if p.h.Kind == ltx.KindDelta && p.h.StartTxID > snapWatermark {
			deltas = append(deltas, p)
		}
	}
	sort.SliceStable(deltas, func(i, j int) bool { return deltas[i].h.StartTxID < deltas[j].h.StartTxID })
	expected := snapWatermark + 1
	for _, d := range deltas {
		if d.h.StartTxID != expected {
			return nil, fmt.Errorf("restore: non-contiguous chain: expected txid %d, got %d", expected, d.h.StartTxID)
		}
		if d.h.EndTxID < d.h.StartTxID {
			return nil, fmt.Errorf("restore: invalid delta txid range %d..%d", d.h.StartTxID, d.h.EndTxID)
		}
		expected = d.h.EndTxID + 1
	}
	order = append(order, deltas...)

	ix := &PageIndex{refs: map[uint32]int{}, Watermark: snapWatermark}
	if len(deltas) > 0 {
		ix.Watermark = deltas[len(deltas)-1].h.EndTxID
	}
	for idx, p := range order {
		pageSize, commit, _, _, err := ltx.DecodeWALPageMap(p.payload)
		if err != nil {
			return nil, fmt.Errorf("restore: decode part %d: %w", idx, err)
		}
		if ix.PageSize == 0 {
			ix.PageSize = pageSize
		} else if pageSize != ix.PageSize {
			return nil, fmt.Errorf("restore: page size mismatch")
		}
		if commit > ix.Commit {
			ix.Commit = commit
		}
		nums, err := ltx.PageMapNumbers(p.payload)
		if err != nil {
			return nil, err
		}
		for _, no := range nums {
			ix.refs[no] = idx
		}
		ix.payloads = append(ix.payloads, p.payload)
	}
	if ix.Commit == 0 || ix.PageSize == 0 {
		return nil, fmt.Errorf("restore: snapshot has no pages")
	}
	return ix, nil
}

// Has reports whether the page is defined by the chain.
func (ix *PageIndex) Has(pgno uint32) bool {
	_, ok := ix.refs[pgno]
	return ok
}

// Page returns the final version of one page, reading only the part that holds it.
func (ix *PageIndex) Page(pgno uint32) ([]byte, error) {
	ref, ok := ix.refs[pgno]
	if !ok {
		return nil, fmt.Errorf("restore: page %d not defined", pgno)
	}
	data, found, err := ltx.PageMapLookup(ix.payloads[ref], pgno)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("restore: page %d missing from part %d", pgno, ref)
	}
	return data, nil
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
