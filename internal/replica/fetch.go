package replica

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
)

// restoreWorkSlots bounds expensive restore work across all managers in a process.
// Construction releases its slot before returning; Materialize acquires separately.
var restoreWorkSlots = make(chan struct{}, 2)

func acquireRestoreWork(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case restoreWorkSlots <- struct{}{}:
		return func() { <-restoreWorkSlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

var ErrNoPageIndex = errors.New("replica: no native page index")

// A frame is at most the native LZ4 bound for a 64 KiB SQLite page, so
// its size fits uint32. Object identity is stored once per native object.
type pageLocation struct {
	Offset uint64
	Size   uint32
	object uint32
}

type pageObject struct{ key, etag string }

// Allocate only blocks containing validated entries, never from the declared
// Commit: a corrupt sparse index can claim up to 2^32-1 pages.
const pageIndexBlockSize = 1024

type pageIndex struct {
	blocks map[uint32]*[pageIndexBlockSize]pageLocation
	count  uint64
}

func (idx *pageIndex) get(pg uint32) (pageLocation, bool) {
	block := idx.blocks[pg/pageIndexBlockSize]
	if block == nil {
		return pageLocation{}, false
	}
	loc := block[pg%pageIndexBlockSize]
	return loc, loc.Size != 0
}

func (idx *pageIndex) set(pg uint32, loc pageLocation) {
	if idx.blocks == nil {
		idx.blocks = make(map[uint32]*[pageIndexBlockSize]pageLocation)
	}
	block := idx.blocks[pg/pageIndexBlockSize]
	if block == nil {
		block = new([pageIndexBlockSize]pageLocation)
		idx.blocks[pg/pageIndexBlockSize] = block
	}
	if block[pg%pageIndexBlockSize].Size == 0 {
		idx.count++
	}
	block[pg%pageIndexBlockSize] = loc
}

func (idx *pageIndex) truncate(commit uint32) {
	for n, block := range idx.blocks {
		if n < commit/pageIndexBlockSize {
			continue
		}
		start := uint32(0)
		if n == commit/pageIndexBlockSize {
			start = commit%pageIndexBlockSize + 1
		}
		for i := start; i < pageIndexBlockSize; i++ {
			if block[i].Size != 0 {
				idx.count--
			}
			block[i] = pageLocation{}
		}
		if n > commit/pageIndexBlockSize {
			delete(idx.blocks, n)
		}
	}
}

// PageFetcher is an immutable activation cut built from native embedded indexes.
// Faults never rebuild against newer data: mixing two cuts can corrupt SQLite.
type PageFetcher struct {
	m                *Manager
	pages            pageIndex
	objects          []pageObject
	commit, pageSize uint32
	watermark        uint64
	resident         int64
}

func (m *Manager) NewPageFetcher(ctx context.Context, s cell.Scope, epoch uint64) (*PageFetcher, error) {
	pf, _, err := m.NewPageFetcherMinBytes(ctx, s, epoch, 0)
	return pf, err
}

func (m *Manager) NewPageFetcherMinBytes(ctx context.Context, s cell.Scope, epoch uint64, minBytes int64) (pf *PageFetcher, ok bool, err error) {
	release, err := acquireRestoreWork(ctx)
	if err != nil {
		return nil, false, err
	}
	defer release()
	plan, err := m.nativeRestorePlan(ctx, s, epoch)
	if err != nil {
		return nil, false, err
	}
	if len(plan) == 0 {
		return nil, false, ErrNoPageIndex
	}
	objects := m.resourceObjects()
	// The newest header gives the final image size before any index downloads.
	newest := plan[len(plan)-1]
	raw, _, err := objects.RangedGet(ctx, newest.key, 0, ltx.NativeHeaderSize)
	if err != nil {
		return nil, false, err
	}
	h, err := ltx.ParseNativeHeader(raw)
	if err != nil {
		return nil, false, err
	}
	if h.MinTXID != newest.min || h.MaxTXID != newest.max {
		return nil, false, fmt.Errorf("replica: header/path mismatch")
	}
	if minBytes > 0 && uint64(h.PageSize)*uint64(h.Commit) < uint64(minBytes) {
		return nil, false, nil
	}
	pf = &PageFetcher{m: m, pageSize: h.PageSize, commit: h.Commit, watermark: h.MaxTXID}
	built := pf
	defer func() {
		if err != nil {
			built.DropLocalIndex()
		}
	}()
	if len(plan) > 16384 {
		return nil, false, ErrPageIndexCapacity
	}
	if err = pf.reserve(int64(len(plan)) * 64); err != nil {
		return nil, false, err
	}
	pf.objects = make([]pageObject, 0, len(plan))
	type result struct {
		header      ltx.NativeHeader
		pages       []ltx.NativePageLocation
		object      pageObject
		err         error
		reservation int64
	}
	parallel := m.RestoreParallel
	if parallel <= 0 {
		parallel = 32
	}
	// Bound retained intermediate indexes even when callers request more workers.
	parallel = min(parallel, 32, len(plan))
	var previousCommit uint32
	for start := 0; start < len(plan); start += parallel {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		batch := plan[start:min(start+parallel, len(plan))]
		results := make([]result, len(batch))
		var wg sync.WaitGroup
		for i, f := range batch {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i].header, results[i].pages, results[i].object, results[i].reservation, results[i].err = m.fetchNativeIndex(ctx, f)
			}()
		}
		wg.Wait()
		var scratch int64
		for i := range results {
			scratch += results[i].reservation
		}
		// Returned parsed indexes are all charged until this batch is merged.
		defer func() { pageIndexResident.Add(-scratch) }()
		for i := range results {
			r := &results[i]
			if r.err != nil {
				return nil, false, r.err
			}
			if r.header.PageSize != h.PageSize {
				return nil, false, fmt.Errorf("replica: page size mismatch")
			}
			// Preserve truncation across batches: extension cannot resurrect old pages.
			if r.header.Commit < previousCommit {
				before := len(pf.pages.blocks)
				pf.pages.truncate(r.header.Commit)
				released := int64(before-len(pf.pages.blocks)) * (pageIndexBlockSize*16 + 128)
				pf.resident -= released
				pageIndexResident.Add(-released)
			}
			previousCommit = r.header.Commit
			if uint64(len(pf.objects)) > uint64(^uint32(0)) {
				return nil, false, fmt.Errorf("replica: too many native objects")
			}
			object := uint32(len(pf.objects))
			if err = pf.reserve(int64(len(r.object.key) + len(r.object.etag))); err != nil {
				return nil, false, err
			}
			pf.objects = append(pf.objects, r.object)
			for _, loc := range r.pages {
				if loc.PageNo <= h.Commit {
					if pf.pages.blocks[loc.PageNo/pageIndexBlockSize] == nil {
						if err = pf.reserve(pageIndexBlockSize*16 + 128); err != nil {
							return nil, false, err
						}
					}
					pf.pages.set(loc.PageNo, pageLocation{Offset: loc.Offset, Size: uint32(loc.Size), object: object})
				}
			}
			r.pages = nil
		}
		pageIndexResident.Add(-scratch)
		scratch = 0
	}
	// Every final image page must have a source before activation. Count the
	// validated unique page numbers instead of walking a potentially huge image.
	// SQLite's pending-byte lock page is intentionally absent from native LTX.
	expected := uint64(h.Commit)
	if h.Commit >= uint32(0x40000000)/h.PageSize+1 {
		expected--
	}
	if pf.pages.count != expected {
		return nil, false, fmt.Errorf("replica: incomplete native page index: have %d pages, need %d", pf.pages.count, expected)
	}
	// Reserve the owning supervisor's possible hydration bitmap up front,
	// so native I/O cannot first encounter capacity while marking a page.
	if err = pf.reserve(int64(len(pf.pages.blocks)+1) * 256); err != nil {
		return nil, false, err
	}
	return pf, true, nil
}

func (m *Manager) fetchNativeIndex(ctx context.Context, f epochFile) (header ltx.NativeHeader, locations []ltx.NativePageLocation, object pageObject, reservation int64, retErr error) {
	// Reserve bounded tail/header scratch before any ranged allocation.
	if err := reservePageIndex(512 << 10); err != nil {
		return header, nil, object, 0, err
	}
	reservation = 512 << 10
	defer func() {
		if retErr != nil {
			pageIndexResident.Add(-reservation)
			reservation = 0
		}
	}()
	objects := m.resourceObjects()
	var h ltx.NativeHeader
	size, etag, err := objects.Stat(ctx, f.key)
	if err != nil {
		return h, nil, pageObject{}, reservation, err
	}
	if size < ltx.NativeHeaderSize+31 {
		return h, nil, pageObject{}, reservation, ltx.ErrShort
	}
	read := func(off, n int64) ([]byte, error) {
		raw, tag, err := objects.RangedGet(ctx, f.key, off, n)
		if err != nil {
			return nil, err
		}
		if int64(len(raw)) != n || tag != etag {
			return nil, fmt.Errorf("replica: short or changed native object %s", f.key)
		}
		return raw, nil
	}
	tailStart := max(int64(0), size-(256<<10))
	tail, err := read(tailStart, size-tailStart)
	if err != nil {
		return h, nil, pageObject{}, reservation, err
	}
	var head []byte
	if tailStart == 0 {
		head = tail[:ltx.NativeHeaderSize]
	} else {
		head, err = read(0, ltx.NativeHeaderSize)
		if err != nil {
			return h, nil, pageObject{}, reservation, err
		}
	}
	h, err = ltx.ParseNativeHeader(head)
	if err != nil {
		return h, nil, pageObject{}, reservation, err
	}
	if h.MinTXID != f.min || h.MaxTXID != f.max {
		return h, nil, pageObject{}, reservation, fmt.Errorf("replica: header/path mismatch %s", f.key)
	}
	indexLen := binary.BigEndian.Uint64(tail[len(tail)-24:])
	if indexLen < 1 || indexLen > uint64(size-ltx.NativeHeaderSize-30) {
		return h, nil, pageObject{}, reservation, fmt.Errorf("replica: invalid index length")
	}
	// Reserve encoded input first, before the ranged allocation. Parsed
	// scratch is charged separately after an allocation-free varint count.
	if indexLen > uint64(pageIndexResidentLimit) {
		return h, nil, pageObject{}, reservation, ErrPageIndexCapacity
	}
	if err = reservePageIndex(int64(indexLen) + 6); err != nil {
		return h, nil, pageObject{}, reservation, err
	}
	reservation += int64(indexLen) + 6
	indexStart := size - 24 - int64(indexLen)
	var index []byte
	// Include the native zero-page terminator in the validation.
	if indexStart-6 >= tailStart {
		index = tail[indexStart-6-tailStart : len(tail)-24]
	} else {
		index, err = read(indexStart-6, int64(indexLen)+6)
		if err != nil {
			return h, nil, pageObject{}, reservation, err
		}
	}
	for _, b := range index[:6] {
		if b != 0 {
			return h, nil, pageObject{}, reservation, fmt.Errorf("replica: invalid page terminator")
		}
	}
	entries, countErr := countNativeIndexEntries(index[6:])
	if countErr != nil || entries > uint64(h.Commit) {
		return h, nil, pageObject{}, reservation, fmt.Errorf("replica: invalid native index entries")
	}
	// Covers old+new backing arrays during parser slice growth (24-byte
	// entries), including transient allocation before the old slice is GC'd.
	if entries > uint64(pageIndexResidentLimit/64) {
		return h, nil, pageObject{}, reservation, ErrPageIndexCapacity
	}
	if err = reservePageIndex(int64(entries) * 64); err != nil {
		return h, nil, pageObject{}, reservation, err
	}
	reservation += int64(entries) * 64
	locs, err := ltx.ParseNativePageIndex(index[6:], uint64(indexStart-6), h)
	if err != nil {
		return h, nil, pageObject{}, reservation, err
	}
	return h, locs, pageObject{key: f.key, etag: etag}, reservation, nil
}

func (pf *PageFetcher) Commit() uint32    { return pf.commit }
func (pf *PageFetcher) PageSize() uint32  { return pf.pageSize }
func (pf *PageFetcher) Watermark() uint64 { return pf.watermark }
func (pf *PageFetcher) TxID() uint64      { return pf.watermark }

// Close implements the page-source lifecycle; native indexes hold no remote
// lease or renewable state and therefore create no COS sidecar objects.
func (pf *PageFetcher) Close() error { pf.DropLocalIndex(); return nil }

func (pf *PageFetcher) Page(ctx context.Context, pgno uint32) ([]byte, error) {
	pages, err := pf.ReadRun(ctx, pgno, 1)
	if err != nil {
		return nil, err
	}
	return pages[0], nil
}

// Bound decoded memory separately from network transfer: compressible pages
// should not force thousands of tiny COS requests during a large restore.
const runByteBudget = 8 << 20
const runEncodedBudget = 1 << 20

// ReadRun stops at a changed page owner, even when an older object's index
// contains physically adjacent frames for the same page numbers.
func (pf *PageFetcher) ReadRun(ctx context.Context, pgno uint32, maxPages int) ([][]byte, error) {
	return pf.readRun(ctx, pgno, maxPages, runEncodedBudget)
}

// readRun uses a larger transfer budget for sequential materialization while
// preserving the decoded-memory bound and the exact latest-page ownership cut.
func (pf *PageFetcher) readRun(ctx context.Context, pgno uint32, maxPages int, encodedBudget uint64) ([][]byte, error) {
	first, ok := pf.pages.get(pgno)
	if !ok || pgno == 0 || pgno > pf.commit {
		return nil, fmt.Errorf("replica: page %d not defined", pgno)
	}
	maxPages = max(1, min(maxPages, runByteBudget/int(pf.pageSize)))
	end := first.Offset + uint64(first.Size)
	locs := []pageLocation{first}
	for n := 1; n < maxPages && uint64(pgno)+uint64(n) <= uint64(pf.commit); n++ {
		loc, ok := pf.pages.get(pgno + uint32(n))
		if !ok || loc.object != first.object || loc.Offset != end || end+uint64(loc.Size)-first.Offset > encodedBudget {
			break
		}
		locs = append(locs, loc)
		end += uint64(loc.Size)
	}
	object := pf.objects[first.object]
	raw, etag, err := pf.m.resourceObjects().RangedGet(ctx, object.key, int64(first.Offset), int64(end-first.Offset))
	if err != nil {
		return nil, err
	}
	if etag != object.etag || uint64(len(raw)) != end-first.Offset {
		return nil, fmt.Errorf("replica: short or changed native object %s", object.key)
	}
	out := make([][]byte, len(locs))
	var at uint64
	for i, loc := range locs {
		out[i], err = ltx.DecodeNativePageFrame(raw[at:at+uint64(loc.Size)], pf.pageSize, pgno+uint32(i))
		if err != nil {
			return nil, err
		}
		at += uint64(loc.Size)
	}
	return out, nil
}
func (pf *PageFetcher) Materialize(ctx context.Context, dest string, ranges [][2]uint32) (int, error) {
	release, err := acquireRestoreWork(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	// Keep one decoded run per materialization, bounded by runByteBudget.
	// ReadRun already stops at a different source/ETag or non-adjacent frame;
	// also stop at the requested sparse range boundary to avoid read amplification.
	var cached [][]byte
	var first uint32
	fill := func(pgno uint32) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if pgno == uint32(0x40000000)/pf.pageSize+1 {
			return make([]byte, pf.pageSize), nil
		}
		if pgno >= first && uint64(pgno)-uint64(first) < uint64(len(cached)) {
			return cached[pgno-first], nil
		}
		end := pf.commit
		if len(ranges) > 0 {
			for _, r := range ranges {
				if pgno >= r[0] && pgno <= r[1] {
					end = r[1]
					break
				}
			}
		}
		limit := int(min(uint64(end)-uint64(pgno)+1, uint64(runByteBudget)/uint64(pf.pageSize)))
		var err error
		cached, err = pf.readRun(ctx, pgno, limit, runByteBudget)
		if err != nil {
			return nil, err
		}
		first = pgno
		return cached[0], nil
	}
	return restore.SparseFileFunc(dest, pf.pageSize, pf.commit, fill, ranges)
}

func countNativeIndexEntries(raw []byte) (uint64, error) {
	var count uint64
	for {
		pg, n := binary.Uvarint(raw)
		if n <= 0 {
			return 0, ltx.ErrShort
		}
		raw = raw[n:]
		if pg == 0 {
			if len(raw) != 0 {
				return 0, ltx.ErrShort
			}
			return count, nil
		}
		for i := 0; i < 2; i++ {
			_, n = binary.Uvarint(raw)
			if n <= 0 {
				return 0, ltx.ErrShort
			}
			raw = raw[n:]
		}
		count++
	}
}
