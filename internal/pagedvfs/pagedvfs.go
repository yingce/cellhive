// Package pagedvfs implements page-on-demand cold restore for cell-agent cells
// (ADR-160): a SQLite VFS that faults main-database pages from the replication
// chain on first use, on top of a sparse local file sized to the pinned cut.
//
// It follows celld's model (crates/ltx/src/paged_vfs.rs): the fault runs
// synchronously on the thread that entered SQLite, writes the page into the
// local file, and marks it hydrated so it is fetched at most once. Pages SQLite
// writes (checkpoints) or truncates update the hydration set. The VFS only wraps
// the registered main database; WAL/SHM/journals/temp go straight to the base
// VFS.
//
// The page source is provided by the caller (cell-agent wires
// replica.PageFetcher, which ranged-reads the L1 page index and newer L0
// segments). A source read failure surfaces as SQLITE_IOERR_READ, so SQLite
// poisons the read instead of continuing on zeros.
package pagedvfs

/*
#cgo CFLAGS: -O2 -I${SRCDIR}/../sqliteheaders
#include "sqlite3.h"
#include <stdlib.h>
int cellhive_paged_init(void);
int cellhive_paged_register(int id, const char *path, unsigned int pageSize, unsigned int commit);
int cellhive_paged_release(int id);
int cellhive_paged_hydrated(int id, unsigned int pgno);
void cellhive_paged_mark(int id, unsigned int pgno);
int cellhive_paged_commit(int id);
int cellhive_paged_page_size(int id);
int cellhive_paged_hydrated_count(int id);
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
	"unsafe"
)

// VFSName is the SQLite VFS name; open a paged file with "?_vfs=cellhive-paged".
const VFSName = "cellhive-paged"

// Source supplies pages of a pinned cut. ReadPage returns exactly PageSize
// bytes for pgno (1-based); os.ErrNotExist may be returned for a page absent
// from the cut (it reads as zeros and is never fetched again).
type Source interface {
	PageSize() int
	Commit() int
	ReadPage(pgno uint32) ([]byte, error)
}

// RunSource is an optional Source extension: ReadRun returns the page at pgno
// plus up to maxPages-1 pages that follow it contiguously, fetched together
// (one ranged read). pagedvfs caches the run so the next faults in the same
// window cost no bucket read (ADR-160 window prefetch).
type RunSource interface {
	ReadRun(pgno uint32, maxPages int) ([][]byte, error)
}

// WindowPages bounds one prefetch window.
var WindowPages = 64

// windowPages may be lowered by tests.
func windowLimit() int {
	if WindowPages < 1 {
		return 1
	}
	return WindowPages
}

var (
	initOnce sync.Once
	initErr  error

	mu     sync.Mutex
	nextID int
	regs   map[int]*reg
)

type reg struct {
	id   int
	path string
	src  Source
	// faults counts pages fetched from the source; runs counts fetch calls
	// (a run fetches a whole window in one call); prefetchHits counts faults
	// served from a prefetched child without touching the source.
	faults       uint64
	runs         uint64
	prefetchHits uint64
	prefetched   uint64
	// window caches the last run: windowBase..windowEnd.
	windowBase uint32
	window     [][]byte
	windowEnd  uint32
	// b-tree awareness (celld's scan.children): an interior page names its
	// children. A child is fetched alone — its page-order neighbours may be a
	// big-row table's overflow chain, megabytes a scan never reads. When a scan
	// walks children in order, the next few are prefetched ahead.
	children      map[uint32]struct{}
	lastChild     uint32
	prefetch      map[uint32][]byte
	prefetchBytes int
}

func init() {
	initOnce.Do(func() {
		if rc := C.cellhive_paged_init(); rc != 0 {
			initErr = fmt.Errorf("pagedvfs: register VFS: rc=%d", rc)
		}
		mu.Lock()
		regs = map[int]*reg{}
		mu.Unlock()
	})
}

// ErrUnavailable means the VFS could not be registered (e.g. another SQLite
// build owns the default VFS).
var ErrUnavailable = errors.New("pagedvfs: unavailable")

// Available reports whether the VFS registered successfully.
func Available() bool {
	initOnce.Do(func() {})
	return initErr == nil
}

// Register declares path as a paged database backed by src. The caller must
// have created the sparse file at path sized PageSize()*Commit() (Prepare does
// that). Registering the same path twice replaces the source and clears the
// hydration set.
func Register(path string, src Source) error {
	if src == nil {
		return errors.New("pagedvfs: nil source")
	}
	if !Available() {
		return ErrUnavailable
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	ps, commit := src.PageSize(), src.Commit()
	if ps <= 0 || commit <= 0 {
		return fmt.Errorf("pagedvfs: bad cut (pageSize=%d commit=%d)", ps, commit)
	}
	cpath := C.CString(abs)
	defer C.free(unsafe.Pointer(cpath))

	mu.Lock()
	defer mu.Unlock()
	id := nextID
	nextID++
	if rc := C.cellhive_paged_register(C.int(id), cpath, C.uint(ps), C.uint(commit)); rc != 0 {
		return fmt.Errorf("pagedvfs: register %s: rc=%d", abs, rc)
	}
	regs[id] = &reg{id: id, path: abs, src: src}
	return nil
}

// Prepare creates (or truncates) the sparse local file at path to the cut size
// and registers it.
func Prepare(path string, src Source) error {
	if _, err := os.Stat(path); err != nil {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	size := int64(src.PageSize()) * int64(src.Commit())
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// A stale WAL from a previous life must not shadow the pinned cut.
	for _, suffix := range []string{"-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
	return Register(path, src)
}

// Release unregisters path (idempotent).
func Release(path string) {
	if !Available() {
		return
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	for id, r := range regs {
		if r.path == abs {
			C.cellhive_paged_release(C.int(id))
			delete(regs, id)
		}
	}
}

// IsPaged reports whether path is currently registered.
func IsPaged(path string) bool {
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	mu.Lock()
	defer mu.Unlock()
	for _, r := range regs {
		if r.path == abs {
			return true
		}
	}
	return false
}

// Runs returns how many source fetch calls (windows/pages) path has made.
func Runs(path string) uint64 {
	abs, err := filepath.Abs(path)
	if err != nil {
		return 0
	}
	mu.Lock()
	defer mu.Unlock()
	for _, r := range regs {
		if r.path == abs {
			return r.runs
		}
	}
	return 0
}

// Prefetch returns (served from prefetch, prefetched pages) for path.
func Prefetch(path string) (hits, prefetched uint64) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return 0, 0
	}
	mu.Lock()
	defer mu.Unlock()
	for _, r := range regs {
		if r.path == abs {
			return r.prefetchHits, r.prefetched
		}
	}
	return 0, 0
}

// Faults returns how many pages have been fetched for path.
func Faults(path string) uint64 {
	abs, err := filepath.Abs(path)
	if err != nil {
		return 0
	}
	mu.Lock()
	defer mu.Unlock()
	for _, r := range regs {
		if r.path == abs {
			return r.faults
		}
	}
	return 0
}

func lookup(id int) *reg {
	mu.Lock()
	defer mu.Unlock()
	return regs[id]
}

// goPagedFetch is the C fault callback: copy one page into buf. It applies the
// celld-model fault policy (ADR-160):
//
//   - a cached page (window or prefetched child) is served without a fetch;
//   - a b-tree child named by an interior page is fetched alone, because its
//     page-order neighbours may be a big-row table's overflow chain;
//   - a fault continuing right after the previous run (a scan) fetches a window;
//   - anything else (root, leaf of a point read, overflow) fetches a window
//     bounded by the byte budget.
//
//export goPagedFetch
func goPagedFetch(id C.int, pgno C.uint, buf unsafe.Pointer, bufLen C.int) C.int {
	r := lookup(int(id))
	if r == nil || buf == nil || bufLen <= 0 {
		return -1
	}
	pg := uint32(pgno)
	page := r.cacheGet(pg)
	if page == nil {
		maxPages := 1
		isChild := r.isChild(pg)
		if isChild {
			maxPages = 1 // never pull a child's overflow neighbours
		} else if r.windowEnd != 0 && pg == r.windowEnd+1 {
			maxPages = windowLimit() // sequential access: read ahead
		}
		pages, err := r.fetch(pg, maxPages)
		if err != nil || len(pages) == 0 {
			return -1
		}
		page = pages[0]
		r.storeRun(pg, pages)
		r.notePageType(pages)
		if isChild {
			prev := r.dropChild(pg)
			r.prefetchChildren(pg, prev)
		}
	}
	if len(page) != int(bufLen) {
		return -1
	}
	dst := unsafe.Slice((*byte)(buf), int(bufLen))
	copy(dst, page)
	return 0
}

// fetch reads up to maxPages pages starting at pg (a run when supported).
func (r *reg) fetch(pg uint32, maxPages int) ([][]byte, error) {
	if maxPages > 1 {
		if rs, ok := r.src.(RunSource); ok {
			pages, err := rs.ReadRun(pg, maxPages)
			if err == nil && len(pages) > 0 {
				mu.Lock()
				r.runs++
				r.faults += uint64(len(pages))
				mu.Unlock()
				return pages, nil
			}
		}
	}
	page, err := r.src.ReadPage(pg)
	if err != nil {
		return nil, err
	}
	mu.Lock()
	r.runs++
	r.faults++
	mu.Unlock()
	return [][]byte{page}, nil
}

// cacheGet returns a cached page (window or prefetched child) and counts a
// prefetch hit.
func (r *reg) cacheGet(pgno uint32) []byte {
	mu.Lock()
	defer mu.Unlock()
	if p, ok := r.prefetch[pgno]; ok {
		delete(r.prefetch, pgno)
		r.prefetchBytes -= len(p)
		r.prefetchHits++
		return p
	}
	if len(r.window) != 0 && pgno >= r.windowBase {
		if i := int(pgno - r.windowBase); i < len(r.window) {
			return r.window[i]
		}
	}
	return nil
}

// storeRun keeps a fetched run as the current window.
func (r *reg) storeRun(pg uint32, pages [][]byte) {
	mu.Lock()
	defer mu.Unlock()
	r.windowBase, r.window = pg, pages
	r.windowEnd = pg + uint32(len(pages)) - 1
}

// isChild reports whether pg was named as a child by an interior page.
func (r *reg) isChild(pg uint32) bool {
	mu.Lock()
	defer mu.Unlock()
	_, ok := r.children[pg]
	return ok
}

// dropChild removes pg from the child set and returns the previous child fault
// (sequential-scan detection).
func (r *reg) dropChild(pg uint32) (prev uint32) {
	mu.Lock()
	defer mu.Unlock()
	prev = r.lastChild
	delete(r.children, pg)
	r.lastChild = pg
	return prev
}

// notePageType parses the first fetched page and records its children when it is
// a b-tree interior page (interior index 0x02 / interior table 0x05).
func (r *reg) notePageType(pages [][]byte) {
	if len(pages) == 0 || len(pages[0]) == 0 {
		return
	}
	kids := parseChildren(pages[0])
	if len(kids) == 0 {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if r.children == nil {
		r.children = map[uint32]struct{}{}
	}
	for _, c := range kids {
		if len(r.children) >= maxChildren {
			break
		}
		r.children[c] = struct{}{}
	}
}

// PrefetchWorkers bounds concurrent child prefetches (celld's PREFETCH_WORKERS).
// The page source must be safe for concurrent reads (replica.PageFetcher is:
// its index and L0 maps are immutable after construction).
var PrefetchWorkers = 4

// prefetchChildren reads the children right after a sequential child fault into
// the memory cache (celld's SCAN_AHEAD). Contiguous children are grouped into one
// ranged read and the groups are fetched concurrently; nothing is written to the
// file from Go.
func (r *reg) prefetchChildren(after, prev uint32) {
	mu.Lock()
	sequential := prev != 0 && after > prev
	var next []uint32
	if sequential {
		for c := range r.children {
			if c > after {
				next = append(next, c)
			}
		}
		sort.Slice(next, func(i, j int) bool { return next[i] < next[j] })
		if len(next) > scanAhead {
			next = next[:scanAhead]
		}
	}
	mu.Unlock()
	if len(next) == 0 {
		return
	}
	// Group contiguous children into runs: a small-row table's leaves come as
	// one ranged read.
	type span struct {
		start uint32
		n     int
	}
	var spans []span
	for i := 0; i < len(next); {
		last := i
		for last+1 < len(next) && next[last+1] == next[last]+1 && last+1-i < windowLimit() {
			last++
		}
		spans = append(spans, span{start: next[i], n: last - i + 1})
		i = last + 1
	}

	type fetched struct {
		pgno uint32
		data []byte
	}
	var (
		resMu sync.Mutex
		got   []fetched
	)
	workers := PrefetchWorkers
	if workers > len(spans) {
		workers = len(spans)
	}
	if workers < 1 {
		workers = 1
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, sp := range spans {
		sp := sp
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			var pages [][]byte
			if sp.n > 1 {
				if rs, ok := r.src.(RunSource); ok {
					if p, err := rs.ReadRun(sp.start, sp.n); err == nil {
						pages = p
					}
				}
			}
			if pages == nil {
				for k := 0; k < sp.n; k++ {
					p, err := r.src.ReadPage(sp.start + uint32(k))
					if err != nil {
						break
					}
					pages = append(pages, p)
				}
			}
			resMu.Lock()
			for k, p := range pages {
				got = append(got, fetched{pgno: sp.start + uint32(k), data: p})
			}
			resMu.Unlock()
		}()
	}
	wg.Wait()

	mu.Lock()
	for _, f := range got {
		if r.prefetch == nil {
			r.prefetch = map[uint32][]byte{}
		}
		if r.prefetchBytes+len(f.data) > prefetchBudget {
			r.prefetch = map[uint32][]byte{}
			r.prefetchBytes = 0
		}
		r.prefetch[f.pgno] = f.data
		r.prefetchBytes += len(f.data)
		r.children[f.pgno] = struct{}{}
		r.prefetched++
	}
	mu.Unlock()
}

const (
	// scanAhead is celld's SCAN_AHEAD: children prefetched after a sequential fault.
	scanAhead = 64
	// maxChildren bounds the recorded child set.
	maxChildren = 256
	// prefetchBudget bounds the child prefetch cache (bytes).
	prefetchBudget = 4 << 20
)

// parseChildren extracts child page numbers from an interior b-tree page.
// Layout: page type at 0 (0x02 interior index, 0x05 interior table); the cell
// pointer array starts at the 12-byte header (offset 12; the 4-byte rightmost
// pointer lives at offset 8 for 0x02 and offset 12 for 0x05, i.e. the array
// starts at 16 for 0x05 and 12 for 0x02). Each interior-table cell starts with
// a 4-byte child; each interior-index cell starts with a 4-byte left child.
// Leaves (0x0d) and overflow (0x0a) name no children.
func parseChildren(page []byte) []uint32 {
	if len(page) < 12 {
		return nil
	}
	kind := page[0]
	if kind != 0x02 && kind != 0x05 {
		return nil
	}
	nCells := int(page[3])<<8 | int(page[4])
	// Interior pages have a 12-byte header: the 8-byte b-tree header followed by
	// the 4-byte rightmost-child pointer at offset 8; the cell pointer array
	// starts at offset 12 for both interior table and interior index pages.
	arr := 12
	rightmostAt := 8
	out := make([]uint32, 0, nCells+1)
	for i := 0; i < nCells; i++ {
		off := arr + 2*i
		if off+2 > len(page) {
			break
		}
		cellOff := int(page[off])<<8 | int(page[off+1])
		if cellOff < 0 || cellOff+4 > len(page) {
			continue
		}
		child := uint32(page[cellOff])<<24 | uint32(page[cellOff+1])<<16 |
			uint32(page[cellOff+2])<<8 | uint32(page[cellOff+3])
		if child > 0 {
			out = append(out, child)
		}
	}
	if rightmostAt+4 <= len(page) {
		child := uint32(page[rightmostAt])<<24 | uint32(page[rightmostAt+1])<<16 |
			uint32(page[rightmostAt+2])<<8 | uint32(page[rightmostAt+3])
		if child > 0 {
			out = append(out, child)
		}
	}
	return out
}

//export goPagedTruncated
func goPagedTruncated(id C.int, keepPages C.uint) {
	r := lookup(int(id))
	if r == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	keep := uint32(keepPages)
	if len(r.window) != 0 {
		if keep < r.windowBase {
			r.window = nil
			r.windowEnd = 0
		} else if i := int(keep-r.windowBase) + 1; i < len(r.window) {
			r.window = r.window[:i]
			r.windowEnd = r.windowBase + uint32(len(r.window)) - 1
		}
	}
	for p := range r.children {
		if p > keep {
			delete(r.children, p)
		}
	}
	for p := range r.prefetch {
		if p > keep {
			r.prefetchBytes -= len(r.prefetch[p])
			delete(r.prefetch, p)
		}
	}
	if r.lastChild > keep {
		r.lastChild = 0
	}
}

// HydrateAll faults every page of the cut into the local file. It must be run
// before anything reads the file outside SQLite (WAL capture snapshots,
// ReadDBPages, compaction) so those readers never see sparse zeros.
func HydrateAll(ctx context.Context, path string) error {
	if !Available() {
		return ErrUnavailable
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	r := func() *reg {
		mu.Lock()
		defer mu.Unlock()
		for _, rr := range regs {
			if rr.path == abs {
				return rr
			}
		}
		return nil
	}()
	if r == nil {
		return nil // not paged: nothing to do
	}
	ps, commit := r.src.PageSize(), r.src.Commit()
	f, err := os.OpenFile(abs, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	write := func(pgno int, page []byte) error {
		if len(page) != ps {
			return fmt.Errorf("pagedvfs: hydrate page %d: got %d bytes, want %d", pgno, len(page), ps)
		}
		if _, err := f.WriteAt(page, int64(pgno-1)*int64(ps)); err != nil {
			return err
		}
		C.cellhive_paged_mark(C.int(r.id), C.uint(pgno))
		return nil
	}
	hydrated := func(pgno int) bool {
		return C.cellhive_paged_hydrated(C.int(r.id), C.uint(pgno)) != 0
	}
	if rs, ok := r.src.(RunSource); ok {
		// Batched fill: one ranged read per window.
		for pgno := 1; pgno <= commit; {
			if err := ctx.Err(); err != nil {
				return err
			}
			if hydrated(pgno) {
				pgno++
				continue
			}
			runLen := windowLimit()
			if pgno+runLen-1 > commit {
				runLen = commit - pgno + 1
			}
			pages, err := rs.ReadRun(uint32(pgno), runLen)
			if err != nil || len(pages) == 0 {
				return fmt.Errorf("pagedvfs: hydrate run %d: %w", pgno, err)
			}
			for k, page := range pages {
				p := pgno + k
				if hydrated(p) {
					continue
				}
				if err := write(p, page); err != nil {
					return err
				}
			}
			pgno += len(pages)
		}
		return f.Sync()
	}
	for pgno := 1; pgno <= commit; pgno++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if hydrated(pgno) {
			continue
		}
		page, err := r.src.ReadPage(uint32(pgno))
		if err != nil {
			return fmt.Errorf("pagedvfs: hydrate page %d: %w", pgno, err)
		}
		if err := write(pgno, page); err != nil {
			return err
		}
	}
	return f.Sync()
}

// HydrateRate faults the remaining pages in the background at mbps (0 disables;
// one background hydrator runs per node at a time, as celld does). done is
// called with true when the file became complete. It returns a stop function.
func HydrateRate(path string, mbps int, done func(complete bool)) (stop func()) {
	if mbps <= 0 || !Available() {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		select {
		case semHydrate <- struct{}{}:
			defer func() { <-semHydrate }()
		case <-ctx.Done():
			return
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return
		}
		r := func() *reg {
			mu.Lock()
			defer mu.Unlock()
			for _, rr := range regs {
				if rr.path == abs {
					return rr
				}
			}
			return nil
		}()
		if r == nil {
			return
		}
		ps, commit := r.src.PageSize(), r.src.Commit()
		f, err := os.OpenFile(abs, os.O_WRONLY, 0o644)
		if err != nil {
			return
		}
		defer f.Close()
		defer func() { done(false) }()
		perSecond := mbps * 1024 * 1024
		start := time.Now()
		budget := 0
		for pgno := 1; pgno <= commit; {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if C.cellhive_paged_hydrated(C.int(r.id), C.uint(pgno)) != 0 {
				pgno++
				continue
			}
			runLen := windowLimit()
			if pgno+runLen-1 > commit {
				runLen = commit - pgno + 1
			}
			var pages [][]byte
			if rs, ok := r.src.(RunSource); ok {
				if got, err := rs.ReadRun(uint32(pgno), runLen); err == nil {
					pages = got
				}
			}
			if pages == nil {
				page, err := r.src.ReadPage(uint32(pgno))
				if err != nil {
					return
				}
				pages = [][]byte{page}
			}
			for k, page := range pages {
				p := pgno + k
				if C.cellhive_paged_hydrated(C.int(r.id), C.uint(p)) != 0 {
					continue
				}
				if _, err := f.WriteAt(page, int64(p-1)*int64(ps)); err != nil {
					return
				}
				C.cellhive_paged_mark(C.int(r.id), C.uint(p))
				budget += ps
			}
			pgno += len(pages)
			if budget >= perSecond/10 { // tick every ~100ms
				expected := time.Duration(float64(budget)/float64(perSecond)*float64(time.Second)) - time.Since(start)
				if expected > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(expected):
					}
				}
				start = time.Now()
				budget = 0
			}
		}
		_ = f.Sync()
		done(true)
	}()
	return func() { cancel(); <-finished }
}

var semHydrate = make(chan struct{}, 1)

// regID returns the registry id for a path (tests/diagnostics).
func regID(path string) (int, bool) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return 0, false
	}
	mu.Lock()
	defer mu.Unlock()
	for id, r := range regs {
		if r.path == abs {
			return id, true
		}
	}
	return 0, false
}

// faultPage drives the C fault path for one page and returns the bytes SQLite
// would receive (tests/diagnostics; keeps the C types inside this file).
func faultPage(path string, pgno uint32, pageSize int) ([]byte, bool) {
	id, ok := regID(path)
	if !ok || pageSize <= 0 {
		return nil, false
	}
	buf := make([]byte, pageSize)
	if goPagedFetch(C.int(id), C.uint(pgno), unsafe.Pointer(&buf[0]), C.int(pageSize)) != 0 {
		return nil, false
	}
	return buf, true
}

// HydratedCount returns how many pages of a registered cut are hydrated (a C
// bitmap popcount, cheap enough for metrics).
func HydratedCount(path string) int {
	id, ok := regID(path)
	if !ok {
		return 0
	}
	return int(C.cellhive_paged_hydrated_count(C.int(id)))
}

// Snapshot aggregates paged-cell state for /metrics.
type SnapshotStats struct {
	Cells         int
	Faults        uint64
	Runs          uint64
	PrefetchHits  uint64
	Prefetched    uint64
	HydratedPages int
	TotalPages    int
}

// SnapshotStatsAll reports aggregate paged stats.
func SnapshotStatsAll() SnapshotStats {
	mu.Lock()
	list := make([]*reg, 0, len(regs))
	for _, r := range regs {
		list = append(list, r)
	}
	mu.Unlock()
	var out SnapshotStats
	out.Cells = len(list)
	for _, r := range list {
		mu.Lock()
		out.Faults += r.faults
		out.Runs += r.runs
		out.PrefetchHits += r.prefetchHits
		out.Prefetched += r.prefetched
		mu.Unlock()
		out.TotalPages += r.src.Commit()
		out.HydratedPages += HydratedCount(r.path)
	}
	return out
}

// HydratedPages returns how many pages are hydrated (for tests/metrics).
func HydratedPages(path string) int {
	abs, err := filepath.Abs(path)
	if err != nil {
		return 0
	}
	mu.Lock()
	var r *reg
	for _, rr := range regs {
		if rr.path == abs {
			r = rr
		}
	}
	mu.Unlock()
	if r == nil {
		return 0
	}
	n := 0
	for pgno := 1; pgno <= r.src.Commit(); pgno++ {
		if C.cellhive_paged_hydrated(C.int(r.id), C.uint(pgno)) != 0 {
			n++
		}
	}
	return n
}

// sortedPaths is used by tests/diagnostics.
func sortedPaths() []string {
	mu.Lock()
	defer mu.Unlock()
	out := make([]string, 0, len(regs))
	for _, r := range regs {
		out = append(out, r.path)
	}
	sort.Strings(out)
	return out
}
