package pagedvfs

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// fakeSource serves pages from an in-memory image (the pinned cut).
type fakeSource struct {
	pageSize int
	commit   int
	pages    map[uint32][]byte
}

func (f *fakeSource) PageSize() int { return f.pageSize }
func (f *fakeSource) Commit() int   { return f.commit }
func (f *fakeSource) ReadPage(pgno uint32) ([]byte, error) {
	p, ok := f.pages[pgno]
	if !ok {
		return nil, os.ErrNotExist
	}
	return p, nil
}

// buildSourceDB creates a real SQLite database with n rows and returns its bytes
// and page geometry.
func buildSourceDB(tb testing.TB, dir string, rows int) ([]byte, *fakeSource) {
	tb.Helper()
	path := filepath.Join(dir, "source.db")
	db, err := sql.Open("sqlite3", "file:"+path+"?_journal_mode=DELETE&_synchronous=FULL")
	if err != nil {
		tb.Fatalf("open source: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT)`); err != nil {
		tb.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		tb.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO t(id, body) VALUES(?,?)`)
	if err != nil {
		tb.Fatal(err)
	}
	body := ""
	for i := 0; i < 400; i++ {
		body += "x"
	}
	for i := 0; i < rows; i++ {
		if _, err := stmt.Exec(i+1, fmt.Sprintf("%05d-%s", i, body)); err != nil {
			tb.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
	_ = db.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	ps := int(binary.BigEndian.Uint16(data[16:18]))
	if ps == 1 {
		ps = 65536
	}
	if ps == 0 || len(data)%ps != 0 {
		tb.Fatalf("source image: page size %d, %d bytes", ps, len(data))
	}
	commit := len(data) / ps
	pages := make(map[uint32][]byte, commit)
	for i := 0; i < commit; i++ {
		p := make([]byte, ps)
		copy(p, data[i*ps:(i+1)*ps])
		pages[uint32(i+1)] = p
	}
	return data, &fakeSource{pageSize: ps, commit: commit, pages: pages}
}

// TestPagedOpenFaultsPagesOnDemand is the core ADR-160 guarantee: opening a
// sparse file through the VFS serves a query by fetching only the pages it
// touches, and HydrateAll makes the local file byte-identical to the source.
func TestPagedOpenFaultsPagesOnDemand(t *testing.T) {
	if !Available() {
		t.Fatal("paged VFS unavailable")
	}
	dir := t.TempDir()
	image, src := buildSourceDB(t, dir, 4000) // ~2 MiB, several hundred pages
	if src.commit < 100 {
		t.Fatalf("source too small: %d pages", src.commit)
	}

	paged := filepath.Join(dir, "paged.db")
	if err := Prepare(paged, src); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer Release(paged)

	db, err := sql.Open("sqlite3", "file:"+paged+"?vfs="+VFSName)
	if err != nil {
		t.Fatalf("open paged: %v", err)
	}
	defer db.Close()

	// A point read touches the header, the table root and a leaf page or two.
	var body string
	if err := db.QueryRow(`SELECT body FROM t WHERE id=?`, 1234).Scan(&body); err != nil {
		t.Fatalf("point read: %v", err)
	}
	faults := Faults(paged)
	if faults == 0 {
		t.Fatal("no page was faulted: the VFS did not wrap the main db")
	}
	if int(faults) >= src.commit {
		t.Fatalf("faulted %d pages of %d: expected far fewer than the whole image", faults, src.commit)
	}

	// A full scan still works (each page faulted at most once).
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 4000 {
		t.Fatalf("count = %d, want 4000", n)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Hydrate the rest and require a byte-identical image.
	if err := HydrateAll(context.Background(), paged); err != nil {
		t.Fatalf("hydrate all: %v", err)
	}
	if got := HydratedPages(paged); got != src.commit {
		t.Fatalf("hydrated %d pages, want %d", got, src.commit)
	}
	got, err := os.ReadFile(paged)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(image) {
		t.Fatalf("hydrated image = %d bytes, want %d", len(got), len(image))
	}
	for i := range image {
		if got[i] != image[i] {
			t.Fatalf("hydrated image differs from the source at byte %d", i)
		}
	}
}

// TestPagedWritesMarkHydrated covers the checkpoint path: pages SQLite writes
// are marked hydrated and are not re-faulted from the cut.
func TestPagedWritesMarkHydrated(t *testing.T) {
	if !Available() {
		t.Fatal("paged VFS unavailable")
	}
	dir := t.TempDir()
	_, src := buildSourceDB(t, dir, 200)
	paged := filepath.Join(dir, "paged.db")
	if err := Prepare(paged, src); err != nil {
		t.Fatal(err)
	}
	defer Release(paged)

	db, err := sql.Open("sqlite3", "file:"+paged+"?vfs="+VFSName+"&_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t(id, body) VALUES(9999, 'new-row')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var body string
	if err := db.QueryRow(`SELECT body FROM t WHERE id=9999`).Scan(&body); err != nil || body != "new-row" {
		t.Fatalf("read back = %q, %v", body, err)
	}
	// The write must have marked its pages hydrated.
	if HydratedPages(paged) == 0 {
		t.Fatal("no page marked hydrated after a write")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Hydration must not resurrect stale cut bytes over the new row.
	if err := HydrateAll(context.Background(), paged); err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	db2, err := sql.Open("sqlite3", "file:"+paged+"?vfs="+VFSName)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if err := db2.QueryRow(`SELECT body FROM t WHERE id=9999`).Scan(&body); err != nil || body != "new-row" {
		t.Fatalf("row after hydrate = %q, %v", body, err)
	}
}

// TestBackgroundHydrateFillsTheFile covers the optional background fill
// (ADR-160): with a rate set, the remaining pages materialize without any
// query, and the file ends fully hydrated.
func TestBackgroundHydrateFillsTheFile(t *testing.T) {
	if !Available() {
		t.Fatal("paged VFS unavailable")
	}
	dir := t.TempDir()
	image, src := buildSourceDB(t, dir, 1500) // ~1.2 MiB
	paged := filepath.Join(dir, "paged.db")
	if err := Prepare(paged, src); err != nil {
		t.Fatal(err)
	}
	defer Release(paged)
	stop := HydrateRate(paged, 8, func(bool) {}) // 8 MiB/s
	defer stop()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if HydratedPages(paged) == src.commit {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := HydratedPages(paged); got != src.commit {
		t.Fatalf("background hydrate filled %d of %d pages", got, src.commit)
	}
	got, err := os.ReadFile(paged)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(image) {
		t.Fatalf("hydrated size %d, want %d", len(got), len(image))
	}
	for i := range image {
		if got[i] != image[i] {
			t.Fatalf("background-hydrated image differs at byte %d", i)
		}
	}
}

// runFakeSource serves pages from memory and counts fetch calls, implementing the
// optional RunSource window interface.
type runFakeSource struct {
	*fakeSource
	mu        sync.Mutex
	readCalls int
	runCalls  int
}

func (r *runFakeSource) ReadPage(pgno uint32) ([]byte, error) {
	r.mu.Lock()
	r.readCalls++
	r.mu.Unlock()
	return r.fakeSource.ReadPage(pgno)
}

func (r *runFakeSource) ReadRun(pgno uint32, maxPages int) ([][]byte, error) {
	r.mu.Lock()
	r.runCalls++
	r.mu.Unlock()
	out := make([][]byte, 0, maxPages)
	for n := 0; n < maxPages; n++ {
		p, err := r.fakeSource.ReadPage(pgno + uint32(n))
		if err != nil {
			break
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, os.ErrNotExist
	}
	return out, nil
}

func (r *runFakeSource) calls() (reads, runs int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readCalls, r.runCalls
}

// TestRunPrefetchReducesBucketReads: a full scan must cost one fetch per window,
// not one per page (ADR-160 window prefetch).
func TestRunPrefetchReducesBucketReads(t *testing.T) {
	if !Available() {
		t.Fatal("paged VFS unavailable")
	}
	dir := t.TempDir()
	_, base := buildSourceDB(t, dir, 3000) // a few hundred pages
	src := &runFakeSource{fakeSource: base}
	paged := filepath.Join(dir, "paged.db")
	if err := Prepare(paged, src); err != nil {
		t.Fatal(err)
	}
	defer Release(paged)
	db, err := sql.Open("sqlite3", "file:"+paged+"?vfs="+VFSName)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 3000 {
		t.Fatalf("count = %d, want 3000", n)
	}
	reads, runs := src.calls()
	if runs == 0 {
		t.Fatal("no window read: RunSource was not used")
	}
	if want := src.commit / 4; runs > want {
		t.Fatalf("full scan used %d window reads for %d pages (want <= %d)", runs, src.commit, want)
	}
	if reads > src.commit/4 {
		t.Fatalf("full scan used %d single-page reads for %d pages", reads, src.commit)
	}
	t.Logf("scan: %d pages served by %d window reads + %d single reads", src.commit, runs, reads)
}

// interiorPage builds a synthetic interior b-tree page.
func interiorPage(kind byte, children []uint32) []byte {
	const ps = 512
	p := make([]byte, ps)
	p[0] = kind
	p[3], p[4] = byte(len(children)>>8), byte(len(children))
	arr := 12
	for i, c := range children {
		cellOff := 100 + i*8
		p[arr+2*i], p[arr+2*i+1] = byte(cellOff>>8), byte(cellOff)
		p[cellOff] = byte(c >> 24)
		p[cellOff+1] = byte(c >> 16)
		p[cellOff+2] = byte(c >> 8)
		p[cellOff+3] = byte(c)
	}
	// rightmost pointer at offset 8
	p[8], p[9], p[10], p[11] = 0, 0, 0, 9
	return p
}

// TestParseChildren covers the b-tree interior parser (both interior types).
func TestParseChildren(t *testing.T) {
	got := parseChildren(interiorPage(0x05, []uint32{3, 7}))
	want := []uint32{3, 7, 9}
	if len(got) != len(want) {
		t.Fatalf("interior table children = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("interior table children = %v, want %v", got, want)
		}
	}
	got = parseChildren(interiorPage(0x02, []uint32{4}))
	if len(got) != 2 || got[0] != 4 || got[1] != 9 {
		t.Fatalf("interior index children = %v, want [4 9]", got)
	}
	leaf := make([]byte, 512)
	leaf[0] = 0x0d
	if parseChildren(leaf) != nil {
		t.Fatal("leaf page reported children")
	}
	overflow := make([]byte, 512)
	overflow[0] = 0x0a
	if parseChildren(overflow) != nil {
		t.Fatal("overflow page reported children")
	}
}

// policySource records how each fault was served.
type policySource struct {
	pageSize int
	pages    map[uint32][]byte
	maxSeen  map[uint32]int // pgno -> maxPages requested by a run fetch
	singles  map[uint32]int // pgno -> single-page reads (faults + prefetch)
}

func (s *policySource) PageSize() int { return s.pageSize }
func (s *policySource) Commit() int   { return len(s.pages) }
func (s *policySource) ReadPage(pgno uint32) ([]byte, error) {
	s.singles[pgno]++
	p, ok := s.pages[pgno]
	if !ok {
		return nil, os.ErrNotExist
	}
	return p, nil
}
func (s *policySource) ReadRun(pgno uint32, maxPages int) ([][]byte, error) {
	if maxPages > s.maxSeen[pgno] {
		s.maxSeen[pgno] = maxPages
	}
	out := make([][]byte, 0, maxPages)
	for n := 0; n < maxPages; n++ {
		p, ok := s.pages[pgno+uint32(n)]
		if !ok {
			break
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, os.ErrNotExist
	}
	return out, nil
}

// TestFaultPolicyChildAloneAndWindow: an interior page names its children; a
// child fault fetches one page (never its overflow neighbours); a sequential
// child walks ahead into the prefetch cache; a sequential non-child fault
// windows.
func TestFaultPolicyChildAloneAndWindow(t *testing.T) {
	if !Available() {
		t.Fatal("paged VFS unavailable")
	}
	const ps = 512
	src := &policySource{pageSize: ps, pages: map[uint32][]byte{}, maxSeen: map[uint32]int{}, singles: map[uint32]int{}}
	for p := uint32(1); p <= 16; p++ {
		src.pages[p] = make([]byte, ps)
		src.pages[p][0] = 0x0d // leaf by default
	}
	// page 2: interior table naming children 3..8 (rightmost 9).
	src.pages[2] = interiorPage(0x05, []uint32{3, 4, 5, 6, 7, 8})

	dir := t.TempDir()
	paged := filepath.Join(dir, "policy.db")
	if err := Prepare(paged, src); err != nil {
		t.Fatal(err)
	}
	defer Release(paged)

	if _, ok := faultPage(paged, 2, ps); !ok {
		t.Fatal("fault page 2 failed")
	}
	// A child fault must fetch exactly one page.
	if _, ok := faultPage(paged, 3, ps); !ok {
		t.Fatal("fault page 3 failed")
	}
	if src.maxSeen[3] != 0 || src.singles[3] != 1 {
		t.Fatalf("child fault should be one single-page read: run(maxPages)=%d singles=%d",
			src.maxSeen[3], src.singles[3])
	}
	// The next child (sequential walk) triggers the ahead prefetch of 5..9.
	if _, ok := faultPage(paged, 4, ps); !ok {
		t.Fatal("fault page 4 failed")
	}
	hits, prefetched := Prefetch(paged)
	if prefetched < 4 {
		t.Fatalf("prefetched %d pages, want the next children (5..9)", prefetched)
	}
	// A prefetched child is served from memory: no new source read.
	before := src.singles[5]
	if _, ok := faultPage(paged, 5, ps); !ok {
		t.Fatal("fault page 5 failed")
	}
	if src.singles[5] != before {
		t.Fatalf("prefetched child was fetched again (singles %d -> %d)", before, src.singles[5])
	}
	if hits2, _ := Prefetch(paged); hits2 <= hits {
		t.Fatalf("prefetch hits did not increase: %d -> %d", hits, hits2)
	}
	// A sequential non-child fault windows (page 11 then 12).
	if _, ok := faultPage(paged, 11, ps); !ok {
		t.Fatal("fault page 11 failed")
	}
	if _, ok := faultPage(paged, 12, ps); !ok {
		t.Fatal("fault page 12 failed")
	}
	if got := src.maxSeen[12]; got != windowLimit() {
		t.Fatalf("sequential non-child fault requested maxPages=%d, want %d", got, windowLimit())
	}
}

// TestHydrateUsesRuns: the bulk hydrate must batch its reads.
func TestHydrateUsesRuns(t *testing.T) {
	if !Available() {
		t.Fatal("paged VFS unavailable")
	}
	dir := t.TempDir()
	_, base := buildSourceDB(t, dir, 3000)
	src := &runFakeSource{fakeSource: base}
	paged := filepath.Join(dir, "hydrate.db")
	if err := Prepare(paged, src); err != nil {
		t.Fatal(err)
	}
	defer Release(paged)
	if err := HydrateAll(context.Background(), paged); err != nil {
		t.Fatal(err)
	}
	if got := HydratedPages(paged); got != src.commit {
		t.Fatalf("hydrated %d of %d pages", got, src.commit)
	}
	_, runs := src.calls()
	if want := src.commit/4 + 1; runs > want {
		t.Fatalf("hydrate used %d fetch calls for %d pages (want <= %d)", runs, src.commit, want)
	}
}

// slowSource adds latency to each read and tracks peak concurrency.
type slowSource struct {
	*sync.Mutex
	pageSize int
	pages    map[uint32][]byte
	delay    time.Duration
	inflight int
	maxSeen  int
}

func newSlowSource(pageSize int, pages map[uint32][]byte, delay time.Duration) *slowSource {
	return &slowSource{Mutex: &sync.Mutex{}, pageSize: pageSize, pages: pages, delay: delay}
}

func (s *slowSource) PageSize() int { return s.pageSize }
func (s *slowSource) Commit() int   { return len(s.pages) }
func (s *slowSource) enter() {
	s.Lock()
	s.inflight++
	if s.inflight > s.maxSeen {
		s.maxSeen = s.inflight
	}
	s.Unlock()
}
func (s *slowSource) exit() {
	s.Lock()
	s.inflight--
	s.Unlock()
}
func (s *slowSource) ReadPage(pgno uint32) ([]byte, error) {
	s.enter()
	defer s.exit()
	time.Sleep(s.delay)
	p, ok := s.pages[pgno]
	if !ok {
		return nil, os.ErrNotExist
	}
	return p, nil
}
func (s *slowSource) ReadRun(pgno uint32, maxPages int) ([][]byte, error) {
	s.enter()
	defer s.exit()
	time.Sleep(s.delay)
	out := make([][]byte, 0, maxPages)
	for n := 0; n < maxPages; n++ {
		p, ok := s.pages[pgno+uint32(n)]
		if !ok {
			break
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, os.ErrNotExist
	}
	return out, nil
}
func (s *slowSource) peak() int {
	s.Lock()
	defer s.Unlock()
	return s.maxSeen
}

// TestChildPrefetchIsConcurrent: prefetching a scattered child set must use the
// worker pool (celld's PREFETCH_WORKERS), not one blocking read at a time.
func TestChildPrefetchIsConcurrent(t *testing.T) {
	if !Available() {
		t.Fatal("paged VFS unavailable")
	}
	const ps = 512
	pages := map[uint32][]byte{}
	children := []uint32{3, 5, 7, 9, 11, 13, 15, 17}
	for p := uint32(1); p <= 20; p++ {
		pages[p] = make([]byte, ps)
		pages[p][0] = 0x0d
	}
	pages[2] = interiorPage(0x05, children)
	src := newSlowSource(ps, pages, 15*time.Millisecond)
	dir := t.TempDir()
	paged := filepath.Join(dir, "conc.db")
	if err := Prepare(paged, src); err != nil {
		t.Fatal(err)
	}
	defer Release(paged)

	if _, ok := faultPage(paged, 2, ps); !ok {
		t.Fatal("interior fault failed")
	}
	if _, ok := faultPage(paged, 3, ps); !ok {
		t.Fatal("child 3 fault failed")
	}
	start := time.Now()
	if _, ok := faultPage(paged, 5, ps); !ok { // sequential child -> prefetch 7..17
		t.Fatal("child 5 fault failed")
	}
	elapsed := time.Since(start)
	peak := src.peak()
	if peak < 2 {
		t.Fatalf("child prefetch peak concurrency = %d, want >= 2", peak)
	}
	// Six scattered children at 15ms each: sequential would be ~90ms; four
	// workers make it about two rounds.
	if elapsed > 70*time.Millisecond {
		t.Fatalf("prefetch of 6 children took %v (peak %d): not concurrent", elapsed, peak)
	}
	t.Logf("prefetch: peak=%d elapsed=%v for 6 scattered children", peak, elapsed)
}

// BenchmarkPagedScanSQLite measures a full table scan through the fault-in VFS
// with an in-memory page source (VFS policy + bookkeeping overhead).
func BenchmarkPagedScanSQLite(b *testing.B) {
	if !Available() {
		b.Fatal("paged VFS unavailable")
	}
	dir := b.TempDir()
	_, src := buildSourceDB(b, dir, 20000)
	paged := filepath.Join(dir, "bench.db")
	if err := Prepare(paged, src); err != nil {
		b.Fatal(err)
	}
	defer Release(paged)
	db, err := sql.Open("sqlite3", "file:"+paged+"?vfs="+VFSName)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
			b.Fatal(err)
		}
		if n != 20000 {
			b.Fatalf("count = %d", n)
		}
	}
}
