package cellstore

import (
	"context"
	"database/sql"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/pagedvfs"
)

// fileSource serves pages from a consistent on-disk image (a pinned cut),
// standing in for replica.PageFetcher in these tests.
type fileSource struct {
	pageSize int
	commit   int
	pages    map[uint32][]byte
}

func (f *fileSource) PageSize() int { return f.pageSize }
func (f *fileSource) Commit() int   { return f.commit }
func (f *fileSource) ReadPage(pgno uint32) ([]byte, error) {
	p, ok := f.pages[pgno]
	if !ok {
		return nil, pagedvfs.ErrUnavailable
	}
	return p, nil
}

func sourceFromImage(t *testing.T, path string) *fileSource {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read image: %v", err)
	}
	ps := int(binary.BigEndian.Uint16(data[16:18]))
	if ps == 1 {
		ps = 65536
	}
	if ps == 0 || len(data)%ps != 0 {
		t.Fatalf("bad image: page size %d, %d bytes", ps, len(data))
	}
	commit := len(data) / ps
	pages := make(map[uint32][]byte, commit)
	for i := 0; i < commit; i++ {
		p := make([]byte, ps)
		copy(p, data[i*ps:(i+1)*ps])
		pages[uint32(i+1)] = p
	}
	return &fileSource{pageSize: ps, commit: commit, pages: pages}
}

// TestPagedCellServesKVAndSnapshots is the cellstore-level ADR-160 guarantee: a
// cold cell with a Paged hook is served through the fault-in VFS (a point read
// faults pages instead of downloading the whole image), and SnapshotPages
// hydrates every page first so capture never encodes sparse zeros.
func TestPagedCellServesKVAndSnapshots(t *testing.T) {
	if !pagedvfs.Available() {
		t.Fatal("paged VFS unavailable")
	}
	ctx := context.Background()
	srcDir := t.TempDir()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}

	// Build the durable image: a cell with enough data to span many pages.
	src, err := New(srcDir)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	c, err := src.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	value := make([]byte, 900)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	for i := 0; i < 3000; i++ {
		key := "key-" + itoa(i)
		if err := c.Put(ctx, key, value, nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := c.CheckpointTruncate(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	imagePath := c.Path
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	source := sourceFromImage(t, imagePath)
	if source.commit < 100 {
		t.Fatalf("image too small: %d pages", source.commit)
	}

	// A fresh store hydrates the cell by paging.
	cold := filepath.Join(t.TempDir())
	dst, err := New(cold)
	if err != nil {
		t.Fatalf("cold store: %v", err)
	}
	dst.Paged = func(_ context.Context, sc cell.Scope, _ string) (pagedvfs.Source, bool, error) {
		return source, true, nil
	}
	dst.PagedHydrateMBPS = 0
	pc, err := dst.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("paged cell: %v", err)
	}
	if !pagedvfs.IsPaged(pc.Path) {
		t.Fatalf("cell %s is not registered with the paged VFS", pc.Path)
	}
	got, _, err := pc.Get(ctx, "key-42")
	if err != nil || string(got) != string(value) {
		t.Fatalf("paged get = %d bytes, %v; want the stored value", len(got), err)
	}
	faults := pagedvfs.Faults(pc.Path)
	if faults == 0 {
		t.Fatal("no page faulted: the cell did not open through the paged VFS")
	}
	if int(faults) >= source.commit {
		t.Fatalf("faulted %d of %d pages; a point read must not download the image", faults, source.commit)
	}

	// SnapshotPages must hydrate everything before reading the file directly.
	if _, commit, pages, err := pc.SnapshotPages(ctx); err != nil {
		t.Fatalf("snapshot pages: %v", err)
	} else if int(commit) != source.commit || len(pages) != source.commit {
		t.Fatalf("snapshot = %d/%d pages, want %d", commit, len(pages), source.commit)
	}
	if n := pagedvfs.HydratedPages(pc.Path); n != source.commit {
		t.Fatalf("hydrated %d of %d pages after SnapshotPages", n, source.commit)
	}

	// And the cell is writable through the VFS.
	if err := pc.Put(ctx, "new-key", []byte("fresh"), nil); err != nil {
		t.Fatalf("paged put: %v", err)
	}
	if v, _, err := pc.Get(ctx, "new-key"); err != nil || string(v) != "fresh" {
		t.Fatalf("paged read-back = %q, %v", v, err)
	}

	if err := pc.Close(); err != nil {
		t.Fatalf("close paged: %v", err)
	}
	if pagedvfs.IsPaged(pc.Path) {
		t.Fatal("paged registration survived Close")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

var _ = sql.ErrNoRows

// TestPagedCellReopenAfterClose: a paged cell's file is a cache. If the process
// releases the VFS registration (Close/eviction/restart) while the file is still
// sparse, reopening must not read it with the base VFS (that would return zeros).
// It must either re-register from the page source or discard the cache.
func TestPagedCellReopenAfterClose(t *testing.T) {
	if !pagedvfs.Available() {
		t.Fatal("paged VFS unavailable")
	}
	ctx := context.Background()
	srcDir := t.TempDir()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "reopen"}
	built, err := New(srcDir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := built.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	value := make([]byte, 900)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	for i := 0; i < 3000; i++ {
		if err := c.Put(ctx, "key-"+itoa(i), value, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.CheckpointTruncate(ctx); err != nil {
		t.Fatal(err)
	}
	imagePath := c.Path
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	source := sourceFromImage(t, imagePath)

	cold := filepath.Join(t.TempDir())
	st, err := New(cold)
	if err != nil {
		t.Fatal(err)
	}
	st.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return source, true, nil
	}
	st.PagedHydrateMBPS = 0
	pc, err := st.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := pc.Get(ctx, "key-7"); err != nil {
		t.Fatalf("first paged read: %v", err)
	}
	partial := pagedvfs.Faults(pc.Path)
	if partial == 0 || partial >= uint64(source.commit) {
		t.Fatalf("setup: faults = %d of %d pages", partial, source.commit)
	}
	partialPath := pc.Path
	// A process restart / eviction drops every registration and every handle;
	// the sparse file is still on disk.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(partialPath); err != nil {
		t.Fatalf("sparse cache file disappeared: %v", err)
	}
	if pagedvfs.IsPaged(partialPath) {
		t.Fatal("registration survived Store.Close")
	}

	// A fresh store over the same data dir must not read the sparse file with
	// the base VFS.
	st2, err := New(cold)
	if err != nil {
		t.Fatal(err)
	}
	st2.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		return source, true, nil
	}
	st2.PagedHydrateMBPS = 0
	pc2, err := st2.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, _, err := pc2.Get(ctx, "key-7")
	if err != nil || string(got) != string(value) {
		t.Fatalf("reopen get = %d bytes, %v; want the stored value (zeros => base VFS read a sparse file)", len(got), err)
	}
	if v, _, err := pc2.Get(ctx, "key-2999"); err != nil || string(v) != string(value) {
		t.Fatalf("reopen high key = %d bytes, %v", len(v), err)
	}
	if err := pc2.Close(); err != nil {
		t.Fatal(err)
	}
}
