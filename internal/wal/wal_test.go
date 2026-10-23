package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

func openCell(t *testing.T) (*cellstore.Cell, context.Context) {
	t.Helper()
	ctx := context.Background()
	s, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	c, err := s.Open(ctx, cell.Scope{Namespace: "n", Class: "__kv__", ID: "a"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, ctx
}

func TestReadCommittedFrames(t *testing.T) {
	c, ctx := openCell(t)
	for i := 0; i < 5; i++ {
		if err := c.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	snap, err := Read(c.WALPath())
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	if snap.CommitCount < 5 {
		t.Fatalf("commit count = %d, want >= 5", snap.CommitCount)
	}
	if len(snap.Frames) == 0 {
		t.Fatalf("no committed frames")
	}
	if snap.Header.PageSize == 0 {
		t.Fatalf("page size not parsed")
	}
}

func TestCursorDetectsNewCommits(t *testing.T) {
	c, ctx := openCell(t)
	cur := NewCursor()

	// Consume initial state.
	if _, err := cur.Poll(c.WALPath()); err != nil {
		t.Fatalf("poll 1: %v", err)
	}

	// New writes must surface as committed frames.
	if err := c.Put(ctx, "later", []byte("x"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	res, err := cur.Poll(c.WALPath())
	if err != nil {
		t.Fatalf("poll 2: %v", err)
	}
	if len(res.NewCommitted) == 0 {
		t.Fatalf("expected new committed frames after write")
	}
}

func TestCursorGroupsTransactions(t *testing.T) {
	c, ctx := openCell(t)
	cur := NewCursor()
	if _, err := cur.Poll(c.WALPath()); err != nil {
		t.Fatalf("baseline poll: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := c.PutTx(ctx, fmt.Sprintf("tx-%d", i), []byte("v"), nil); err != nil {
			t.Fatalf("put tx %d: %v", i, err)
		}
	}
	res, err := cur.Poll(c.WALPath())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(res.Transactions) != 3 {
		t.Fatalf("transactions = %d, want 3", len(res.Transactions))
	}
	flattened := 0
	for i, tx := range res.Transactions {
		if len(tx.Frames) == 0 {
			t.Fatalf("transaction %d has no frames", i)
		}
		last := tx.Frames[len(tx.Frames)-1]
		if last.DBSize == 0 || tx.DBSize != last.DBSize {
			t.Fatalf("transaction %d missing commit marker", i)
		}
		flattened += len(tx.Frames)
	}
	if flattened != len(res.NewCommitted) {
		t.Fatalf("flattened frames = %d, NewCommitted = %d", flattened, len(res.NewCommitted))
	}
}

func TestCursorReadsOnlyNewWALTail(t *testing.T) {
	c, ctx := openCell(t)
	for i := 0; i < 100; i++ {
		if _, err := c.PutTx(ctx, fmt.Sprintf("baseline-%d", i), []byte("value"), nil); err != nil {
			t.Fatalf("baseline put: %v", err)
		}
	}
	cur := NewCursor()
	defer cur.Close()
	baseline, err := cur.Poll(c.WALPath())
	if err != nil {
		t.Fatalf("baseline poll: %v", err)
	}
	before := cur.BytesRead()
	if _, err := c.PutTx(ctx, "new", []byte("value"), nil); err != nil {
		t.Fatalf("new put: %v", err)
	}
	res, err := cur.Poll(c.WALPath())
	if err != nil {
		t.Fatalf("incremental poll: %v", err)
	}
	if len(res.Transactions) != 1 {
		t.Fatalf("transactions = %d, want 1", len(res.Transactions))
	}
	delta := cur.BytesRead() - before
	frameSize := uint64(baseline.Header.PageSize + frameHdrSize)
	if delta > 4*frameSize {
		t.Fatalf("incremental bytes = %d, want <= %d", delta, 4*frameSize)
	}
}

func TestCursorDetectsCheckpoint(t *testing.T) {
	c, ctx := openCell(t)
	_ = c.Put(ctx, "a", []byte("1"), nil)

	cur := NewCursor()
	if _, err := cur.Poll(c.WALPath()); err != nil {
		t.Fatalf("initial poll: %v", err)
	}

	// Force a checkpoint (truncates/rotates the WAL).
	if err := c.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	res, err := cur.Poll(c.WALPath())
	if err != nil {
		t.Fatalf("poll after checkpoint: %v", err)
	}
	if !res.Checkpoint {
		t.Fatalf("checkpoint not detected (salt/truncation)")
	}
}

func TestReadRejectsCorruptCommitFrame(t *testing.T) {
	c, ctx := openCell(t)
	if err := c.Put(ctx, "k", []byte("v"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	data, err := os.ReadFile(c.WALPath())
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	h, err := parseHeader(data[:walHeaderSize])
	if err != nil {
		t.Fatalf("header: %v", err)
	}
	frameSize := int(frameHdrSize) + int(h.PageSize)
	total := (len(data) - walHeaderSize) / frameSize
	if total == 0 {
		t.Fatal("no frames written")
	}
	// Flip every page byte of the last frame while leaving its pgno/dbsize and
	// stored checksum untouched. A checksum-validating reader must reject it.
	off := walHeaderSize + (total-1)*frameSize + frameHdrSize
	for i := off; i < off+int(h.PageSize); i++ {
		data[i] ^= 0xff
	}
	path := filepath.Join(t.TempDir(), "torn-wal")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(path); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Read(corrupt) error = %v, want ErrChecksum", err)
	}
}

func TestCursorReopensReplacedWAL(t *testing.T) {
	dir := t.TempDir()
	store, err := cellstore.New(dir)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "reopen"}
	c, err := store.Open(context.Background(), sc)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := c.Put(context.Background(), "a", []byte("1"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	cur := NewCursor()
	if _, err := cur.Poll(c.WALPath()); err != nil {
		t.Fatalf("baseline poll: %v", err)
	}
	walPath := c.WALPath()
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Simulate SQLite unlinking the WAL on last-connection close, then a new
	// process recreating the same path.
	_ = os.Remove(walPath)
	c2, err := store.Open(context.Background(), sc)
	if err != nil {
		t.Fatalf("reopen cell: %v", err)
	}
	defer c2.Close()
	for i := 0; i < 3; i++ {
		if err := c2.Put(context.Background(), fmt.Sprintf("b%d", i), []byte("2"), nil); err != nil {
			t.Fatalf("put after reopen: %v", err)
		}
	}
	res, err := cur.Poll(c2.WALPath())
	if err != nil {
		t.Fatalf("poll replaced WAL: %v", err)
	}
	if len(res.Transactions) == 0 {
		t.Fatal("cursor stayed on the unlinked inode: no transactions after WAL replacement")
	}
}
