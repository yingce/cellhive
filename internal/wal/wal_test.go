package wal

import (
	"context"
	"fmt"
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
