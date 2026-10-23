package sqlcapture

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
)

// openStreamCell opens one cell and returns its page size so a test can pick a
// snapshot part budget that yields several parts.
func openStreamCell(t *testing.T, ctx context.Context, id string, rows int) (*cellstore.Cell, cell.Scope, int) {
	t.Helper()
	s, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: id}
	db, err := s.Open(ctx, scope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	var pageSize int
	if err := db.DB.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
		t.Fatalf("page size: %v", err)
	}
	for i := 0; i < rows; i++ {
		if _, err := db.PutTx(ctx, fmt.Sprintf("k%02d", i), bytes.Repeat([]byte{byte(i)}, 512), nil); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	return db, scope, pageSize
}

func newCapture(t *testing.T, db *cellstore.Cell, scope cell.Scope, committer Committer) (*Capture, uint64) {
	t.Helper()
	baseline, err := db.TxID(context.Background())
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	capture, err := New(db, scope, 1, baseline, committer)
	if err != nil {
		t.Fatalf("new capture: %v", err)
	}
	return capture, baseline
}

// decodeSnapshotParts decodes a streamed part set and validates the part
// contract: one KindSnapshot watermark, paged indices 0..n-1 in order, identical
// dimensions, and a contiguous 1..commit page cover.
func decodeSnapshotParts(t *testing.T, segments [][]byte, watermark uint64) (pageSize, commit uint32) {
	t.Helper()
	nextPage := uint32(1)
	for i, segment := range segments {
		h, payload, err := ltx.Decode(segment)
		if err != nil {
			t.Fatalf("decode part %d: %v", i, err)
		}
		if h.Kind != ltx.KindSnapshot || h.Epoch != 1 {
			t.Fatalf("part %d header %+v", i, h)
		}
		if h.StartTxID != watermark || h.EndTxID != watermark {
			t.Fatalf("part %d watermark %d..%d, want %d", i, h.StartTxID, h.EndTxID, watermark)
		}
		part, paged := ltx.SnapshotPart(h)
		if !paged || part != i {
			t.Fatalf("part %d: paged=%v index=%d", i, paged, part)
		}
		ps, cm, txCount, pages, err := ltx.DecodeWALPageMap(payload)
		if err != nil {
			t.Fatalf("decode part %d payload: %v", i, err)
		}
		if txCount != 1 {
			t.Fatalf("part %d tx count = %d, want 1", i, txCount)
		}
		if i == 0 {
			pageSize, commit = ps, cm
		} else if ps != pageSize || cm != commit {
			t.Fatalf("part %d dims ps=%d commit=%d, want ps=%d commit=%d", i, ps, cm, pageSize, commit)
		}
		if len(pages) == 0 || pages[0].PageNo != nextPage {
			t.Fatalf("part %d starts at page %d, want %d", i, pages[0].PageNo, nextPage)
		}
		for _, p := range pages {
			if p.PageNo != nextPage {
				t.Fatalf("part %d page %d, want %d", i, p.PageNo, nextPage)
			}
			if len(p.Data) != int(pageSize) {
				t.Fatalf("part %d page %d length %d, want %d", i, p.PageNo, len(p.Data), pageSize)
			}
			nextPage++
		}
	}
	if nextPage != commit+1 {
		t.Fatalf("parts cover through page %d, want %d", nextPage-1, commit)
	}
	return pageSize, commit
}

func assertRestored(t *testing.T, ctx context.Context, dir string, segments [][]byte, rows int) {
	t.Helper()
	dest := filepath.Join(dir, "restored.db")
	if _, err := restore.ApplyFile(dest, segments); err != nil {
		t.Fatalf("restore apply: %v", err)
	}
	restored, err := cellstore.OpenAt(ctx, dest)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()
	for i := 0; i < rows; i++ {
		v, _, err := restored.Get(ctx, fmt.Sprintf("k%02d", i))
		if err != nil {
			t.Fatalf("restored k%02d: %v", i, err)
		}
		if !bytes.Equal(v, bytes.Repeat([]byte{byte(i)}, 512)) {
			t.Fatalf("restored k%02d has unexpected value", i)
		}
	}
	var ic string
	if err := restored.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&ic); err != nil || ic != "ok" {
		t.Fatalf("integrity = %q, %v", ic, err)
	}
}

func TestSnapshotStreamsMultiplePartsAndRestores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, err := cellstore.New(dir)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "stream"}
	db, err := s.Open(ctx, scope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	var pageSizeHint int
	if err := db.DB.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSizeHint); err != nil {
		t.Fatalf("page size: %v", err)
	}
	const rows = 12
	for i := 0; i < rows; i++ {
		if _, err := db.PutTx(ctx, fmt.Sprintf("k%02d", i), bytes.Repeat([]byte{byte(i)}, 512), nil); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	wantWatermark, err := db.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	committer := &recordingCommitter{}
	capture, _ := newCapture(t, db, scope, committer)
	// One page plus a little overhead per part: a small database is still split.
	capture.SnapshotPartBytes = pageSizeHint + 1024
	if err := capture.Snapshot(ctx); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	committer.mu.Lock()
	segments := append([][]byte(nil), committer.segments...)
	committer.mu.Unlock()
	if len(segments) < 2 {
		t.Fatalf("want a multi-part snapshot, got %d part(s)", len(segments))
	}
	for i, segment := range segments {
		if len(segment) > capture.SnapshotPartBytes {
			t.Fatalf("part %d size %d exceeds part budget %d", i, len(segment), capture.SnapshotPartBytes)
		}
	}
	pageSize, commit := decodeSnapshotParts(t, segments, wantWatermark)
	if pageSize == 0 || commit < 2 {
		t.Fatalf("streamed dims ps=%d commit=%d", pageSize, commit)
	}
	stats := capture.Stats()
	if stats.Snapshots != 1 || stats.LTXBinaryBytes == 0 {
		t.Fatalf("stats after snapshot: %+v", stats)
	}
	if capture.nextTxID != wantWatermark || capture.barrier.durable() != wantWatermark {
		t.Fatalf("capture anchor txid=%d durable=%d, want %d", capture.nextTxID, capture.barrier.durable(), wantWatermark)
	}

	// A paged snapshot must chain with later deltas exactly like an unpaged one.
	capture.Start(ctx)
	const extra = 3
	for i := 0; i < extra; i++ {
		key := fmt.Sprintf("k%02d", rows+i)
		txid, err := db.PutTx(ctx, key, bytes.Repeat([]byte{byte(rows + i)}, 512), nil)
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		capture.Notify()
		if err := capture.Wait(ctx, txid); err != nil {
			t.Fatalf("wait %s: %v", key, err)
		}
	}
	cancel()
	committer.mu.Lock()
	chain := append([][]byte(nil), committer.segments...)
	committer.mu.Unlock()
	assertRestored(t, context.Background(), dir, chain, rows+extra)
}

func TestSnapshotSinglePartStaysUnpaged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, scope, _ := openStreamCell(t, ctx, "unpaged", 5)
	wantWatermark, err := db.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	committer := &recordingCommitter{}
	capture, _ := newCapture(t, db, scope, committer)
	if err := capture.Snapshot(ctx); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	committer.mu.Lock()
	segments := append([][]byte(nil), committer.segments...)
	committer.mu.Unlock()
	if len(segments) != 1 {
		t.Fatalf("want 1 part for a small database, got %d", len(segments))
	}
	h, _, err := ltx.Decode(segments[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if part, paged := ltx.SnapshotPart(h); paged {
		t.Fatalf("single-part streamed snapshot must stay unpaged (part %d)", part)
	}
	if h.StartTxID != wantWatermark || h.EndTxID != wantWatermark {
		t.Fatalf("watermark %d..%d, want %d", h.StartTxID, h.EndTxID, wantWatermark)
	}
}

func TestSnapshotRejectsOverBudgetBeforeReading(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, scope, _ := openStreamCell(t, ctx, "budget", 5)
	info, err := os.Stat(db.Path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	committer := &recordingCommitter{}
	capture, baseline := newCapture(t, db, scope, committer)
	capture.SnapshotMaxBytes = info.Size() - 1
	err = capture.Snapshot(ctx)
	if !errors.Is(err, ErrSnapshotTooLarge) {
		t.Fatalf("err = %v, want ErrSnapshotTooLarge", err)
	}
	committer.mu.Lock()
	committed := len(committer.segments)
	committer.mu.Unlock()
	if committed != 0 {
		t.Fatalf("committed %d parts over budget", committed)
	}
	if capture.nextTxID != baseline || capture.barrier.durable() != baseline {
		t.Fatalf("snapshot advanced anchor to %d/%d after over-budget failure", capture.nextTxID, capture.barrier.durable())
	}
	if snapshots := capture.Stats().Snapshots; snapshots != 0 {
		t.Fatalf("snapshots stats advanced to %d after over-budget failure", snapshots)
	}
}

// TestSnapshotRejectsImageGrownByCheckpoint covers the exact budget check: the
// database file fits the budget before the snapshot, but the checkpoint
// backfills WAL pages and the image no longer does. Nothing may be committed.
func TestSnapshotRejectsImageGrownByCheckpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, err := cellstore.New(dir)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "grow"}
	db, err := s.Open(ctx, scope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	// Keep the WAL under SQLite's auto-checkpoint threshold so the growth only
	// happens in SnapshotStream's own checkpoint.
	const rows = 100
	for i := 0; i < rows; i++ {
		if _, err := db.PutTx(ctx, fmt.Sprintf("k%03d", i), bytes.Repeat([]byte{byte(i)}, 2048), nil); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	info, err := os.Stat(db.Path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	committer := &recordingCommitter{}
	capture, baseline := newCapture(t, db, scope, committer)
	capture.SnapshotMaxBytes = info.Size() // file fits; checkpointed image should not
	err = capture.Snapshot(ctx)
	if !errors.Is(err, ErrSnapshotTooLarge) {
		t.Fatalf("err = %v, want ErrSnapshotTooLarge", err)
	}
	committer.mu.Lock()
	committed := len(committer.segments)
	committer.mu.Unlock()
	if committed != 0 {
		t.Fatalf("committed %d parts over budget", committed)
	}
	if capture.nextTxID != baseline {
		t.Fatalf("nextTxID advanced to %d, want %d", capture.nextTxID, baseline)
	}
}

// batchRecordingCommitter implements both Committer and BatchCommitter so the
// optional batching path is exercised.
type batchRecordingCommitter struct {
	mu      sync.Mutex
	singles int
	batches [][][]byte
}

func (c *batchRecordingCommitter) Commit(_ context.Context, _ cell.Scope, _ uint64, _ []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.singles++
	return nil
}

func (c *batchRecordingCommitter) CommitBatch(_ context.Context, _ cell.Scope, _ uint64, segments [][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	batch := make([][]byte, len(segments))
	for i, segment := range segments {
		batch[i] = append([]byte(nil), segment...)
	}
	c.batches = append(c.batches, batch)
	return nil
}

func TestSnapshotBatchCommitsGroupsParts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	s, err := cellstore.New(dir)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "batch"}
	db, err := s.Open(ctx, scope)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	var pageSizeHint int
	if err := db.DB.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSizeHint); err != nil {
		t.Fatalf("page size: %v", err)
	}
	const rows = 12
	for i := 0; i < rows; i++ {
		if _, err := db.PutTx(ctx, fmt.Sprintf("k%02d", i), bytes.Repeat([]byte{byte(i)}, 512), nil); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	wantWatermark, err := db.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	committer := &batchRecordingCommitter{}
	capture, _ := newCapture(t, db, scope, committer)
	capture.SnapshotPartBytes = pageSizeHint + 1024
	capture.SnapshotBatchParts = 2
	if err := capture.Snapshot(ctx); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	if committer.singles != 0 {
		t.Fatalf("batched snapshot still issued %d single commits", committer.singles)
	}
	if len(committer.batches) < 2 {
		t.Fatalf("want several batches, got %d", len(committer.batches))
	}
	var segments [][]byte
	for i, batch := range committer.batches {
		if len(batch) > 2 {
			t.Fatalf("batch %d has %d parts, want at most 2", i, len(batch))
		}
		segments = append(segments, batch...)
	}
	if len(segments) < 2 {
		t.Fatalf("batched snapshot produced %d parts, want a paged snapshot", len(segments))
	}
	decodeSnapshotParts(t, segments, wantWatermark)
	assertRestored(t, ctx, dir, segments, rows)
}
