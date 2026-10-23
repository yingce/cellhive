package cellstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"cellhive/internal/cell"
)

func openSnapshotStreamCell(t *testing.T) (*Cell, context.Context) {
	t.Helper()
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	c, err := s.Open(ctx, cell.Scope{Namespace: "n", Class: "__kv__", ID: "stream"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	for i := 0; i < 40; i++ {
		if err := c.Put(ctx, fmt.Sprintf("k%02d", i), bytes.Repeat([]byte{byte(i)}, 256), nil); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	return c, ctx
}

func TestSnapshotStreamCoversImageInOrderedParts(t *testing.T) {
	c, ctx := openSnapshotStreamCell(t)
	wantWatermark, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	// Reference image: the existing whole-file API, after a checkpoint because
	// SnapshotPages reads the database file and not the WAL.
	if err := c.CheckpointTruncate(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	pageSize, commit, pages, err := c.SnapshotPages(ctx)
	if err != nil {
		t.Fatalf("snapshot pages: %v", err)
	}

	var (
		gotPages   [][]byte
		firstPages []uint32
		gotTotal   int
	)
	// Two pages' worth of budget keeps the page size plus one page-map entry.
	partBytes := int(pageSize) + 1024
	watermark, streamPageSize, streamCommit, err := c.SnapshotStream(ctx, partBytes, func(partIndex, total int, wm uint64, ps, cm, firstPage uint32, part [][]byte) error {
		if partIndex != len(firstPages) {
			t.Fatalf("part index %d out of order (have %d parts)", partIndex, len(firstPages))
		}
		if total != gotTotal && len(firstPages) == 0 {
			gotTotal = total
		} else if total != gotTotal {
			t.Fatalf("part %d total = %d, want %d", partIndex, total, gotTotal)
		}
		if partIndex == 0 && wm != wantWatermark {
			t.Fatalf("callback watermark = %d, want %d", wm, wantWatermark)
		}
		firstPages = append(firstPages, firstPage)
		for _, p := range part {
			if len(p) != int(ps) {
				t.Fatalf("part %d page length = %d, want %d", partIndex, len(p), ps)
			}
			gotPages = append(gotPages, append([]byte(nil), p...))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot stream: %v", err)
	}
	if watermark != wantWatermark {
		t.Fatalf("watermark = %d, want %d", watermark, wantWatermark)
	}
	if streamPageSize != pageSize || streamCommit != commit {
		t.Fatalf("stream dims ps=%d commit=%d, want ps=%d commit=%d", streamPageSize, streamCommit, pageSize, commit)
	}
	if gotTotal < 2 {
		t.Fatalf("want a multi-part stream, got %d part(s)", gotTotal)
	}
	if len(gotPages) != int(commit) {
		t.Fatalf("streamed %d pages, want %d", len(gotPages), commit)
	}
	for i := range gotPages {
		want := pages[uint32(i+1)]
		if !bytes.Equal(gotPages[i], want) {
			t.Fatalf("page %d differs from SnapshotPages image", i+1)
		}
	}
	wantFirst := uint32(1)
	for _, first := range firstPages {
		if first != wantFirst {
			t.Fatalf("part first page = %d, want %d", first, wantFirst)
		}
		// Each part is one page whenever partBytes covers only one page plus
		// the fixed overhead; advance by the part's own size instead of
		// assuming it.
		wantFirst++
	}
}

func TestSnapshotStreamPagesAfterCheckpointMatchesStream(t *testing.T) {
	c, ctx := openSnapshotStreamCell(t)
	partBytes := 16 << 10
	stream := func(checkpoint bool) [][]byte {
		t.Helper()
		var out [][]byte
		collect := func(_ int, _ int, _ uint64, _ uint32, _ uint32, _ uint32, pages [][]byte) error {
			for _, p := range pages {
				out = append(out, append([]byte(nil), p...))
			}
			return nil
		}
		var err error
		if checkpoint {
			_, _, _, err = c.SnapshotStream(ctx, partBytes, collect)
		} else {
			_, _, _, err = c.SnapshotStreamPages(ctx, partBytes, collect)
		}
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		return out
	}
	// SnapshotStreamPages relies on the caller having checkpointed.
	if err := c.CheckpointTruncate(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	afterCheckpoint := stream(false)
	withCheckpoint := stream(true)
	if len(afterCheckpoint) != len(withCheckpoint) {
		t.Fatalf("page counts differ: %d vs %d", len(afterCheckpoint), len(withCheckpoint))
	}
	for i := range afterCheckpoint {
		if !bytes.Equal(afterCheckpoint[i], withCheckpoint[i]) {
			t.Fatalf("page %d differs between SnapshotStreamPages and SnapshotStream", i+1)
		}
	}
}

func TestSnapshotStreamRejectsTinyPartBudget(t *testing.T) {
	c, ctx := openSnapshotStreamCell(t)
	_, _, _, err := c.SnapshotStream(ctx, 100, func(int, int, uint64, uint32, uint32, uint32, [][]byte) error {
		t.Fatalf("callback must not run for an unusably small budget")
		return nil
	})
	if err == nil {
		t.Fatalf("want error for tiny part budget")
	}
}

func TestSnapshotStreamPropagatesCallbackError(t *testing.T) {
	c, ctx := openSnapshotStreamCell(t)
	boom := errors.New("boom")
	_, _, _, err := c.SnapshotStream(ctx, 16<<10, func(int, int, uint64, uint32, uint32, uint32, [][]byte) error {
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want wrapped %v", err, boom)
	}
}
