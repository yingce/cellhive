package vectorize

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"cellhive/internal/restore"
)

// recordingCommitter collects the LTX segment chain a capture loop produces.
type recordingCommitter struct {
	mu   sync.Mutex
	segs [][]byte
}

func (r *recordingCommitter) Commit(_ context.Context, _ cell.Scope, _ uint64, segment []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.segs = append(r.segs, append([]byte(nil), segment...))
	return nil
}

func (r *recordingCommitter) chain() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.segs))
	copy(out, r.segs)
	return out
}

// TestVec1CellReplicatesThroughLTX is the end-to-end replication guarantee for
// vec1-backed indexes (ADR-159): a vectorize cell (vectors + trained ANN model +
// vec1 shadow tables) is captured as LTX segments, restored from that chain into
// a fresh cellstore, and answers the same queries with the same model.
//
// It deliberately writes before and after the baseline snapshot so both the
// snapshot and the delta path carry vec1 pages (shadow tables + overflow pages).
func TestVec1CellReplicatesThroughLTX(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	src, err := cellstore.New(srcDir)
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer src.Close()
	store := New(src)
	cfg := Config{Dimensions: 8, Metric: "euclidean"}

	mk := func(i int) []float32 {
		v := make([]float32, 8)
		for j := range v {
			v[j] = float32((i*7+j*13)%1009) / 1009
		}
		return v
	}
	// Pre-snapshot writes: 200 vectors, then train the ANN model.
	var pre []Vector
	for i := 0; i < 200; i++ {
		pre = append(pre, Vector{ID: "v" + itoa(i), Values: mk(i), Metadata: []byte(`{"bucket":` + itoa(i%5) + `}`)})
	}
	if _, err := store.Upsert(ctx, "acme", "idx", cfg, pre, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BuildANN(ctx, "acme", "idx", cfg, ANNOptions{Buckets: 16, Quantizer: "none", CodeSize: 0, NProbe: 16}); err != nil {
		t.Fatalf("BuildANN: %v", err)
	}

	// Capture: the first Ensure emits the baseline snapshot of the ANN index.
	segs := &recordingCommitter{}
	m := &cellcapture.Manager{
		Store: src, Committer: segs, AutoSnapshot: true,
		Checkpoint: 20 * time.Millisecond,
		Owner:      func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
	}
	scope := Scope("acme", "idx")
	if _, err := m.Ensure(ctx, scope); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	defer m.Stop()

	// Post-snapshot writes (deltas over the trained index).
	var post []Vector
	for i := 200; i < 220; i++ {
		post = append(post, Vector{ID: "v" + itoa(i), Values: mk(i)})
	}
	if _, err := store.Upsert(ctx, "acme", "idx", cfg, post, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteByIds(ctx, "acme", "idx", []string{"v0", "v1"}); err != nil {
		t.Fatal(err)
	}

	c, err := src.Cell(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	txid, err := c.TxID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := m.Wait(wctx, scope, txid); err != nil {
		t.Fatalf("wait for capture: %v", err)
	}
	m.Stop()
	chain := segs.chain()
	if len(chain) < 2 {
		t.Fatalf("chain has %d segments, want a snapshot plus deltas", len(chain))
	}

	// Restore the chain into a fresh cellstore layout and query through the store.
	dstDir := t.TempDir()
	dest := filepath.Join(dstDir, "cells", "acme", "__vectorize__", "idx.db")
	if _, err := restore.ApplyFile(dest, chain); err != nil {
		t.Fatalf("restore apply: %v", err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("restored file: %v", err)
	}
	restored, err := cellstore.New(dstDir)
	if err != nil {
		t.Fatalf("restored cellstore: %v", err)
	}
	defer restored.Close()
	rc, err := restored.Cell(ctx, scope)
	if err != nil {
		t.Fatalf("restored cell: %v", err)
	}
	// Sanity: the restored image still carries the vec1 module wiring.
	var info string
	if err := rc.DB.QueryRowContext(ctx, `SELECT vec1_info()`).Scan(&info); err != nil {
		t.Fatalf("vec1 info after restore: %v", err)
	}

	rstore := New(restored)
	d, err := rstore.Describe(ctx, "acme", "idx", cfg)
	if err != nil {
		t.Fatalf("describe restored: %v", err)
	}
	if d.VectorCount != 218 { // 220 inserted - 2 deleted
		t.Fatalf("restored vector count = %d, want 218", d.VectorCount)
	}
	if d.ANN == nil {
		t.Fatalf("ANN model did not survive replication: %+v", d)
	}
	// Same query, same answers as the source (ANN on both sides, nprobe covers
	// every bucket so the result is exact).
	q := Query{Vector: mk(42), TopK: 10, ReturnMetadata: "all"}
	want, err := store.Query(ctx, "acme", "idx", cfg, q)
	if err != nil {
		t.Fatalf("source query: %v", err)
	}
	got, err := rstore.Query(ctx, "acme", "idx", cfg, q)
	if err != nil {
		t.Fatalf("restored query: %v", err)
	}
	if len(got.Matches) != len(want.Matches) {
		t.Fatalf("restored matches = %d, want %d", len(got.Matches), len(want.Matches))
	}
	for i := range want.Matches {
		if got.Matches[i].ID != want.Matches[i].ID {
			t.Fatalf("restored match %d = %s, want %s", i, got.Matches[i].ID, want.Matches[i].ID)
		}
		if got.Matches[i].Score != want.Matches[i].Score {
			t.Fatalf("restored score %d = %v, want %v", i, got.Matches[i].Score, want.Matches[i].Score)
		}
	}
	// Deleted ids stayed deleted and metadata round-tripped.
	if vs, _ := rstore.GetByIds(ctx, "acme", "idx", []string{"v0", "v42"}); len(vs) != 1 || vs[0].ID != "v42" {
		t.Fatalf("restored getByIds = %+v", vs)
	}
	if string(got.Matches[0].Metadata) == "" {
		t.Fatalf("metadata missing after restore: %+v", got.Matches[0])
	}
	// The restored cell can still be written and captured (it is a normal cell).
	if _, err := rstore.Upsert(ctx, "acme", "idx", cfg, []Vector{{ID: "v999", Values: mk(999)}}, true); err != nil {
		t.Fatalf("write to restored cell: %v", err)
	}
}
