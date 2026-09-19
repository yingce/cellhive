package cellstore

import (
	"context"
	"sync"
	"testing"

	"cellhive/internal/cell"
)

// TestTxIDMirrorContinuesAcrossReopen proves the in-memory txid mirror is
// reloaded from cell_meta on open, so a restart (or a takeover that restores the
// file) continues the sequence instead of reusing txids.
func TestTxIDMirrorContinuesAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"}

	s1, err := New(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	c1, err := s1.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	last := uint64(0)
	for i := 0; i < 5; i++ {
		last, err = c1.PutTx(ctx, "k", []byte("v"), nil)
		if err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := New(dir)
	if err != nil {
		t.Fatalf("new store 2: %v", err)
	}
	defer s2.Close()
	c2, err := s2.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("cell 2: %v", err)
	}
	if got, err := c2.TxID(ctx); err != nil || got != last {
		t.Fatalf("txid after reopen = %d (err=%v), want %d", got, err, last)
	}
	next, err := c2.PutTx(ctx, "k2", []byte("v"), nil)
	if err != nil {
		t.Fatalf("put 2: %v", err)
	}
	if next != last+1 {
		t.Fatalf("txid after reopen put = %d, want %d", next, last+1)
	}
}

// TestTxIDsUniqueUnderConcurrency proves the writeMu + mirror yield exactly one
// unique, gap-free txid per committed write under concurrent writers.
func TestTxIDsUniqueUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()
	c, err := s.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"})
	if err != nil {
		t.Fatalf("cell: %v", err)
	}

	const n = 512
	var mu sync.Mutex
	seen := make(map[uint64]bool, n)
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < n/16; i++ {
				id, err := c.PutTx(ctx, "k", []byte("v"), nil)
				if err != nil {
					t.Errorf("put: %v", err)
					return
				}
				mu.Lock()
				if seen[id] {
					t.Errorf("duplicate txid %d", id)
				}
				seen[id] = true
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("distinct txids = %d, want %d", len(seen), n)
	}
	for i := uint64(1); i <= n; i++ {
		if !seen[i] {
			t.Fatalf("missing txid %d (gap)", i)
		}
	}
}
