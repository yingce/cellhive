package cellstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/wal"
)

func TestPutGetDelete(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	c, err := s.Open(ctx, sc)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()

	if err := c.Put(ctx, "alice", []byte("1"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, _, err := c.Get(ctx, "alice")
	if err != nil || string(v) != "1" {
		t.Fatalf("get = %q, %v", v, err)
	}
	if err := c.Put(ctx, "alice", []byte("2"), []byte("m")); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	v, m, _ := c.Get(ctx, "alice")
	if string(v) != "2" || string(m) != "m" {
		t.Fatalf("overwrite get = %q/%q", v, m)
	}
	if err := c.Delete(ctx, "alice"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, _, err := c.Get(ctx, "alice"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
}

func TestTxIDIncreases(t *testing.T) {
	ctx := context.Background()
	s, _ := New(t.TempDir())
	c, _ := s.Open(ctx, cell.Scope{Namespace: "n", Class: "__kv__", ID: "a"})
	defer c.Close()
	before, _ := c.TxID(ctx)
	_ = c.Put(ctx, "k", []byte("v"), nil)
	after, _ := c.TxID(ctx)
	if after <= before {
		t.Fatalf("txid did not increase: %d -> %d", before, after)
	}
}

func TestPutTxAtomic(t *testing.T) {
	ctx := context.Background()
	s, _ := New(t.TempDir())
	c, _ := s.Open(ctx, cell.Scope{Namespace: "n", Class: "__kv__", ID: "atomic"})
	defer c.Close()

	beforeTxID, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid before: %v", err)
	}
	beforeWAL, err := wal.Read(c.WALPath())
	if err != nil {
		t.Fatalf("wal before: %v", err)
	}

	txid, err := c.PutTx(ctx, "k", []byte("v"), []byte("m"))
	if err != nil {
		t.Fatalf("put tx: %v", err)
	}
	if txid != beforeTxID+1 {
		t.Fatalf("returned txid = %d, want %d", txid, beforeTxID+1)
	}
	afterTxID, _ := c.TxID(ctx)
	if afterTxID != txid {
		t.Fatalf("stored txid = %d, want %d", afterTxID, txid)
	}
	afterWAL, err := wal.Read(c.WALPath())
	if err != nil {
		t.Fatalf("wal after: %v", err)
	}
	if got := afterWAL.CommitCount - beforeWAL.CommitCount; got != 1 {
		t.Fatalf("WAL commits added = %d, want 1", got)
	}
}

func TestWALAutoCheckpointDisabled(t *testing.T) {
	ctx := context.Background()
	s, _ := New(t.TempDir())
	c, _ := s.Open(ctx, cell.Scope{Namespace: "n", Class: "__kv__", ID: "checkpoint"})
	defer c.Close()
	var pages int
	if err := c.DB.QueryRowContext(ctx, `PRAGMA wal_autocheckpoint`).Scan(&pages); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if pages != 0 {
		t.Fatalf("wal_autocheckpoint = %d, want 0", pages)
	}
}

func TestOpenAtUsesNormalSynchronousMode(t *testing.T) {
	ctx := context.Background()
	c, err := OpenAt(ctx, filepath.Join(t.TempDir(), "bench.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()
	var mode int
	if err := c.DB.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&mode); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if mode != 1 {
		t.Fatalf("synchronous = %d, want NORMAL (1)", mode)
	}
}

func TestScanPrefix(t *testing.T) {
	ctx := context.Background()
	s, _ := New(t.TempDir())
	c, _ := s.Open(ctx, cell.Scope{Namespace: "n", Class: "__kv__", ID: "b"})
	defer c.Close()
	_ = c.Put(ctx, "user:1", []byte("a"), nil)
	_ = c.Put(ctx, "user:2", []byte("b"), nil)
	_ = c.Put(ctx, "other", []byte("c"), nil)
	var n int
	_ = c.Scan(ctx, "user:", func(string, []byte, []byte) error { n++; return nil })
	if n != 2 {
		t.Fatalf("scan count = %d, want 2", n)
	}
}

func TestSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, _ := New(dir)
	c, _ := s.Open(ctx, cell.Scope{Namespace: "n", Class: "__d1__", ID: "db"})
	defer c.Close()
	_ = c.Put(ctx, "k", []byte("v"), nil)

	dest := filepath.Join(dir, "snap", "copy.db")
	if err := c.Snapshot(ctx, dest); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	info, err := os.Stat(dest)
	if err != nil || info.Size() == 0 {
		t.Fatalf("snapshot missing/empty: %v", err)
	}

	// The snapshot must open as an independent database with the same data.
	restored, err := OpenAt(ctx, dest)
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer restored.Close()
	v, _, err := restored.Get(ctx, "k")
	if err != nil || !bytes.Equal(v, []byte("v")) {
		t.Fatalf("restored get = %q, %v", v, err)
	}
}

func BenchmarkPutTx(b *testing.B) {
	ctx := context.Background()
	s, err := New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	c, err := s.Open(ctx, cell.Scope{Namespace: "bench", Class: "__kv__", ID: "puttx"})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	value := bytes.Repeat([]byte("x"), 64)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := c.PutTx(ctx, "hot", value, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRawUpsert(b *testing.B) {
	ctx := context.Background()
	s, err := New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	c, err := s.Open(ctx, cell.Scope{Namespace: "bench", Class: "__kv__", ID: "raw"})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	value := bytes.Repeat([]byte("x"), 64)
	const upsert = `INSERT INTO kv(key,value,meta,updated_ms) VALUES('hot',?,?,0)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value,meta=excluded.meta`
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx, err := c.DB.BeginTx(ctx, nil)
			if err == nil {
				_, err = tx.ExecContext(ctx, upsert, value, []byte(nil))
			}
			if err == nil {
				err = tx.Commit()
			} else if tx != nil {
				_ = tx.Rollback()
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRawUpsertPrepared(b *testing.B) {
	ctx := context.Background()
	s, err := New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	c, err := s.Open(ctx, cell.Scope{Namespace: "bench", Class: "__kv__", ID: "rawprep"})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	stmt, err := c.DB.PrepareContext(ctx, `INSERT INTO kv(key,value,meta,updated_ms) VALUES('hot',?,?,0)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value,meta=excluded.meta`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	value := bytes.Repeat([]byte("x"), 64)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx, err := c.DB.BeginTx(ctx, nil)
			if err == nil {
				_, err = tx.StmtContext(ctx, stmt).ExecContext(ctx, value, []byte(nil))
			}
			if err == nil {
				err = tx.Commit()
			} else if tx != nil {
				_ = tx.Rollback()
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestCellCachesAndReopensAfterClose(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "kv"}

	c1, err := s.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	c2, err := s.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	if c1 != c2 {
		t.Fatalf("same scope must return the cached cell")
	}
	if err := c1.Put(ctx, "k", []byte("v"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}

	c3, err := s.Cell(ctx, cell.Scope{Namespace: "demo", Class: "__kv__", ID: "other"})
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	if c3 == c1 {
		t.Fatalf("distinct scopes must not share a cell")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	c4, err := s.Cell(ctx, sc)
	if err != nil {
		t.Fatalf("cell after close: %v", err)
	}
	if c4 == c1 {
		t.Fatalf("expected a fresh handle after Close")
	}
	v, _, err := c4.Get(ctx, "k")
	if err != nil || string(v) != "v" {
		t.Fatalf("get after reopen = %q, %v", v, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestCellConcurrentOpenAndPut(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "kv"}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := s.Cell(ctx, sc)
			if err != nil {
				t.Errorf("cell: %v", err)
				return
			}
			if err := c.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"), nil); err != nil {
				t.Errorf("put: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
