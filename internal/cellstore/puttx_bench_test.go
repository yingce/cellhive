package cellstore

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"cellhive/internal/cell"
)

func benchCell(b *testing.B) *Cell {
	b.Helper()
	s, err := New(b.TempDir())
	if err != nil {
		b.Fatalf("new store: %v", err)
	}
	c, err := s.Cell(context.Background(), cell.Scope{Namespace: "bench", Class: "__kv__", ID: "main"})
	if err != nil {
		b.Fatalf("cell: %v", err)
	}
	return c
}

// BenchmarkPutTxSerial is the raw cost of one cell transaction: BEGIN, UPSERT,
// txid bump, COMMIT (the same path a KV/D1 write uses), single-threaded.
func BenchmarkPutTxSerial(b *testing.B) {
	c := benchCell(b)
	ctx := context.Background()
	val := []byte("v")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := "k" + strconv.Itoa(i%1000)
		if _, err := c.PutTx(ctx, k, val, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutTxParallel is the ceiling of the local SQLite write path under
// concurrency (single writer, WAL).
func BenchmarkPutTxParallel(b *testing.B) {
	c := benchCell(b)
	ctx := context.Background()
	val := []byte("v")
	var n int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := "k" + strconv.Itoa(int(atomic.AddInt64(&n, 1))%1000)
			if _, err := c.PutTx(ctx, k, val, nil); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkPutTxBatch100 amortizes one commit over 100 keys, showing how much
// throughput is available when writes are batched (the lever the single-key
// HTTP path cannot use).
func BenchmarkPutTxBatch100(b *testing.B) {
	c := benchCell(b)
	ctx := context.Background()
	val := []byte("v")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := i * 100
		if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
			for j := 0; j < 100; j++ {
				k := fmt.Sprintf("k%d", (base+j)%100000)
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO kv(key,value,meta,updated_ms) VALUES(?,?,?,0)
					 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, k, val, nil); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRawInsertSerial is a plain database/sql INSERT (autocommit, no txid
// bookkeeping) as a driver-level baseline.
func BenchmarkRawInsertSerial(b *testing.B) {
	path := b.TempDir() + "/raw.db"
	db, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(3)
	if _, err := db.Exec(`CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB NOT NULL, meta BLOB, updated_ms INTEGER NOT NULL)`); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	val := []byte("v")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := "k" + strconv.Itoa(i%1000)
		if _, err := db.ExecContext(ctx,
			`INSERT INTO kv(key,value,meta,updated_ms) VALUES(?,?,?,0)
			 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, k, val, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTxNoop is the cost of an empty explicit transaction (BEGIN/COMMIT).
func BenchmarkTxNoop(b *testing.B) {
	c := benchCell(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpsertInTx is an explicit transaction with just the UPSERT (no txid
// bookkeeping), isolating the txid cost in PutTx.
func BenchmarkUpsertInTx(b *testing.B) {
	c := benchCell(b)
	ctx := context.Background()
	val := []byte("v")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.txNoBump(ctx, func(tx *sql.Tx) error {
			k := "k" + strconv.Itoa(i%1000)
			_, err := tx.ExecContext(ctx,
				`INSERT INTO kv(key,value,meta,updated_ms) VALUES(?,?,?,0)
				 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, k, val, nil)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}
