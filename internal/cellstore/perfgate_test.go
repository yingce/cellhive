package cellstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"cellhive/internal/cell"
)

// TestPerfGatePutTx is an opt-in performance regression gate. It is skipped
// unless CELLHIVE_PERF_GATE=1 (so it never flakes a normal test run on a loaded
// machine). It asserts a conservative floor for the local write path and that a
// 100-key batch amortises a commit.
func TestPerfGatePutTx(t *testing.T) {
	if os.Getenv("CELLHIVE_PERF_GATE") == "" {
		t.Skip("set CELLHIVE_PERF_GATE=1 to run the performance gate")
	}
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer s.Close()
	c, err := s.Cell(ctx, cell.Scope{Namespace: "perf", Class: "__kv__", ID: "gate"})
	if err != nil {
		t.Fatalf("cell: %v", err)
	}

	const n = 2000
	start := time.Now()
	for i := 0; i < n; i++ {
		if _, err := c.PutTx(ctx, fmt.Sprintf("k%d", i%1000), []byte("v"), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	perOp := float64(n) / time.Since(start).Seconds()

	const batches, perBatch = 20, 100
	start = time.Now()
	for b := 0; b < batches; b++ {
		if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
			for j := 0; j < perBatch; j++ {
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO kv(key,value,meta,updated_ms) VALUES(?,?,?,0)
					 ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
					fmt.Sprintf("b%d-%d", b, j), []byte("v"), nil); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("batch: %v", err)
		}
	}
	perKey := float64(batches*perBatch) / time.Since(start).Seconds()

	t.Logf("PutTx %.0f ops/s; batch100 %.0f keys/s", perOp, perKey)
	if perOp < 5000 {
		t.Fatalf("PutTx %.0f ops/s below the 5000 floor", perOp)
	}
	if perKey < 1.5*perOp {
		t.Fatalf("batch100 %.0f keys/s is not meaningfully better than per-op %.0f", perKey, perOp)
	}
}
