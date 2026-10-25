package d1

import (
	"cellhive/internal/cellstore"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestResultBudgetBlobExpansion(t *testing.T) {
	s := newStore(t)
	// Each BLOB expands into []int. The final row crosses 32 MiB only after expansion.
	_, err := s.Query(context.Background(), "budget", "blob", `WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<16) SELECT zeroblob(262145) FROM t`, nil)
	if err == nil || !strings.Contains(err.Error(), "result exceeds") {
		t.Fatalf("error=%v", err)
	}
}
func TestResultBudgetStrings(t *testing.T) {
	s := newStore(t)
	_, err := s.Query(context.Background(), "budget", "text", `WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<17) SELECT replace(hex(zeroblob(1048576)), '0', 'x') FROM t`, nil)
	if err == nil || !strings.Contains(err.Error(), "result exceeds") {
		t.Fatalf("error=%v", err)
	}
}
func TestResultCancelled(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Query(ctx, "budget", "cancel", `SELECT 1`, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}
func BenchmarkResultRows(b *testing.B) {
	for _, n := range []int{1000, 5000, 20000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			cs, err := cellstore.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			s := New(cs)
			defer s.cs.Close()
			c, err := s.cell(context.Background(), "bench", "rows")
			if err != nil {
				b.Fatal(err)
			}
			q := fmt.Sprintf(`WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<%d) SELECT 'abcdef',n FROM t`, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := runOne(context.Background(), c.DB, nil, q, nil)
				if err != nil || len(r.Rows) != n {
					b.Fatalf("rows=%d err=%v", len(r.Rows), err)
				}
			}
		})
	}
}

func TestResultNumericAccounting(t *testing.T) {
	if _, got := resultRowBytes([]any{int64(7), float64(1.5), true, nil}); got != 17 {
		t.Fatalf("numeric bytes=%d, want 17", got)
	}
}

func TestResultCancellationDuringQuery(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := s.Query(ctx, "budget", "running", `SELECT sum(n) FROM (WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<100000000) SELECT n FROM t)`, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
}
func TestResultRowLimit(t *testing.T) {
	s := newStore(t)
	_, err := s.Query(context.Background(), "budget", "rowlimit", `WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<100001) SELECT n FROM t`, nil)
	if err == nil || !strings.Contains(err.Error(), "result exceeds 100000 rows") {
		t.Fatalf("error=%v", err)
	}
}
func TestResultBlobBudgetBoundary(t *testing.T) {
	s := newStore(t)
	r, err := s.Query(context.Background(), "budget", "boundary", `WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<16) SELECT zeroblob(262144) FROM t`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 16 || len(r.Rows[0][0].([]int)) != 262144 {
		t.Fatal("incorrect blob result")
	}
}
