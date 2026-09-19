package cellstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
)

// TestDiskStatsAndKVStats covers the ADR-157 operator stats: page accounting,
// the expiry index (which fresh databases must have), expiry counters and the
// dbstat row estimate.
func TestDiskStatsAndKVStats(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer s.Close()
	c, err := s.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "stats"})
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	now := time.Now()
	if err := c.PutOpts(ctx, "live", []byte("v"), nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.PutOpts(ctx, "soon", []byte("v"), nil, now.Add(time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := c.PutOpts(ctx, "gone", []byte("v"), nil, now.Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}

	ds, err := c.DiskStats(ctx)
	if err != nil {
		t.Fatalf("disk stats: %v", err)
	}
	if ds.PageSize <= 0 || ds.PageCount <= 0 || ds.SizeBytes != int64(ds.PageSize)*int64(ds.PageCount) {
		t.Fatalf("disk stats inconsistent: %+v", ds)
	}
	if ds.MainBytes <= 0 || ds.TotalBytes < ds.MainBytes {
		t.Fatalf("file bytes empty: %+v", ds)
	}
	if ds.JournalMode != "wal" {
		t.Fatalf("journal mode = %q, want wal", ds.JournalMode)
	}

	st, err := c.KVStats(ctx, KVStatsOptions{})
	if err != nil {
		t.Fatalf("kv stats: %v", err)
	}
	// Fresh cells must carry the partial expiry index (ADR-157 regression).
	if !st.ExpiresIndexed {
		t.Fatal("kv_expires index missing on a fresh cell")
	}
	if st.Expired != 1 {
		t.Fatalf("expired = %d, want 1", st.Expired)
	}
	if st.NextExpiryMs == 0 {
		t.Fatal("next expiry not reported")
	}
	if st.Exact {
		t.Fatal("Exact must be false unless requested")
	}
	if st.Keys != 0 {
		t.Fatalf("keys without exact = %d, want 0", st.Keys)
	}
	// dbstat is expected to be compiled in (modernc sqlite enables it); if not,
	// the field is skipped with a note instead of failing.
	if st.RowsEstimate == 0 && st.EstimateNote == "" {
		t.Fatalf("neither a row estimate nor a note: %+v", st)
	}
	if st.RowsEstimate > 0 && st.RowsEstimate < 3 {
		t.Fatalf("rows estimate = %d, want >= 3", st.RowsEstimate)
	}

	ex, err := c.KVStats(ctx, KVStatsOptions{Exact: true})
	if err != nil {
		t.Fatalf("kv stats exact: %v", err)
	}
	if !ex.Exact || ex.Keys != 3 {
		t.Fatalf("exact keys = %d (exact=%v), want 3", ex.Keys, ex.Exact)
	}

	// A prefix scopes the count-style fields only.
	pfx, err := c.KVStats(ctx, KVStatsOptions{Prefix: "li", Exact: true})
	if err != nil {
		t.Fatalf("kv stats prefix: %v", err)
	}
	if pfx.Keys != 1 || pfx.Expired != 0 {
		t.Fatalf("prefix stats = keys %d expired %d, want 1/0", pfx.Keys, pfx.Expired)
	}
}

// TestKVExpiryIndexUsed is the falsifiable half of the index fix: the plan for
// the expiry queries must use the partial index rather than scanning the table.
func TestKVExpiryIndexUsed(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer s.Close()
	c, err := s.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "idx"})
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	rows, err := c.DB.QueryContext(ctx,
		`EXPLAIN QUERY PLAN SELECT MIN(expires_ms) FROM kv WHERE expires_ms>0`)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var detail string
	for rows.Next() {
		var id, parent, notused int
		var txt string
		if err := rows.Scan(&id, &parent, &notused, &txt); err != nil {
			t.Fatal(err)
		}
		detail += txt + ";"
	}
	if !strings.Contains(detail, "kv_expires") {
		t.Fatalf("expiry query does not use kv_expires: %s", detail)
	}
}
