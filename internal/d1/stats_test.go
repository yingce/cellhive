package d1

import (
	"context"
	"strings"
	"testing"
)

// TestD1Stats covers the ADR-157 operator stats for a D1 database: page
// accounting, schema inventory (with cellstore's internal tables excluded) and
// the opt-in per-table dbstat breakdown.
func TestD1Stats(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Exec(ctx, "acme", "app", `CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)`, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, "acme", "app", `INSERT INTO users(name) VALUES('a'),('b'),('c')`, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, "acme", "app", `CREATE TABLE orders(id INTEGER PRIMARY KEY, total INTEGER)`, nil); err != nil {
		t.Fatal(err)
	}

	st, err := s.Stats(ctx, "acme", "app", D1StatsOptions{})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.PageSize <= 0 || st.PageCount <= 0 || st.SizeBytes != int64(st.PageSize)*int64(st.PageCount) {
		t.Fatalf("disk stats inconsistent: %+v", st.DiskStats)
	}
	if st.Tables != 2 {
		t.Fatalf("tables = %d (%v), want 2 user tables", st.Tables, st.TableNames)
	}
	if strings.Join(st.TableNames, ",") != "orders,users" {
		t.Fatalf("table names = %v", st.TableNames)
	}
	if st.SQLiteVersion == "" || st.SchemaVersion == 0 {
		t.Fatalf("schema metadata missing: %+v", st)
	}
	if len(st.TableStats) != 0 {
		t.Fatalf("table stats must be opt-in, got %+v", st.TableStats)
	}

	det, err := s.Stats(ctx, "acme", "app", D1StatsOptions{TableDetail: true})
	if err != nil {
		t.Fatalf("stats detail: %v", err)
	}
	if len(det.TableStats) != 2 {
		t.Fatalf("table stats = %+v, want 2", det.TableStats)
	}
	users := -1
	for _, ts := range det.TableStats {
		if ts.Name == "users" {
			users = ts.RowsEstimate
			if ts.Pages <= 0 || ts.PayloadBytes <= 0 {
				t.Fatalf("users usage empty: %+v", ts)
			}
		}
	}
	if users < 3 {
		t.Fatalf("users row estimate = %d, want >= 3", users)
	}
}

// TestD1StatsLargeCellSkipsDetail: the dbstat breakdown must be refused (not
// silently slow) once the cell is above the page guard.
func TestD1StatsLargeCellSkipsDetail(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Exec(ctx, "acme", "big", `CREATE TABLE t(x)`, nil); err != nil {
		t.Fatal(err)
	}
	st, err := s.Stats(ctx, "acme", "big", D1StatsOptions{TableDetail: true, MaxEstimatePages: 1})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Note != "table_stats_skipped_too_large" || len(st.TableStats) != 0 {
		t.Fatalf("note = %q stats = %+v, want skipped", st.Note, st.TableStats)
	}
}
