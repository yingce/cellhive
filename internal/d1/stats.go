package d1

import (
	"context"
	"database/sql"
	"sort"
	"strings"

	"cellhive/internal/cellstore"
)

// D1StatsOptions tunes a D1 stats read (ADR-157).
type D1StatsOptions struct {
	// TableDetail asks for per-table page/payload/row-estimate rows. It costs an
	// O(pages) dbstat scan, so it is skipped for cells above MaxEstimatePages.
	TableDetail bool
	// MaxEstimatePages guards the dbstat scan; 0 uses the default.
	MaxEstimatePages int
}

// D1Stats is the operator-facing summary of a D1 database (one cell). All
// fields are metadata reads; no user rows are decoded.
type D1Stats struct {
	cellstore.DiskStats
	Tables        int            `json:"tables"` // user tables (cellstore internals excluded)
	TableNames    []string       `json:"table_names"`
	Indexes       int            `json:"indexes"`      // index objects, including SQLite autoindexes
	AutoIndexes   int            `json:"auto_indexes"` // the implicit ones
	SchemaVersion int            `json:"schema_version"`
	UserVersion   int            `json:"user_version"`
	SQLiteVersion string         `json:"sqlite_version"`
	TableStats    []D1TableStats `json:"table_stats,omitempty"`
	Note          string         `json:"note,omitempty"`
}

// D1TableStats is one table's dbstat-derived usage.
type D1TableStats struct {
	Name         string `json:"name"`
	Pages        int    `json:"pages"`
	PayloadBytes int64  `json:"payload_bytes"`
	RowsEstimate int    `json:"rows_estimate"` // leaf cells; approximate
}

// cellstoreInternalTables are the platform tables that live in every cell and
// are not part of the tenant database.
var cellstoreInternalTables = map[string]bool{"kv": true, "cell_meta": true}

// Stats summarizes a D1 database. It reads only PRAGMAs, sqlite schema and (on
// request) the dbstat virtual table; it never scans user rows, so it is safe to
// expose to operators without a capture/proof round trip.
func (s *Store) Stats(ctx context.Context, ns, db string, opts D1StatsOptions) (D1Stats, error) {
	c, err := s.cell(ctx, ns, db)
	if err != nil {
		return D1Stats{}, err
	}
	dsk, err := c.DiskStats(ctx)
	if err != nil {
		return D1Stats{}, err
	}
	st := D1Stats{DiskStats: dsk}
	if err := c.DB.QueryRowContext(ctx, `SELECT sqlite_version()`).Scan(&st.SQLiteVersion); err != nil {
		return st, err
	}
	if err := c.DB.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&st.SchemaVersion); err != nil {
		return st, err
	}
	if err := c.DB.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&st.UserVersion); err != nil {
		return st, err
	}

	type tbl struct {
		name, typ string
		ncol      int
	}
	var all []tbl
	rows, err := c.DB.QueryContext(ctx,
		`SELECT name, type, ncol FROM pragma_table_list WHERE schema='main' ORDER BY name`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var t tbl
		if err := rows.Scan(&t.name, &t.typ, &t.ncol); err != nil {
			rows.Close()
			return st, err
		}
		all = append(all, t)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return st, err
	}
	rows.Close()
	names := make([]string, 0, len(all))
	for _, t := range all {
		switch {
		case t.typ == "table" && !cellstoreInternalTables[t.name] && !strings.HasPrefix(t.name, "sqlite_"):
			st.Tables++
			names = append(names, t.name)
		case t.typ == "index":
			st.Indexes++
			if strings.HasPrefix(t.name, "sqlite_autoindex_") {
				st.AutoIndexes++
			}
		}
	}
	sort.Strings(names)
	st.TableNames = names

	maxPages := opts.MaxEstimatePages
	if maxPages <= 0 {
		maxPages = cellstore.DefaultMaxEstimatePages
	}
	if !opts.TableDetail {
		return st, nil
	}
	if dsk.PageCount > maxPages {
		st.Note = "table_stats_skipped_too_large"
		return st, nil
	}
	per, err := dbstatByObject(ctx, c)
	if err != nil {
		st.Note = "dbstat_unavailable"
		return st, nil
	}
	for _, name := range names {
		v := per[name]
		st.TableStats = append(st.TableStats, D1TableStats{
			Name: name, Pages: v.pages, PayloadBytes: v.payload, RowsEstimate: v.cells,
		})
	}
	return st, nil
}

type dbstatRow struct {
	pages, cells int
	payload      int64
}

// dbstatByObject aggregates the dbstat virtual table per b-tree object name:
// pages, leaf cells (a row-count estimate) and payload bytes. It does not
// decode rows.
func dbstatByObject(ctx context.Context, c *cellstore.Cell) (map[string]dbstatRow, error) {
	rows, err := c.DB.QueryContext(ctx, `
		SELECT name,
		       COUNT(*),
		       COALESCE(SUM(CASE WHEN pagetype='leaf' THEN ncell ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN pagetype='leaf' THEN payload ELSE 0 END), 0)
		  FROM dbstat GROUP BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]dbstatRow{}
	for rows.Next() {
		var name string
		var r dbstatRow
		var cells sql.NullInt64
		if err := rows.Scan(&name, &r.pages, &cells, &r.payload); err != nil {
			return nil, err
		}
		r.cells = int(cells.Int64)
		out[name] = r
	}
	return out, rows.Err()
}
