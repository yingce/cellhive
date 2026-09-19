package cellstore

import (
	"context"
	"database/sql"
	"os"
	"time"
)

// DiskStats is a cell's cheap (O(1)) storage footprint: SQLite page accounting
// plus the real file and WAL bytes on disk (ADR-157).
//
// PageCount/SizeBytes cover only the main database file. cellstore opens cells
// with wal_autocheckpoint(0), so the WAL can hold a large part of the recent
// writes; it is reported separately (WALBytes) and TotalBytes is the honest
// on-disk footprint.
type DiskStats struct {
	PageSize      int    `json:"page_size"`
	PageCount     int    `json:"page_count"`
	SizeBytes     int64  `json:"size_bytes"` // page_size * page_count (logical main file)
	FreelistPages int    `json:"freelist_pages"`
	FreelistBytes int64  `json:"freelist_bytes"`
	MainBytes     int64  `json:"main_bytes"` // stat(<cell>.db)
	WALBytes      int64  `json:"wal_bytes"`  // stat(<cell>.db-wal)
	TotalBytes    int64  `json:"total_bytes"`
	JournalMode   string `json:"journal_mode"`
}

// DiskStats reads the cell's page accounting and file sizes. It touches no data
// pages (three PRAGMAs plus two stats) and is safe for operator endpoints.
func (c *Cell) DiskStats(ctx context.Context) (DiskStats, error) {
	var st DiskStats
	if err := c.DB.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&st.PageSize); err != nil {
		return st, err
	}
	if err := c.DB.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&st.PageCount); err != nil {
		return st, err
	}
	if err := c.DB.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&st.FreelistPages); err != nil {
		return st, err
	}
	var mode string
	if err := c.DB.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil {
		return st, err
	}
	st.JournalMode = mode
	st.SizeBytes = int64(st.PageSize) * int64(st.PageCount)
	st.FreelistBytes = int64(st.PageSize) * int64(st.FreelistPages)
	if fi, err := os.Stat(c.Path); err == nil {
		st.MainBytes = fi.Size()
	}
	if fi, err := os.Stat(c.WALPath()); err == nil {
		st.WALBytes = fi.Size()
	}
	st.TotalBytes = st.MainBytes + st.WALBytes
	return st, nil
}

// ErrNotInline is returned when a stats probe needs a page scan the caller did
// not ask for (too large / exactness not requested). Callers surface it as a
// skipped field rather than failing the whole request.
type ErrNotInline struct{ Reason string }

func (e ErrNotInline) Error() string { return "cellstore: not inline: " + e.Reason }

// KVStatsOptions tunes a KV stats read.
type KVStatsOptions struct {
	// Prefix restricts the count-style fields (Keys, Expired, NextExpiryMs) to
	// one key range. RowsEstimate always covers the whole cell.
	Prefix string
	// Exact requests the exact key count, which scans the primary-key index
	// (O(keys)). Without it, no key count is returned.
	Exact bool
	// MaxEstimatePages guards the dbstat row estimate; 0 uses the default.
	MaxEstimatePages int
}

// DefaultMaxEstimatePages is the largest cell (in 4 KiB-ish pages) for which a
// dbstat row estimate is considered cheap enough for an operator request.
const DefaultMaxEstimatePages = 20000

// KVStats is the operator-facing summary of a KV cell. Every field comes from a
// metadata read or an index/`dbstat` scan; nothing decodes value payloads.
type KVStats struct {
	DiskStats
	ExpiresIndexed bool  `json:"expires_indexed"`
	Expired        int   `json:"expired"`
	NextExpiryMs   int64 `json:"next_expiry_ms"`
	// RowsEstimate is the leaf-cell count from the dbstat virtual table: an
	// O(pages) estimate that does not decode rows.
	RowsEstimate int    `json:"rows_estimate"`
	EstimateNote string `json:"estimate_note,omitempty"`
	// Keys is only set when Exact was requested.
	Keys  int    `json:"keys,omitempty"`
	Exact bool   `json:"exact"`
	Note  string `json:"note,omitempty"`
}

// KVStats summarizes a KV cell (ADR-157).
func (c *Cell) KVStats(ctx context.Context, opts KVStatsOptions) (KVStats, error) {
	ds, err := c.DiskStats(ctx)
	if err != nil {
		return KVStats{}, err
	}
	st := KVStats{DiskStats: ds, Exact: opts.Exact}
	if err := c.DB.QueryRowContext(ctx,
		`SELECT count(1) FROM sqlite_master WHERE type='index' AND name='kv_expires'`).Scan(&st.ExpiresIndexed); err != nil {
		return st, err
	}
	now := time.Now().UnixMilli()
	lo, hi, scoped := kvRange(opts.Prefix)
	q := `SELECT COUNT(1) FROM kv WHERE expires_ms>0 AND expires_ms<=?`
	args := []any{now}
	if scoped {
		q += ` AND key >= ?`
		args = append(args, lo)
		if hi != "" {
			q += ` AND key < ?`
			args = append(args, hi)
		}
	}
	if err := c.DB.QueryRowContext(ctx, q, args...).Scan(&st.Expired); err != nil {
		return st, err
	}
	nq := `SELECT MIN(expires_ms) FROM kv WHERE expires_ms>0`
	nargs := []any{}
	if scoped {
		nq += ` AND key >= ?`
		nargs = append(nargs, lo)
		if hi != "" {
			nq += ` AND key < ?`
			nargs = append(nargs, hi)
		}
	}
	var next sql.NullInt64
	if err := c.DB.QueryRowContext(ctx, nq, nargs...).Scan(&next); err != nil {
		return st, err
	}
	if next.Valid {
		st.NextExpiryMs = next.Int64
	}
	if opts.Exact {
		kq := `SELECT COUNT(1) FROM kv`
		kargs := []any{}
		if scoped {
			kq += ` WHERE key >= ?`
			kargs = append(kargs, lo)
			if hi != "" {
				kq += ` AND key < ?`
				kargs = append(kargs, hi)
			}
		}
		if err := c.DB.QueryRowContext(ctx, kq, kargs...).Scan(&st.Keys); err != nil {
			return st, err
		}
	}
	if scoped {
		st.EstimateNote = "whole_cell"
	}
	maxPages := opts.MaxEstimatePages
	if maxPages <= 0 {
		maxPages = DefaultMaxEstimatePages
	}
	if ds.PageCount > maxPages {
		st.EstimateNote = joinNote(st.EstimateNote, "too_large")
	} else {
		est := 0
		if err := c.DB.QueryRowContext(ctx,
			`SELECT COALESCE(SUM(ncell),0) FROM dbstat WHERE name='kv' AND pagetype='leaf'`).Scan(&est); err == nil {
			st.RowsEstimate = est
		} else {
			st.EstimateNote = joinNote(st.EstimateNote, "dbstat_unavailable")
		}
	}
	return st, nil
}

// kvRange converts an inclusive key prefix into a half-open range. An empty hi
// means the prefix has no finite upper bound (every byte is 0xff).
func kvRange(prefix string) (lo, hi string, ok bool) {
	if prefix == "" {
		return "", "", false
	}
	hi, bounded := prefixUpperBound(prefix)
	if !bounded {
		hi = ""
	}
	return prefix, hi, true
}

func joinNote(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	return a + "," + b
}
