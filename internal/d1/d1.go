// Package d1 implements the D1-compatible SQL API over per-database cell SQLite
// (docs/bindings.md). Each D1 database is a cell at <ns>/__d1__/<db>; queries run
// against its SQLite file, so the data is replicated and fenced like any cell.
package d1

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

// Store runs D1 statements over cell databases.
type Store struct {
	cs *cellstore.Store
}

// New creates a D1 store.
func New(cs *cellstore.Store) *Store { return &Store{cs: cs} }

// Scope is the cell scope for a database.
func Scope(ns, db string) cell.Scope {
	return cell.Scope{Namespace: ns, Class: "__d1__", ID: db}
}

func (s *Store) cell(ctx context.Context, ns, db string) (*cellstore.Cell, error) {
	if ns == "" || db == "" {
		return nil, fmt.Errorf("d1: ns and db are required")
	}
	return s.cs.Cell(ctx, Scope(ns, db))
}

// Statement is one parameterized statement in a batch.
type Statement struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params,omitempty"`
}

// Result is one statement's outcome. Query results carry Columns/Rows; mutations
// carry RowsAffected.
type Result struct {
	Columns      []string `json:"columns,omitempty"`
	Rows         [][]any  `json:"rows,omitempty"`
	RowsAffected int64    `json:"rows_affected"`
	LastInsertID int64    `json:"last_row_id"`
	DurationMs   float64  `json:"duration_ms"`
}

// Query runs a single statement and returns its rows (or affected count for a
// mutation), mirroring D1's prepare().all()/run().
func (s *Store) Query(ctx context.Context, ns, db, sqlText string, params []any) (Result, error) {
	c, err := s.cell(ctx, ns, db)
	if err != nil {
		return Result{}, err
	}
	// Reads skip the capture advance; anything else is a mutation and must
	// advance the cell txid in the same commit so capture's Wait() covers it.
	// IsReadOnly (not returnsRows) decides this: a WITH statement returns rows
	// but may still mutate.
	if IsReadOnly(sqlText) {
		return runOne(ctx, c.DB, nil, sqlText, params)
	}
	var res Result
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		var e error
		res, e = runOne(ctx, nil, tx, sqlText, params)
		return e
	})
	return res, err
}

// Exec runs a single mutation statement and returns the affected count.
func (s *Store) Exec(ctx context.Context, ns, db, sqlText string, params []any) (Result, error) {
	c, err := s.cell(ctx, ns, db)
	if err != nil {
		return Result{}, err
	}
	start := time.Now()
	var n, id int64
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		res, e := tx.ExecContext(ctx, sqlText, normalizeParams(params)...)
		if e != nil {
			return e
		}
		n, _ = res.RowsAffected()
		id, _ = res.LastInsertId()
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: n, LastInsertID: id, DurationMs: msSince(start)}, nil
}

// Batch runs several statements in one transaction, so a failure rolls all back.
func (s *Store) Batch(ctx context.Context, ns, db string, stmts []Statement) ([]Result, error) {
	c, err := s.cell(ctx, ns, db)
	if err != nil {
		return nil, err
	}
	out := make([]Result, 0, len(stmts))
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		for i, st := range stmts {
			r, rerr := runOne(ctx, nil, tx, st.SQL, st.Params)
			if rerr != nil {
				return fmt.Errorf("d1: batch statement %d: %w", i, rerr)
			}
			out = append(out, r)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func runOne(ctx context.Context, db *sql.DB, tx *sql.Tx, sqlText string, params []any) (Result, error) {
	start := time.Now()
	if sqlText = strings.TrimSpace(sqlText); sqlText == "" {
		return Result{}, fmt.Errorf("d1: empty statement")
	}
	if !returnsRows(sqlText) {
		var (
			res sql.Result
			err error
		)
		if tx != nil {
			res, err = tx.ExecContext(ctx, sqlText, normalizeParams(params)...)
		} else {
			res, err = db.ExecContext(ctx, sqlText, normalizeParams(params)...)
		}
		if err != nil {
			return Result{}, err
		}
		n, _ := res.RowsAffected()
		id, _ := res.LastInsertId()
		return Result{RowsAffected: n, LastInsertID: id, DurationMs: msSince(start)}, nil
	}

	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, sqlText, normalizeParams(params)...)
	} else {
		rows, err = db.QueryContext(ctx, sqlText, normalizeParams(params)...)
	}
	if err != nil {
		return Result{}, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return Result{}, err
	}
	out := Result{Columns: cols}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return Result{}, err
		}
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				vals[i] = string(b)
			}
		}
		out.Rows = append(out.Rows, vals)
	}
	if err := rows.Err(); err != nil {
		return Result{}, err
	}
	out.DurationMs = msSince(start)
	return out, nil
}

// IsReadOnly reports whether a statement can never write, so the capture layer
// may skip the durability proof for it. It is deliberately conservative: WITH
// and PRAGMA can write, so they are treated as mutations and still captured.
func IsReadOnly(sqlText string) bool {
	t := strings.TrimSpace(sqlText)
	for strings.HasPrefix(t, "(") {
		t = strings.TrimSpace(t[1:])
	}
	up := strings.ToUpper(t)
	for _, p := range []string{"SELECT", "EXPLAIN", "VALUES"} {
		if strings.HasPrefix(up, p) && (len(t) == len(p) || !isWordByte(t[len(p)])) {
			return true
		}
	}
	return false
}

func isWordByte(b byte) bool {
	return b == '_' || (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func returnsRows(sqlText string) bool {
	up := strings.ToUpper(strings.TrimSpace(sqlText))
	for _, p := range []string{"SELECT", "WITH", "PRAGMA", "EXPLAIN", "VALUES"} {
		if strings.HasPrefix(up, p) {
			return true
		}
	}
	return false
}

// normalizeParams converts JSON numbers (float64) with integral values to int64
// so SQLite comparisons and integer columns behave as expected.
func normalizeParams(params []any) []any {
	out := make([]any, len(params))
	for i, p := range params {
		if f, ok := p.(float64); ok && f == float64(int64(f)) {
			out[i] = int64(f)
			continue
		}
		out[i] = p
	}
	return out
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }
