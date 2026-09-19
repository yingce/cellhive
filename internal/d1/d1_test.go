package d1

import (
	"context"
	"testing"

	"cellhive/internal/cellstore"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	return New(cs)
}

func TestD1CreateInsertQuery(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Exec(ctx, "acme", "main", `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	r, err := s.Exec(ctx, "acme", "main", `INSERT INTO users (id, name, age) VALUES (?, ?, ?)`, []any{1.0, "alice", 30.0})
	if err != nil || r.RowsAffected != 1 {
		t.Fatalf("insert = %+v, %v", r, err)
	}
	if _, err := s.Exec(ctx, "acme", "main", `INSERT INTO users (id, name, age) VALUES (?, ?, ?)`, []any{2, "bob", 25}); err != nil {
		t.Fatalf("insert2: %v", err)
	}
	q, err := s.Query(ctx, "acme", "main", `SELECT id, name, age FROM users WHERE age > ? ORDER BY id`, []any{26})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(q.Columns) != 3 || len(q.Rows) != 1 || q.Rows[0][1] != "alice" {
		t.Fatalf("query = %+v", q)
	}
	// int64 normalization: the integer column round-trips as int64.
	if _, ok := q.Rows[0][0].(int64); !ok {
		t.Fatalf("id type = %T", q.Rows[0][0])
	}
}

func TestD1BatchAtomic(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Exec(ctx, "acme", "main", `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	res, err := s.Batch(ctx, "acme", "main", []Statement{
		{SQL: `INSERT INTO t (id, v) VALUES (?, ?)`, Params: []any{1, "a"}},
		{SQL: `INSERT INTO t (id, v) VALUES (?, ?)`, Params: []any{2, "b"}},
		{SQL: `SELECT count(1) FROM t`},
	})
	if err != nil || len(res) != 3 || res[2].Rows[0][0] != int64(2) {
		t.Fatalf("batch = %+v, %v", res, err)
	}
	// A failing batch rolls everything back.
	_, err = s.Batch(ctx, "acme", "main", []Statement{
		{SQL: `INSERT INTO t (id, v) VALUES (?, ?)`, Params: []any{3, "c"}},
		{SQL: `INSERT INTO nope (id) VALUES (1)`},
	})
	if err == nil {
		t.Fatalf("bad batch did not error")
	}
	q, _ := s.Query(ctx, "acme", "main", `SELECT count(1) FROM t`, nil)
	if q.Rows[0][0] != int64(2) {
		t.Fatalf("rollback failed, count = %v", q.Rows[0][0])
	}
}

func TestD1Errors(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Query(ctx, "acme", "main", `SELECT * FROM missing`, nil); err == nil {
		t.Fatalf("missing table did not error")
	}
	if _, err := s.Query(ctx, "", "main", `SELECT 1`, nil); err == nil {
		t.Fatalf("empty ns did not error")
	}
	if _, err := s.Query(ctx, "acme", "main", "   ", nil); err == nil {
		t.Fatalf("empty sql did not error")
	}
}

func TestLastInsertID(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Exec(ctx, "acme", "main", `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	r, err := s.Exec(ctx, "acme", "main", `INSERT INTO t (v) VALUES (?)`, []any{"a"})
	if err != nil || r.LastInsertID != 1 {
		t.Fatalf("exec insert last_row_id = %+v, %v (want 1)", r, err)
	}
	q, err := s.Query(ctx, "acme", "main", `INSERT INTO t (v) VALUES (?)`, []any{"b"})
	if err != nil || q.LastInsertID != 2 {
		t.Fatalf("query insert last_row_id = %+v, %v (want 2)", q, err)
	}
	u, err := s.Exec(ctx, "acme", "main", `UPDATE t SET v='c' WHERE id=1`, nil)
	if err != nil || u.RowsAffected != 1 {
		t.Fatalf("update = %+v, %v", u, err)
	}
	b, err := s.Batch(ctx, "acme", "main", []Statement{
		{SQL: `INSERT INTO t (v) VALUES (?)`, Params: []any{"d"}},
		{SQL: `INSERT INTO t (v) VALUES (?)`, Params: []any{"e"}},
	})
	if err != nil || len(b) != 2 || b[0].LastInsertID != 3 || b[1].LastInsertID != 4 {
		t.Fatalf("batch last_row_id = %+v, %v", b, err)
	}
}
