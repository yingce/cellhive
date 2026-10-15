package d1

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
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

func TestMigrationAtomicAndIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	rows, err := s.ListMigrations(ctx, "acme", "main")
	if err != nil || len(rows) != 0 {
		t.Fatalf("empty list = %v, %v", rows, err)
	}
	good := `CREATE TABLE migrated (v TEXT); INSERT INTO migrated VALUES ('once');`
	applied, err := s.ApplyMigration(ctx, "acme", "main", "0001_init.sql", good)
	if err != nil || !applied {
		t.Fatalf("first apply = %v, %v", applied, err)
	}
	applied, err = s.ApplyMigration(ctx, "acme", "main", "0001_init.sql", good)
	if err != nil || applied {
		t.Fatalf("retry = %v, %v", applied, err)
	}
	if _, err := s.ApplyMigration(ctx, "acme", "main", "0001_init.sql", good+" SELECT 1;"); err == nil {
		t.Fatal("changed migration was silently skipped")
	}
	rows, err = s.ListMigrations(ctx, "acme", "main")
	if err != nil || len(rows) != 1 || rows[0].Name != "0001_init.sql" {
		t.Fatalf("list = %v, %v", rows, err)
	}
	q, err := s.Query(ctx, "acme", "main", `SELECT count(*) FROM migrated`, nil)
	if err != nil || q.Rows[0][0] != int64(1) {
		t.Fatalf("duplicate SQL execution = %v, %v", q, err)
	}
	_, err = s.ApplyMigration(ctx, "acme", "main", "0002_bad.sql", `INSERT INTO migrated VALUES ('rollback'); INSERT INTO missing VALUES (1);`)
	if err == nil {
		t.Fatal("bad migration succeeded")
	}
	q, err = s.Query(ctx, "acme", "main", `SELECT count(*) FROM migrated`, nil)
	if err != nil || q.Rows[0][0] != int64(1) {
		t.Fatalf("failed migration left data = %v, %v", q, err)
	}
	rows, err = s.ListMigrations(ctx, "acme", "main")
	if err != nil || len(rows) != 1 {
		t.Fatalf("failed migration left tracking row = %v, %v", rows, err)
	}
}

func TestImportChecksDigestAndRetriesOnce(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	script := "CREATE TABLE imported(v TEXT); INSERT INTO imported VALUES ('once');"
	if _, err := s.Import(ctx, "acme", "main", "wrong", script); err == nil {
		t.Fatal("import accepted a mismatched digest")
	}
	hash := sha256.Sum256([]byte(script))
	digest := hex.EncodeToString(hash[:])
	for attempt := range 2 {
		applied, err := s.Import(ctx, "acme", "main", digest, script)
		if err != nil || applied != (attempt == 0) {
			t.Fatalf("attempt %d = %v, %v", attempt, applied, err)
		}
	}
	q, err := s.Query(ctx, "acme", "main", "SELECT count(*) FROM imported", nil)
	if err != nil || q.Rows[0][0] != int64(1) {
		t.Fatalf("import retry changed data: %v, %v", q, err)
	}
}

func TestExportSQLRoundTripAndTableFilter(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for _, statement := range []string{
		`CREATE TABLE witness(v TEXT, b BLOB)`,
		`INSERT INTO witness VALUES ('O''Brien', X'00ff')`,
		`CREATE TABLE excluded(v TEXT)`,
	} {
		if _, err := s.Exec(ctx, "acme", "source", statement, nil); err != nil {
			t.Fatal(err)
		}
	}
	dump, err := s.ExportSQL(ctx, "acme", "source", []string{"witness"}, true, true)
	if err != nil || strings.Contains(string(dump), "excluded") {
		t.Fatalf("filtered export = %s, %v", dump, err)
	}
	if strings.Contains(string(dump), "BEGIN TRANSACTION") {
		t.Fatal("export nested a transaction inside D1 import")
	}
	digest := sha256.Sum256(dump)
	if _, err := s.Import(ctx, "acme", "restored", hex.EncodeToString(digest[:]), string(dump)); err != nil {
		t.Fatalf("restore: %v\n%s", err, dump)
	}
	res, err := s.Query(ctx, "acme", "restored", `SELECT v, hex(b) FROM witness`, nil)
	if err != nil || len(res.Rows) != 1 || res.Rows[0][0] != "O'Brien" || res.Rows[0][1] != "00FF" {
		t.Fatalf("restored values = %+v, %v", res, err)
	}
	schemaOnly, err := s.ExportSQL(ctx, "acme", "source", []string{"witness"}, true, false)
	if err != nil || !strings.Contains(string(schemaOnly), "CREATE TABLE witness") || strings.Contains(string(schemaOnly), "INSERT INTO") {
		t.Fatalf("schema-only export = %s, %v", schemaOnly, err)
	}
	dataOnly, err := s.ExportSQL(ctx, "acme", "source", []string{"witness"}, false, true)
	if err != nil || strings.Contains(string(dataOnly), "CREATE TABLE") || !strings.Contains(string(dataOnly), "O''Brien") {
		t.Fatalf("data-only export = %s, %v", dataOnly, err)
	}
	importSQL := "CREATE TABLE imported_by_file(v TEXT);"
	importHash := sha256.Sum256([]byte(importSQL))
	if _, err := s.Import(ctx, "acme", "source", hex.EncodeToString(importHash[:]), importSQL); err != nil {
		t.Fatal(err)
	}
	all, err := s.ExportSQL(ctx, "acme", "source", nil, true, true)
	if err != nil || strings.Contains(string(all), "__cellhive_d1_imports") {
		t.Fatalf("internal import ledger leaked into SQL export: %v, %s", err, all)
	}
	for _, statement := range []string{`CREATE TABLE seqtest(id INTEGER PRIMARY KEY AUTOINCREMENT)`, `INSERT INTO seqtest DEFAULT VALUES`, `INSERT INTO seqtest DEFAULT VALUES`, `DELETE FROM seqtest WHERE id=2`} {
		if _, err := s.Exec(ctx, "acme", "source", statement, nil); err != nil {
			t.Fatal(err)
		}
	}
	all, err = s.ExportSQL(ctx, "acme", "source", nil, true, true)
	if err != nil {
		t.Fatal(err)
	}
	allHash := sha256.Sum256(all)
	if _, err := s.Import(ctx, "acme", "restored_all", hex.EncodeToString(allHash[:]), string(all)); err != nil {
		t.Fatal(err)
	}
	inserted, err := s.Exec(ctx, "acme", "restored_all", `INSERT INTO seqtest DEFAULT VALUES`, nil)
	if err != nil || inserted.LastInsertID != 3 {
		t.Fatalf("autoincrement after restore = %+v, %v", inserted, err)
	}
}

func TestCustomMigrationTableIsIndependent(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for _, table := range []string{"d1_migrations", `custom"migrations`} {
		applied, err := s.ApplyMigrationTable(ctx, "acme", "main", table, "0001_init.sql", "CREATE TABLE IF NOT EXISTS shared(v TEXT);")
		if err != nil || !applied {
			t.Fatalf("apply %q = %v, %v", table, applied, err)
		}
		rows, err := s.ListMigrationsTable(ctx, "acme", "main", table)
		if err != nil || len(rows) != 1 || rows[0].Name != "0001_init.sql" {
			t.Fatalf("list %q = %v, %v", table, rows, err)
		}
		if applied, err := s.ApplyMigrationTable(ctx, "acme", "main", table, "0001_init.sql", "CREATE TABLE IF NOT EXISTS shared(v TEXT);\n"); err != nil || applied {
			t.Fatalf("whitespace retry %q = %v, %v", table, applied, err)
		}
	}
	if _, err := s.ApplyMigrationTable(ctx, "acme", "main", `custom"migrations`, "0001_init.sql", "CREATE TABLE changed(v TEXT);"); err == nil {
		t.Fatal("changed custom migration was accepted")
	}
	if _, err := s.ListMigrationsTable(ctx, "acme", "main", "sqlite_master"); err == nil {
		t.Fatal("reserved table name was accepted")
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

func TestD1CloudflareInputLimits(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Query(ctx, "acme", "main", strings.Repeat("x", MaxSQLBytes+1), nil); err == nil {
		t.Fatal("oversized SQL accepted")
	}
	params := make([]any, MaxParams+1)
	if _, err := s.Query(ctx, "acme", "main", "SELECT 1", params); err == nil {
		t.Fatal("too many params accepted")
	}
	if _, err := s.Query(ctx, "acme", "main", "SELECT ?", []any{strings.Repeat("x", MaxValueBytes+1)}); err == nil {
		t.Fatal("oversized value accepted")
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

func TestExecStatementCountIsNotRowsAffected(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Exec(ctx, "acme", "main", `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, "acme", "main", `INSERT INTO t(v) VALUES ('a'),('b'),('c')`, nil); err != nil {
		t.Fatal(err)
	}
	if got := SQLStatementCount(`UPDATE t SET v='z'`); got != 1 {
		t.Fatalf("single statement count = %d", got)
	}
	if got := SQLStatementCount("UPDATE t SET v='z';\nDELETE FROM t WHERE id=99;"); got != 2 {
		t.Fatalf("multi statement count = %d", got)
	}
}

func TestDumpReturnsSanitizedSQLite(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Exec(ctx, "acme", "main", `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(ctx, "acme", "main", `INSERT INTO users(name) VALUES ('alice')`, nil); err != nil {
		t.Fatal(err)
	}
	dump, err := s.Dump(ctx, "acme", "main")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(dump, []byte("SQLite format 3")) {
		t.Fatalf("dump header = %q", dump[:min(len(dump), 16)])
	}
	if bytes.Contains(dump, []byte("cell_meta")) || bytes.Contains(dump, []byte("__cellhive")) {
		t.Fatal("dump leaked CellHive internal tables")
	}
}

// TestD1ModifyingCTEAdvancesTxID is the regression for returnsRows treating every
// WITH statement as a read: a data-modifying CTE was executed without Cell.Tx, so
// the txid never advanced and RPO=0 capture could not cover it.
func TestD1ModifyingCTEAdvancesTxID(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	s := New(cs)
	if _, err := s.Exec(ctx, "acme", "main", `CREATE TABLE t (id INTEGER PRIMARY KEY)`, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Exec(ctx, "acme", "main", `INSERT INTO t (id) VALUES (1),(2),(3)`, nil); err != nil {
		t.Fatalf("insert: %v", err)
	}
	c, err := cs.Cell(ctx, Scope("acme", "main"))
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	before, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	if _, err := s.Query(ctx, "acme", "main",
		`WITH del AS (SELECT id FROM t WHERE id < 3) DELETE FROM t WHERE id IN (SELECT id FROM del)`, nil); err != nil {
		t.Fatalf("modifying cte: %v", err)
	}
	after, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	if after <= before {
		t.Fatalf("txid did not advance: %d -> %d", before, after)
	}
	q, err := s.Query(ctx, "acme", "main", `SELECT count(1) FROM t`, nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if q.Rows[0][0] != int64(1) {
		t.Fatalf("rows after delete = %v, want 1", q.Rows[0][0])
	}
}

// TestD1ReturningRows is the regression for returnsRows missing RETURNING: the
// rows of INSERT/UPDATE/DELETE ... RETURNING were dropped by Exec, and the
// statement's read/write gate (IsReadOnly) disagreed with the executor.
func TestD1ReturningRows(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Exec(ctx, "acme", "main", `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	r, err := s.Query(ctx, "acme", "main", `INSERT INTO t (id, v) VALUES (?, ?) RETURNING id, v`, []any{1, "a"})
	if err != nil {
		t.Fatalf("returning insert: %v", err)
	}
	if len(r.Rows) != 1 || len(r.Rows[0]) != 2 || r.Rows[0][1] != "a" {
		t.Fatalf("returning rows = %+v, want one [1 a]", r.Rows)
	}
	if r.RowsAffected != 1 || r.LastInsertID != 1 {
		t.Fatalf("returning metadata = changes %d last id %d, want 1/1", r.RowsAffected, r.LastInsertID)
	}
	// A plain mutation must still report affected rows (Exec path).
	ra, err := s.Exec(ctx, "acme", "main", `INSERT INTO t (id, v) VALUES (?, ?)`, []any{2, "b"})
	if err != nil || ra.RowsAffected != 1 {
		t.Fatalf("plain insert = %+v, %v", ra, err)
	}
}

func TestD1ReadOnlyWithDoesNotInheritMutationMeta(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Exec(ctx, "acme", "main", `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	results, err := s.Batch(ctx, "acme", "main", []Statement{
		{SQL: `INSERT INTO t (v) VALUES (?)`, Params: []any{"a"}},
		{SQL: `WITH current AS (SELECT id, v FROM t) SELECT id, v FROM current`},
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(results) != 2 || results[0].RowsAffected != 1 {
		t.Fatalf("batch results = %+v", results)
	}
	if results[1].RowsAffected != 0 || results[1].LastInsertID != 0 {
		t.Fatalf("read-only WITH inherited mutation meta: %+v", results[1])
	}
}

func TestD1JSONByteArrayBindsAsBlob(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Exec(ctx, "acme", "main", `CREATE TABLE blobs (v BLOB)`, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Exec(ctx, "acme", "main", `INSERT INTO blobs (v) VALUES (?)`, []any{[]any{float64(0), float64(127), float64(255)}}); err != nil {
		t.Fatalf("insert byte array: %v", err)
	}
	got, err := s.Query(ctx, "acme", "main", `SELECT typeof(v), v FROM blobs`, nil)
	if err != nil {
		t.Fatalf("query blob: %v", err)
	}
	if len(got.Rows) != 1 || got.Rows[0][0] != "blob" {
		t.Fatalf("blob row = %+v, want blob", got.Rows)
	}
	bytes, ok := got.Rows[0][1].([]int)
	if !ok || len(bytes) != 3 || bytes[0] != 0 || bytes[1] != 127 || bytes[2] != 255 {
		t.Fatalf("blob value = %#v, want [0 127 255]", got.Rows[0][1])
	}
}
