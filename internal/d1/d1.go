// Package d1 implements the D1-compatible SQL API over per-database cell SQLite
// (docs/bindings.md). Each D1 database is a cell at <ns>/__d1__/<db>; queries run
// against its SQLite file, so the data is replicated and fenced like any cell.
package d1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

// Store runs D1 statements over cell databases.
type Store struct {
	cs *cellstore.Store
}

const (
	MaxSQLBytes    = 100 * 1024
	MaxParams      = 100
	MaxValueBytes  = 2 * 1024 * 1024
	MaxQueryTime   = 30 * time.Second
	MaxResultRows  = 100000
	MaxResultBytes = 32 << 20
)

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
	c, err := s.cs.Cell(ctx, Scope(ns, db))
	if err != nil {
		return nil, err
	}
	return c, nil
}

// Statement is one parameterized statement in a batch.
type Statement struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params,omitempty"`
}

// Result is one statement's outcome. Query results carry Columns/Rows; mutations
// carry RowsAffected.
type Result struct {
	Columns        []string `json:"columns,omitempty"`
	Rows           [][]any  `json:"rows,omitempty"`
	RowsAffected   int64    `json:"rows_affected"`
	LastInsertID   int64    `json:"last_row_id"`
	DurationMs     float64  `json:"duration_ms"`
	StatementCount int      `json:"statement_count"`
	Bookmark       string   `json:"bookmark,omitempty"`
}

// SQLStatementCount counts top-level semicolon-delimited statements for the
// D1 exec() result. It ignores semicolons in quoted strings and SQL comments.
func SQLStatementCount(sqlText string) int {
	count, start := 0, false
	quote, lineComment, blockComment := byte(0), false, false
	for i := 0; i < len(sqlText); i++ {
		c := sqlText[i]
		if lineComment {
			if c == '\n' {
				lineComment = false
			}
			continue
		}
		if blockComment {
			if c == '*' && i+1 < len(sqlText) && sqlText[i+1] == '/' {
				blockComment = false
				i++
			}
			continue
		}
		if quote != 0 {
			if c == quote {
				if i+1 < len(sqlText) && sqlText[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if c == '-' && i+1 < len(sqlText) && sqlText[i+1] == '-' {
			lineComment = true
			i++
			continue
		}
		if c == '/' && i+1 < len(sqlText) && sqlText[i+1] == '*' {
			blockComment = true
			i++
			continue
		}
		if c == '\'' || c == '"' || c == '`' {
			quote = c
			start = true
			continue
		}
		if c == ';' {
			if start {
				count++
				start = false
			}
			continue
		}
		if c > ' ' {
			start = true
		}
	}
	if start {
		count++
	}
	return count
}

// Query runs a single statement and returns its rows (or affected count for a
// mutation), mirroring D1's prepare().all()/run().
func (s *Store) Query(ctx context.Context, ns, db, sqlText string, params []any) (Result, error) {
	if err := ValidatePreparedSQL(sqlText, params); err != nil {
		return Result{}, err
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, MaxQueryTime)
	defer cancel()
	c, err := s.cell(ctx, ns, db)
	if err != nil {
		return Result{}, err
	}
	// Reads skip the capture advance; anything else is a mutation and must
	// advance the cell txid in the same commit so capture's Wait() covers it.
	// IsReadOnly (not returnsRows) decides this: a WITH statement returns rows
	// but may still mutate.
	if IsReadOnly(sqlText) {
		res, err := runOne(ctx, c.DB, nil, sqlText, params)
		if err == nil {
			res.Bookmark = bookmark(ctx, c)
		}
		return res, err
	}
	var res Result
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		var e error
		res, e = runOne(ctx, nil, tx, sqlText, params)
		return e
	})
	if err == nil {
		res.Bookmark = bookmark(ctx, c)
	}
	return res, err
}

// Exec runs a single mutation statement and returns the affected count.
func (s *Store) Exec(ctx context.Context, ns, db, sqlText string, params []any) (Result, error) {
	if err := ValidateExecSQL(sqlText, params); err != nil {
		return Result{}, err
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, MaxQueryTime)
	defer cancel()
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
	return Result{RowsAffected: n, LastInsertID: id, DurationMs: msSince(start), StatementCount: SQLStatementCount(sqlText), Bookmark: bookmark(ctx, c)}, nil
}

// Batch runs several statements in one transaction, so a failure rolls all back.
func (s *Store) Batch(ctx context.Context, ns, db string, stmts []Statement) ([]Result, error) {
	if len(stmts) == 0 {
		return nil, fmt.Errorf("d1: batch requires at least one statement")
	}
	for _, st := range stmts {
		if err := ValidatePreparedSQL(st.SQL, st.Params); err != nil {
			return nil, err
		}
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, MaxQueryTime)
	defer cancel()
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
	mark := bookmark(ctx, c)
	for i := range out {
		out[i].Bookmark = mark
	}
	return out, nil
}

// ValidateInput applies the common Cloudflare D1 statement/value limits before
// SQLite sees the request. The limits prevent a large body from becoming an
// unbounded SQLite/JS result allocation.
func ValidateInput(sqlText string, params []any) error {
	if len(sqlText) > MaxSQLBytes {
		return fmt.Errorf("d1: SQL statement exceeds %d bytes", MaxSQLBytes)
	}
	if len(params) > MaxParams {
		return fmt.Errorf("d1: too many parameters: %d > %d", len(params), MaxParams)
	}
	for _, p := range params {
		size := 0
		switch v := p.(type) {
		case string:
			size = len(v)
		case bool, nil, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			// Scalar values are accepted by the D1 binding contract.
		case []byte:
			size = len(v)
		case []any:
			for _, b := range v {
				f, ok := b.(float64)
				if !ok || f < 0 || f > 255 || f != float64(byte(f)) {
					return fmt.Errorf("d1: unsupported parameter type %T", p)
				}
			}
			size = len(v)
		default:
			return fmt.Errorf("d1: unsupported parameter type %T", p)
		}
		if size > MaxValueBytes {
			return fmt.Errorf("d1: bound value exceeds %d bytes", MaxValueBytes)
		}
	}
	return nil
}

func ValidatePreparedSQL(sqlText string, params []any) error {
	if err := ValidateInput(sqlText, params); err != nil {
		return err
	}
	if SQLStatementCount(sqlText) != 1 {
		return fmt.Errorf("d1: prepared statement must contain exactly one SQL statement")
	}
	words := sqlWords(sqlText)
	for _, word := range words {
		switch word {
		case "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "ATTACH", "DETACH", "VACUUM":
			return fmt.Errorf("d1: SQL statement %s is not supported", word)
		case "LOAD_EXTENSION":
			return fmt.Errorf("d1: load_extension is not supported")
		}
	}
	for i := 0; i+1 < len(words); i++ {
		if words[i] == "CREATE" && (words[i+1] == "TEMP" || words[i+1] == "TEMPORARY") {
			return fmt.Errorf("d1: temporary objects are not supported")
		}
	}
	return nil
}

func ValidateExecSQL(sqlText string, params []any) error {
	if err := ValidateInput(sqlText, params); err != nil {
		return err
	}
	words := sqlWords(sqlText)
	for _, word := range words {
		switch word {
		case "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "ATTACH", "DETACH", "VACUUM":
			return fmt.Errorf("d1: SQL statement %s is not supported", word)
		case "LOAD_EXTENSION":
			return fmt.Errorf("d1: load_extension is not supported")
		}
	}
	for i := 0; i+1 < len(words); i++ {
		if words[i] == "CREATE" && (words[i+1] == "TEMP" || words[i+1] == "TEMPORARY") {
			return fmt.Errorf("d1: temporary objects are not supported")
		}
	}
	return nil
}

func sqlWords(sqlText string) []string {
	var words []string
	var b strings.Builder
	quote, line, block := byte(0), false, false
	flush := func() {
		if b.Len() > 0 {
			words = append(words, strings.ToUpper(b.String()))
			b.Reset()
		}
	}
	for i := 0; i < len(sqlText); i++ {
		c := sqlText[i]
		if line {
			if c == '\n' {
				line = false
			}
			continue
		}
		if block {
			if c == '*' && i+1 < len(sqlText) && sqlText[i+1] == '/' {
				block = false
				i++
			}
			continue
		}
		if quote != 0 {
			if c == quote {
				if i+1 < len(sqlText) && sqlText[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if c == '-' && i+1 < len(sqlText) && sqlText[i+1] == '-' {
			flush()
			line = true
			i++
			continue
		}
		if c == '/' && i+1 < len(sqlText) && sqlText[i+1] == '*' {
			flush()
			block = true
			i++
			continue
		}
		if c == '\'' || c == '"' || c == '`' {
			flush()
			quote = c
			continue
		}
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' {
			b.WriteByte(c)
		} else {
			flush()
		}
	}
	flush()
	return words
}

func bookmark(ctx context.Context, c *cellstore.Cell) string {
	id, err := c.TxID(ctx)
	if err != nil {
		return ""
	}
	return strconv.FormatUint(id, 10)
}

// Migration is an applied SQL file. The filename is its stable identity.
type Migration struct {
	Name      string `json:"name"`
	AppliedAt int64  `json:"applied_at"`
}

// ListMigrations is read-only, including before the first migration.
func (s *Store) ListMigrations(ctx context.Context, ns, db string) ([]Migration, error) {
	return s.ListMigrationsTable(ctx, ns, db, "d1_migrations")
}

func migrationTable(table string) (string, error) {
	if table == "" || len(table) > 128 || strings.ContainsRune(table, 0) || strings.HasPrefix(strings.ToLower(table), "sqlite_") {
		return "", fmt.Errorf("d1: invalid migrations table")
	}
	return `"` + strings.ReplaceAll(table, `"`, `""`) + `"`, nil
}

func (s *Store) ListMigrationsTable(ctx context.Context, ns, db, table string) ([]Migration, error) {
	quoted, err := migrationTable(table)
	if err != nil {
		return nil, err
	}
	c, err := s.cell(ctx, ns, db)
	if err != nil {
		return nil, err
	}
	out := []Migration{}
	var present int
	if err := c.DB.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&present); err != nil {
		return nil, err
	}
	if present == 0 {
		return out, nil
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT name, unixepoch(applied_at) FROM `+quoted+` ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var m Migration
		if err := rows.Scan(&m.Name, &m.AppliedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ApplyMigration commits a whole SQL file and its tracking row together.
// Retrying after an uncertain HTTP response is safe: the tracking row and SQL
// are in the same cell transaction, including the cell txid capture advance.
func (s *Store) ApplyMigration(ctx context.Context, ns, db, name, sqlText string) (bool, error) {
	return s.ApplyMigrationTable(ctx, ns, db, "d1_migrations", name, sqlText)
}

func (s *Store) ApplyMigrationTable(ctx context.Context, ns, db, table, name, sqlText string) (bool, error) {
	if name == "" || strings.ContainsAny(name, `/\\`) || !strings.HasSuffix(name, ".sql") || strings.TrimSpace(sqlText) == "" {
		return false, fmt.Errorf("d1: invalid migration name or empty SQL")
	}
	quoted, err := migrationTable(table)
	if err != nil {
		return false, err
	}
	c, err := s.cell(ctx, ns, db)
	if err != nil {
		return false, err
	}
	applied := false
	// Wrangler strips surrounding whitespace before sending the migration SQL.
	// Normalize that difference so Wrangler and the native CLI identify the
	// same file by the same digest.
	digest := sha256.Sum256([]byte(strings.TrimSpace(sqlText)))
	hash := hex.EncodeToString(digest[:])
	legacyDigest := sha256.Sum256([]byte(sqlText))
	legacyHash := hex.EncodeToString(legacyDigest[:])
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+quoted+` (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS __cellhive_migration_hashes_v2 (table_name TEXT NOT NULL, name TEXT NOT NULL, sha256 TEXT NOT NULL, PRIMARY KEY(table_name, name))`); err != nil {
			return err
		}
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM `+quoted+` WHERE name=?`, name).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			var stored string
			err := tx.QueryRowContext(ctx, `SELECT sha256 FROM __cellhive_migration_hashes_v2 WHERE table_name=? AND name=?`, table, name).Scan(&stored)
			if err != nil && err != sql.ErrNoRows {
				return err
			}
			if err == sql.ErrNoRows && table == "d1_migrations" {
				var legacy int
				if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='__cellhive_migration_hashes'`).Scan(&legacy); err != nil {
					return err
				}
				if legacy != 0 {
					err = tx.QueryRowContext(ctx, `SELECT sha256 FROM __cellhive_migration_hashes WHERE name=?`, name).Scan(&stored)
				}
			}
			if err == nil && stored != hash && stored != legacyHash {
				return fmt.Errorf("d1: migration %q content changed after apply", name)
			}
			return nil
		}
		if _, err := tx.ExecContext(ctx, sqlText); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+quoted+`(name) VALUES(?)`, name); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO __cellhive_migration_hashes_v2(table_name, name, sha256) VALUES(?, ?, ?)`, table, name, hash); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

// Import executes an uploaded SQL file once per content digest. A retry after
// a lost API response cannot apply the same file twice.
func (s *Store) Import(ctx context.Context, ns, db, digest, sqlText string) (bool, error) {
	if digest == "" || strings.TrimSpace(sqlText) == "" {
		return false, fmt.Errorf("d1: import digest and SQL are required")
	}
	actual := sha256.Sum256([]byte(sqlText))
	if digest != hex.EncodeToString(actual[:]) {
		return false, fmt.Errorf("d1: import digest does not match SQL")
	}
	c, err := s.cell(ctx, ns, db)
	if err != nil {
		return false, err
	}
	applied := false
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS __cellhive_d1_imports (digest TEXT PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)`); err != nil {
			return err
		}
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM __cellhive_d1_imports WHERE digest=?`, digest).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			return nil
		}
		if _, err := tx.ExecContext(ctx, sqlText); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO __cellhive_d1_imports(digest) VALUES(?)`, digest); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
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
	// WITH and PRAGMA are conservatively treated as writes by IsReadOnly for
	// durability, but row-producing forms may be read-only. Compare connection-
	// local total_changes() around the statement before exposing mutation meta.
	var changesBefore int64
	trackMutation := !IsReadOnly(sqlText) && tx != nil
	if trackMutation {
		if err := tx.QueryRowContext(ctx, `SELECT total_changes()`).Scan(&changesBefore); err != nil {
			return Result{}, err
		}
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
	resultBytes := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return Result{}, err
		}
		rowBytes, expandedBytes := resultRowBytes(vals)
		if rowBytes > MaxValueBytes {
			return Result{}, fmt.Errorf("d1: result row exceeds %d bytes", MaxValueBytes)
		}
		if len(out.Rows) >= MaxResultRows {
			return Result{}, fmt.Errorf("d1: result exceeds %d rows", MaxResultRows)
		}
		if expandedBytes > MaxResultBytes-resultBytes {
			return Result{}, fmt.Errorf("d1: result exceeds %d bytes", MaxResultBytes)
		}
		resultBytes += expandedBytes
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				encoded := make([]int, len(b))
				for j, value := range b {
					encoded[j] = int(value)
				}
				vals[i] = encoded
			}
		}
		out.Rows = append(out.Rows, vals)
	}
	if err := rows.Err(); err != nil {
		return Result{}, err
	}
	if err := rows.Close(); err != nil {
		return Result{}, err
	}
	out.DurationMs = msSince(start)
	if trackMutation {
		var changesAfter int64
		if err := tx.QueryRowContext(ctx, `SELECT total_changes()`).Scan(&changesAfter); err != nil {
			return Result{}, err
		}
		if changesAfter > changesBefore {
			if err := tx.QueryRowContext(ctx, `SELECT changes(), last_insert_rowid()`).Scan(&out.RowsAffected, &out.LastInsertID); err != nil {
				return Result{}, err
			}
		}
	}
	return out, nil
}

// resultRowBytes measures SQLite payload and the retained result representation.
// BLOBs become []int for the D1 wire contract, so charge their expanded size
// before allocating that slice. Numeric SQLite values also consume budget.
func resultRowBytes(row []any) (payload, retained int) {
	for _, value := range row {
		switch v := value.(type) {
		case string:
			payload += len(v)
			retained += len(v)
		case []byte:
			payload += len(v)
			retained += len(v) * (strconv.IntSize / 8)
		case int64, float64:
			payload += 8
			retained += 8
		case bool:
			payload++
			retained++
		}
	}
	return
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
	// A data-modifying statement with RETURNING also yields rows; it must run as
	// a query or the rows are silently dropped by Exec (the gate that chooses
	// Cell.Tx is IsReadOnly, independent of this).
	return containsWord(up, "RETURNING")
}

// containsWord reports whether word occurs in up delimited by non-word bytes.
func containsWord(up, word string) bool {
	for i := 0; ; {
		j := strings.Index(up[i:], word)
		if j < 0 {
			return false
		}
		j += i
		before := j == 0 || !isWordByte(up[j-1])
		after := j+len(word) >= len(up) || !isWordByte(up[j+len(word)])
		if before && after {
			return true
		}
		i = j + len(word)
	}
}

// normalizeParams converts JSON numbers with integral values to int64 and D1's
// byte-array representation to []byte, matching the values accepted by
// Cloudflare D1PreparedStatement.bind(). Other arrays remain unsupported.
func normalizeParams(params []any) []any {
	out := make([]any, len(params))
	for i, p := range params {
		if f, ok := p.(float64); ok && f == float64(int64(f)) {
			out[i] = int64(f)
			continue
		}
		if values, ok := p.([]any); ok {
			bytes := make([]byte, len(values))
			valid := true
			for j, value := range values {
				f, ok := value.(float64)
				if !ok || f < 0 || f > 255 || f != float64(byte(f)) {
					valid = false
					break
				}
				bytes[j] = byte(f)
			}
			if valid {
				out[i] = bytes
				continue
			}
		}
		out[i] = p
	}
	return out
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }
