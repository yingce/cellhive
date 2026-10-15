package d1

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

const maxExportBytes = 32 << 20

// ExportSQL creates a consistent SQL snapshot. SQLite owns the schema text;
// values are escaped according to their actual SQLite storage class.
func (s *Store) ExportSQL(ctx context.Context, ns, db string, tables []string, schema, data bool) ([]byte, error) {
	if !schema && !data {
		return nil, fmt.Errorf("d1: export must include schema or data")
	}
	c, err := s.cell(ctx, ns, db)
	if err != nil {
		return nil, err
	}
	tx, err := c.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	wanted := map[string]bool{}
	for _, table := range tables {
		if table == "" {
			return nil, fmt.Errorf("d1: empty export table")
		}
		wanted[table] = true
	}
	type object struct{ name, kind, table, ddl string }
	rows, err := tx.QueryContext(ctx, `SELECT name, type, tbl_name, sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	var objects []object
	known := map[string]bool{}
	for rows.Next() {
		var o object
		if err := rows.Scan(&o.name, &o.kind, &o.table, &o.ddl); err != nil {
			rows.Close()
			return nil, err
		}
		if o.name == "cell_meta" || o.table == "cell_meta" || o.name == "kv" || o.table == "kv" || strings.HasPrefix(o.name, "__cellhive_") || strings.HasPrefix(o.table, "__cellhive_") {
			continue
		}
		if o.kind == "table" {
			known[o.name] = true
		}
		objects = append(objects, o)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for name := range wanted {
		if !known[name] {
			return nil, fmt.Errorf("d1: export table %q not found", name)
		}
	}
	var out bytes.Buffer
	// CellHive's D1 import already wraps a file in a fenced transaction. Defer
	// foreign-key checks until that transaction commits, including for tables
	// restored before the rows they reference.
	out.WriteString("PRAGMA defer_foreign_keys=ON;\n")
	write := func(s string) error {
		if out.Len()+len(s) > maxExportBytes {
			return fmt.Errorf("d1: SQL export exceeds 32 MiB")
		}
		_, _ = out.WriteString(s)
		return nil
	}
	for _, o := range objects {
		if o.kind != "table" || len(wanted) != 0 && !wanted[o.name] {
			continue
		}
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(o.ddl)), "CREATE VIRTUAL TABLE") {
			return nil, fmt.Errorf("d1: SQL export of virtual table %q is not supported", o.name)
		}
		if schema {
			if err := write(o.ddl + ";\n"); err != nil {
				return nil, err
			}
		}
		if !data {
			continue
		}
		quoted := quoteIdentifier(o.name)
		columns, err := tx.QueryContext(ctx, "PRAGMA table_info("+quoted+")")
		if err != nil {
			return nil, err
		}
		var names []string
		for columns.Next() {
			var cid, notnull, pk int
			var name, typ string
			var defaultValue sql.NullString
			if err := columns.Scan(&cid, &name, &typ, &notnull, &defaultValue, &pk); err != nil {
				columns.Close()
				return nil, err
			}
			names = append(names, quoteIdentifier(name))
		}
		if err := columns.Err(); err != nil {
			columns.Close()
			return nil, err
		}
		columns.Close()
		if len(names) == 0 {
			continue
		}
		dataRows, err := tx.QueryContext(ctx, "SELECT "+strings.Join(names, ",")+" FROM "+quoted)
		if err != nil {
			return nil, err
		}
		for dataRows.Next() {
			values := make([]any, len(names))
			ptrs := make([]any, len(names))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := dataRows.Scan(ptrs...); err != nil {
				dataRows.Close()
				return nil, err
			}
			literals := make([]string, len(values))
			for i, value := range values {
				literals[i] = sqlLiteral(value)
			}
			if err := write("INSERT INTO " + quoted + "(" + strings.Join(names, ",") + ") VALUES(" + strings.Join(literals, ",") + ");\n"); err != nil {
				dataRows.Close()
				return nil, err
			}
		}
		if err := dataRows.Err(); err != nil {
			dataRows.Close()
			return nil, err
		}
		dataRows.Close()
		if strings.Contains(strings.ToUpper(o.ddl), "AUTOINCREMENT") {
			var sequence int64
			if err := tx.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name=?`, o.name).Scan(&sequence); err == nil {
				if err := write("DELETE FROM sqlite_sequence WHERE name=" + sqlLiteral(o.name) + ";\nINSERT INTO sqlite_sequence(name,seq) VALUES(" + sqlLiteral(o.name) + "," + strconv.FormatInt(sequence, 10) + ");\n"); err != nil {
					return nil, err
				}
			} else if err != sql.ErrNoRows {
				return nil, err
			}
		}
	}
	if schema {
		for _, o := range objects {
			if o.kind == "table" || len(wanted) != 0 && !wanted[o.table] {
				continue
			}
			if err := write(o.ddl + ";\n"); err != nil {
				return nil, err
			}
		}
	}
	return out.Bytes(), nil
}

func quoteIdentifier(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func sqlLiteral(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case []byte:
		return "X'" + hex.EncodeToString(v) + "'"
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(v), "'", "''") + "'"
	}
}
