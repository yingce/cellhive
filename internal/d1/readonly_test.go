package d1

import "testing"

func TestIsReadOnly(t *testing.T) {
	read := []string{"SELECT 1", "select * from t", "  (SELECT 1)", "EXPLAIN SELECT 1", "EXPLAIN QUERY PLAN SELECT 1", "VALUES(1)", "SELECT", "select"}
	write := []string{"", "INSERT INTO t VALUES(1)", "UPDATE t SET a=1", "DELETE FROM t", "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", "PRAGMA journal_mode=WAL", "CREATE TABLE t(a)", "SELECTED"}
	for _, q := range read {
		if !IsReadOnly(q) {
			t.Errorf("IsReadOnly(%q) = false, want true", q)
		}
	}
	for _, q := range write {
		if IsReadOnly(q) {
			t.Errorf("IsReadOnly(%q) = true, want false", q)
		}
	}
}
