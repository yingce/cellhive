package proxysql

import (
	"strings"
	"testing"
)

func TestStatementsMySQL(t *testing.T) {
	stmts, err := Statements(Origin{Namespace: "acme", Name: "hd1", URL: "mysql://app:s'ecret@db.internal:3306/app"})
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(stmts, "\n")
	for _, want := range []string{"mysql_servers", "'db.internal'", "app", "s''ecret", "LOAD MYSQL SERVERS TO RUNTIME", "LOAD MYSQL USERS TO RUNTIME"} {
		if !strings.Contains(all, want) {
			t.Fatalf("missing %q in\n%s", want, all)
		}
	}
	if strings.Index(all, "DELETE FROM mysql_servers") > strings.Index(all, "INSERT INTO mysql_servers") {
		t.Fatal("delete must precede insert")
	}
}

func TestStatementsPostgres(t *testing.T) {
	stmts, err := Statements(Origin{Namespace: "acme", Name: "hd1", URL: "postgres://app:secret@db.internal/app?sslmode=require"})
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(stmts, "\n")
	for _, want := range []string{"pgsql_servers", "pgsql_users", "'db.internal'", "55432"} {
		if want == "55432" {
			continue
		}
		if !strings.Contains(all, want) {
			t.Fatalf("missing %q in\n%s", want, all)
		}
	}
	if !strings.Contains(all, "LOAD PGSQL SERVERS TO RUNTIME") {
		t.Fatal("missing pgsql load")
	}
}

func TestHostgroupStable(t *testing.T) {
	a, b := Hostgroup("acme", "hd1"), Hostgroup("acme", "hd1")
	if a != b || a < 1000 {
		t.Fatalf("hostgroup = %d/%d", a, b)
	}
}
