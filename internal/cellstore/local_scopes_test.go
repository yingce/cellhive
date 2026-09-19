package cellstore_test

import (
	"context"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

// TestLocalScopesAndPendingTimerMin covers the ADR-177 repair scanner: it lists
// every local cell and reads the earliest pending timer without migrating the
// database (a cell without a timers table reports has=false).
func TestLocalScopesAndPendingTimerMin(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	withTimer := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "with"}
	empty := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "empty"}
	noTable := cell.Scope{Namespace: "demo", Class: "n", ID: "plain"}
	for _, sc := range []cell.Scope{withTimer, empty, noTable} {
		c, err := cs.Open(ctx, sc)
		if err != nil {
			t.Fatalf("open %s: %v", sc, err)
		}
		switch sc {
		case withTimer:
			if _, err := c.DB.ExecContext(ctx, `CREATE TABLE timers(token TEXT PRIMARY KEY, due_ms INTEGER, kind TEXT, scope TEXT, occurrence TEXT)`); err != nil {
				t.Fatalf("timers table: %v", err)
			}
			if _, err := c.DB.ExecContext(ctx, `INSERT INTO timers(token,due_ms,kind,scope,occurrence) VALUES('t',1234,'kv-expire','demo/__kv__/with','kv-expire')`); err != nil {
				t.Fatalf("insert: %v", err)
			}
		case empty:
			if _, err := c.DB.ExecContext(ctx, `CREATE TABLE timers(token TEXT PRIMARY KEY, due_ms INTEGER, kind TEXT, scope TEXT, occurrence TEXT)`); err != nil {
				t.Fatalf("timers table: %v", err)
			}
		}
		if err := c.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	scopes, err := cs.LocalScopes()
	if err != nil {
		t.Fatalf("local scopes: %v", err)
	}
	if len(scopes) != 3 {
		t.Fatalf("local scopes = %v, want 3", scopes)
	}
	pathOf := func(sc cell.Scope) string {
		p, err := cs.Path(sc)
		if err != nil {
			t.Fatalf("path: %v", err)
		}
		return p
	}
	if min, has, err := cellstore.PendingTimerMin(ctx, pathOf(withTimer)); err != nil || !has || min != 1234 {
		t.Fatalf("withTimer min=%d has=%v err=%v, want 1234/true", min, has, err)
	}
	if min, has, err := cellstore.PendingTimerMin(ctx, pathOf(empty)); err != nil || !has || min != 0 {
		t.Fatalf("empty min=%d has=%v err=%v, want 0/true", min, has, err)
	}
	if _, has, err := cellstore.PendingTimerMin(ctx, pathOf(noTable)); err != nil || has {
		t.Fatalf("plain has=%v err=%v, want false", has, err)
	}
}
