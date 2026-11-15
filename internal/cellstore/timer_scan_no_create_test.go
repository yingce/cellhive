package cellstore_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

// The repair scanner may retain a path after ownership invalidation removed it.
// A read must not recreate an empty SQLite file and suppress the next hydrate.
func TestPendingTimerMinDoesNotRecreateEvictedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evicted.db")
	min, has, err := cellstore.PendingTimerMin(context.Background(), path)
	if err == nil || has || min != 0 {
		t.Fatalf("absent database: min=%d has=%v err=%v", min, has, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("timer scan recreated absent database: stat error=%v", err)
	}
}

func TestPendingTimerMinPreservesColdHydration(t *testing.T) {
	ctx := context.Background()
	s, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := cell.Scope{Namespace: "test", Class: "__d1__", ID: "business"}
	path, err := s.Path(sc)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	s.Hydrate = func(_ context.Context, _ cell.Scope, dest string) error {
		calls++
		db, err := cellstore.Open(dest)
		if err != nil {
			return err
		}
		defer db.Close()
		_, err = db.Exec("CREATE TABLE business_sentinel(value TEXT); INSERT INTO business_sentinel VALUES ('recovered')")
		return err
	}
	_, _, _ = cellstore.PendingTimerMin(ctx, path)
	c, err := s.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("hydrate calls=%d", calls)
	}
	var value string
	if err := c.DB.QueryRow("SELECT value FROM business_sentinel").Scan(&value); err != nil || value != "recovered" {
		t.Fatalf("value=%q err=%v", value, err)
	}
}

func TestPendingTimerMinReadsExistingJournalModes(t *testing.T) {
	for _, mode := range []string{"DELETE", "WAL"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "timer.db")
			db, err := sql.Open(cellstore.DriverName, "file:"+path+"?_journal_mode="+mode)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			if _, err = db.Exec("PRAGMA wal_autocheckpoint=0; CREATE TABLE timers(due_ms INTEGER); INSERT INTO timers VALUES(4321)"); err != nil {
				t.Fatal(err)
			}
			if mode == "WAL" {
				if info, err := os.Stat(path + "-wal"); err != nil || info.Size() == 0 {
					t.Fatalf("expected live WAL: %v", err)
				}
			}
			min, has, err := cellstore.PendingTimerMin(context.Background(), path)
			if err != nil || !has || min != 4321 {
				t.Fatalf("min=%d has=%v err=%v", min, has, err)
			}
			var journal string
			if err = db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil {
				t.Fatal(err)
			}
			if journal != map[string]string{"DELETE": "delete", "WAL": "wal"}[mode] {
				t.Fatalf("journal changed: %s", journal)
			}
			var tables int
			if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table'").Scan(&tables); err != nil || tables != 1 {
				t.Fatalf("schema changed tables=%d err=%v", tables, err)
			}
		})
	}
}

func TestPendingTimerMinReadsRelativePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timer.db")
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE timers(due_ms INTEGER); INSERT INTO timers VALUES(4321)"); err != nil {
		t.Fatal(err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(cwd, path)
	if err != nil {
		t.Fatal(err)
	}
	min, has, err := cellstore.PendingTimerMin(context.Background(), rel)
	if err != nil || !has || min != 4321 {
		t.Fatalf("min=%d has=%v err=%v", min, has, err)
	}
}
