package cellstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/cell"
)

// Keep the writer open so the fixture's metadata lives in a real WAL, not just
// the main file. The legacy schema also detects migration before validation.
func localValidationFixture(t *testing.T, setup string) (*Store, cell.Scope, string, *sql.DB) {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "local", Class: "test", ID: "watermark"}
	p, err := s.Path(sc)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE evidence(value TEXT); INSERT INTO evidence VALUES('preserve');` + setup); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); _ = db.Close() })
	return s, sc, p, db
}

func TestLocalValidationRejectsWithoutMigrationOrDeletion(t *testing.T) {
	remoteErr := errors.New("durable unavailable")
	for _, tc := range []struct {
		name, setup string
		durable     uint64
		remoteErr   error
	}{
		{"missing_table", "", 0, nil},
		{"missing_row", `CREATE TABLE cell_meta(k TEXT PRIMARY KEY,v TEXT);`, 0, nil},
		{"invalid", `CREATE TABLE cell_meta(k TEXT PRIMARY KEY,v TEXT); INSERT INTO cell_meta VALUES('txid','bad');`, 0, nil},
		{"negative", `CREATE TABLE cell_meta(k TEXT PRIMARY KEY,v TEXT); INSERT INTO cell_meta VALUES('txid','-1');`, 0, nil},
		{"overflow", `CREATE TABLE cell_meta(k TEXT PRIMARY KEY,v TEXT); INSERT INTO cell_meta VALUES('txid','18446744073709551616');`, 0, nil},
		{"behind", `CREATE TABLE cell_meta(k TEXT PRIMARY KEY,v TEXT); INSERT INTO cell_meta VALUES('txid','4');`, 5, nil},
		{"remote_error", `CREATE TABLE cell_meta(k TEXT PRIMARY KEY,v TEXT); INSERT INTO cell_meta VALUES('txid','4');`, 0, remoteErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, sc, p, db := localValidationFixture(t, tc.setup)
			before := make(map[string][]byte)
			for _, path := range []string{p, p + "-wal", p + "-shm"} {
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				before[path] = b
			}
			s.LocalDurable = func(context.Context, cell.Scope) (uint64, error) { return tc.durable, tc.remoteErr }
			s.Hydrate = func(context.Context, cell.Scope, string) error { t.Error("existing file hydrated"); return nil }
			c, err := s.Cell(context.Background(), sc)
			if err == nil || c != nil {
				t.Fatalf("activation accepted untrusted local file: cell=%v err=%v", c != nil, err)
			}
			if tc.remoteErr != nil && !errors.Is(err, tc.remoteErr) {
				t.Fatalf("lost remote error: %v", err)
			}
			if s.IsOpen(sc) {
				t.Fatal("rejected file became resident")
			}
			for path, b := range before {
				after, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				// SHM reader marks may change during a read-only query; it must remain.
				if path != p+"-shm" && !bytes.Equal(b, after) {
					t.Fatalf("modified %s", path)
				}
			}
			var n int
			if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='kv'`).Scan(&n); err != nil || n != 0 {
				t.Fatalf("migration ran: count=%d err=%v", n, err)
			}
		})
	}
}

func TestLocalValidationAllowsWatermarksAndCaches(t *testing.T) {
	for _, durable := range []uint64{0, 3, 4} {
		s, sc, _, _ := localValidationFixture(t, `CREATE TABLE cell_meta(k TEXT PRIMARY KEY,v TEXT); INSERT INTO cell_meta VALUES('txid','4');`)
		var calls atomic.Int32
		s.LocalDurable = func(_ context.Context, got cell.Scope) (uint64, error) {
			calls.Add(1)
			if got != sc {
				t.Error("wrong scope")
			}
			return durable, nil
		}
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, err := s.Cell(context.Background(), sc)
				if err != nil {
					t.Error(err)
					return
				}
				tx, err := c.TxID(context.Background())
				if err != nil || tx != 4 {
					t.Errorf("local watermark changed: %d %v", tx, err)
				}
			}()
		}
		wg.Wait()
		if _, err := s.Cell(context.Background(), sc); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 1 {
			t.Fatalf("cold singleflight/warm callback count=%d", calls.Load())
		}
	}
}

func TestLocalValidationColdTimeoutAndSlot(t *testing.T) {
	for _, held := range []bool{false, true} {
		s, sc, _, _ := localValidationFixture(t, `CREATE TABLE cell_meta(k TEXT PRIMARY KEY,v TEXT); INSERT INTO cell_meta VALUES('txid','4');`)
		s.HydrateTimeout = 30 * time.Millisecond
		s.HydrateParallel = 1
		if held {
			release, err := s.hydrateSlot(context.Background(), false)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
		}
		var calls int
		s.LocalDurable = func(ctx context.Context, _ cell.Scope) (uint64, error) { calls++; <-ctx.Done(); return 0, ctx.Err() }
		_, err := s.Cell(context.Background(), sc)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("timeout not enforced: %v", err)
		}
		if held && calls != 0 {
			t.Fatal("query bypassed hydrate slot")
		}
	}
}

func TestLocalValidationNewFileStillHydrates(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc := cell.Scope{Namespace: "local", Class: "test", ID: "new"}
	s.LocalDurable = func(context.Context, cell.Scope) (uint64, error) {
		t.Error("new file validated as existing")
		return 0, errors.New("unexpected")
	}
	var hydrated bool
	s.Hydrate = func(context.Context, cell.Scope, string) error { hydrated = true; return nil }
	if _, err := s.Cell(context.Background(), sc); err != nil {
		t.Fatal(err)
	}
	if !hydrated {
		t.Fatal("new file skipped hydrate")
	}
}
