package dosupervisor

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

type failingCapture struct {
	*bucketCommitter
	fail, persist bool
	failed        []byte
	replayed      bool
}

func (f *failingCapture) Commit(ctx context.Context, sc cell.Scope, epoch uint64, seg []byte) error {
	if f.fail {
		f.fail = false
		f.failed = append([]byte(nil), seg...)
		if f.persist {
			if err := f.bucketCommitter.Commit(ctx, sc, epoch, seg); err != nil {
				return err
			}
		}
		return errors.New("injected lost capture acknowledgement")
	}
	if f.failed != nil && !f.replayed {
		if !bytes.Equal(f.failed, seg) {
			return errors.New("failed LTX was skipped or changed")
		}
		f.replayed = true
	}
	return f.bucketCommitter.Commit(ctx, sc, epoch, seg)
}

func TestFailedCaptureRetainsExactLTX(t *testing.T) {
	for _, phase := range []string{"baseline", "delta"} {
		for _, persist := range []bool{false, true} {
			t.Run(phase+map[bool]string{false: "-rejected", true: "-ack-lost"}[persist], func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				path := filepath.Join(dir, "h.sqlite")
				db, err := cellstore.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				for _, sql := range []string{"PRAGMA journal_mode=WAL", "PRAGMA wal_autocheckpoint=0", "CREATE TABLE t(a)", "INSERT INTO t VALUES(1)", "PRAGMA wal_checkpoint(TRUNCATE)"} {
					if _, err := db.Exec(sql); err != nil {
						t.Fatal(err)
					}
				}
				b, err := bucket.NewFSBucket(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				rep := replica.New(b)
				fc := &failingCapture{bucketCommitter: &bucketCommitter{rep: rep, epoch: 1}, persist: persist}
				sup := &Supervisor{Dir: dir, Epoch: 1, ScopePrefix: "workerd/__do__", Committer: fc}
				if phase == "delta" {
					if err := sup.syncFile(ctx, path); err != nil {
						t.Fatal(err)
					}
					if _, err := db.Exec("UPDATE t SET a=2"); err != nil {
						t.Fatal(err)
					}
				}
				fc.fail = true
				if err := sup.syncFile(ctx, path); err == nil {
					t.Fatal("failure acknowledged")
				}
				if _, err := db.Exec("UPDATE t SET a=3"); err != nil {
					t.Fatal(err)
				}
				if err := sup.syncFile(ctx, path); err != nil {
					t.Fatalf("next gate: %v", err)
				}
				if !fc.replayed {
					t.Fatal("failed capture skipped")
				}
				segs, err := rep.Restore(ctx, sup.fileFor(path).scope, 1)
				if err != nil {
					t.Fatal(err)
				}
				raw := make([][]byte, len(segs))
				for i, s := range segs {
					raw[i] = s.Raw
				}
				restored := filepath.Join(t.TempDir(), "restored.sqlite")
				if _, err := restore.ApplyFile(restored, raw); err != nil {
					t.Fatal(err)
				}
				restoredDB, err := cellstore.Open(restored)
				if err != nil {
					t.Fatal(err)
				}
				defer restoredDB.Close()
				var got int
				if err := restoredDB.QueryRow("SELECT a FROM t").Scan(&got); err != nil {
					t.Fatal(err)
				}
				if got != 3 {
					t.Fatalf("restored value=%d want 3", got)
				}
			})
		}
	}
}
