package dosupervisor

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cellstore"
	"cellhive/internal/replica"
	"cellhive/internal/wal"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPollStableWALRetainsCheckpoint(t *testing.T) {
	calls := 0
	r, err := pollStableWAL(t.Context(), func() (wal.PollResult, error) {
		calls++
		if calls == 1 {
			return wal.PollResult{Checkpoint: true}, wal.ErrChecksum
		}
		return wal.PollResult{Transactions: []wal.Transaction{{DBSize: 2}}}, nil
	})
	if err != nil || !r.Checkpoint || len(r.Transactions) != 1 || calls != 2 {
		t.Fatalf("result=%+v calls=%d err=%v", r, calls, err)
	}
}

func TestGateCorruptWALDoesNotCommitAndRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.sqlite")
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(a); INSERT INTO t VALUES(1); PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	fc := &fakeCommitter{}
	s := &Supervisor{Dir: filepath.Dir(path), Epoch: 1, ScopePrefix: "workerd/__do__", Committer: fc}
	if err := s.syncFile(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE t SET a=2"); err != nil {
		t.Fatal(err)
	}
	if err := s.syncFile(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	_, before := fc.counts()
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE); UPDATE t SET a=3"); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), good...)
	bad[len(bad)-1] ^= 0xff
	if err := os.WriteFile(path+"-wal", bad, 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.syncFile(t.Context(), path); !errors.Is(err, wal.ErrChecksum) {
		t.Fatalf("corruption was acknowledged: %v", err)
	}
	if _, after := fc.counts(); after != before {
		t.Fatal("corrupt frame committed")
	}
	if !s.fileFor(path).checkpointPending {
		t.Fatal("failed poll lost checkpoint")
	}
	if err := os.WriteFile(path+"-wal", good, 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.syncFile(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	if _, after := fc.counts(); after <= before {
		t.Fatal("repaired WAL was skipped")
	}
	if s.fileFor(path).checkpointPending {
		t.Fatal("completed snapshot retained checkpoint")
	}
}

func TestPollStableWALPersistentCorruptionFailsClosed(t *testing.T) {
	calls := 0
	_, err := pollStableWAL(t.Context(), func() (wal.PollResult, error) { calls++; return wal.PollResult{}, wal.ErrChecksum })
	if !errors.Is(err, wal.ErrChecksum) || calls < 2 || calls > 8 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestPollStableWALCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	_, err := pollStableWAL(ctx, func() (wal.PollResult, error) { cancel(); return wal.PollResult{}, wal.ErrChecksum })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// A successful gate must persist both the checkpoint image and the new WAL,
// without depending on another request to trigger a second capture.
func TestCheckpointGateCapturesNewWALBeforeAcknowledgment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "host.sqlite")
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(a); INSERT INTO t VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	s := &Supervisor{Dir: dir, Epoch: 1, ScopePrefix: "workerd/__do__", Bucket: b, Store: rep, Committer: &bucketCommitter{rep: rep}}
	if _, err = s.SyncAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("PRAGMA wal_checkpoint(TRUNCATE); UPDATE t SET a=42"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SyncAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	restored := t.TempDir()
	if _, err = s.RestoreAll(t.Context(), restored); err != nil {
		t.Fatal(err)
	}
	check, err := cellstore.Open(filepath.Join(restored, "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var got int
	if err = check.QueryRow("SELECT a FROM t").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("acknowledged gate restored stale checkpoint: got %d, want 42", got)
	}
}
