package workflow

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cellstore"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	return New(cs)
}

func TestCreateGetAndStatus(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	in, err := s.Create(ctx, "acme", "wf", "", []byte(`{"n":1}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if in.ID == "" || in.Status != StatusQueued {
		t.Fatalf("instance = %+v", in)
	}
	// Idempotent create with the same id returns the same instance.
	again, err := s.Create(ctx, "acme", "wf", in.ID, []byte(`{"n":9}`))
	if err != nil || again.ID != in.ID {
		t.Fatalf("idempotent create = %+v, %v", again, err)
	}
	if err := s.SetStatus(ctx, "acme", "wf", in.ID, StatusRunning, ""); err != nil {
		t.Fatalf("set status: %v", err)
	}
	got, err := s.Get(ctx, "acme", "wf", in.ID)
	if err != nil || got.Status != StatusRunning {
		t.Fatalf("get = %+v, %v", got, err)
	}
}

func TestStepsMemoizedAndEvents(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	in, _ := s.Create(ctx, "acme", "wf", "", nil)

	if _, ok, err := s.GetStep(ctx, "acme", "wf", in.ID, "a"); err != nil || ok {
		t.Fatalf("unexpected step before put: ok=%v err=%v", ok, err)
	}
	if err := s.PutStep(ctx, "acme", "wf", in.ID, "a", []byte(`"first"`)); err != nil {
		t.Fatalf("put step: %v", err)
	}
	// Second put must not overwrite (memoized).
	if err := s.PutStep(ctx, "acme", "wf", in.ID, "a", []byte(`"second"`)); err != nil {
		t.Fatalf("put step 2: %v", err)
	}
	got, ok, err := s.GetStep(ctx, "acme", "wf", in.ID, "a")
	if err != nil || !ok || string(got) != `"first"` {
		t.Fatalf("step = %q ok=%v err=%v, want memoized \"first\"", got, ok, err)
	}
	if err := s.AppendEvent(ctx, "acme", "wf", in.ID, []byte(`{"e":1}`)); err != nil {
		t.Fatalf("event: %v", err)
	}
	ev, err := s.Events(ctx, "acme", "wf", in.ID)
	if err != nil || len(ev) != 1 || string(ev[0]) != `{"e":1}` {
		t.Fatalf("events = %v, %v", ev, err)
	}
	if err := s.SetOutput(ctx, "acme", "wf", in.ID, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("output: %v", err)
	}
	final, _ := s.Get(ctx, "acme", "wf", in.ID)
	if final.Status != StatusComplete || string(final.Output) != `{"ok":true}` {
		t.Fatalf("final = %+v", final)
	}
}

func TestLifecycleAndRestart(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	in, _ := s.Create(ctx, "acme", "wf", "", nil)
	if err := s.Pause(ctx, "acme", "wf", in.ID); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if got, _ := s.Get(ctx, "acme", "wf", in.ID); got.Status != StatusPaused {
		t.Fatalf("status = %s, want paused", got.Status)
	}
	if err := s.Resume(ctx, "acme", "wf", in.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got, _ := s.Get(ctx, "acme", "wf", in.ID); got.Status != StatusQueued {
		t.Fatalf("status = %s, want queued", got.Status)
	}
	if err := s.Terminate(ctx, "acme", "wf", in.ID); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	if got, _ := s.Get(ctx, "acme", "wf", in.ID); got.Status != StatusTerminated {
		t.Fatalf("status = %s, want terminated", got.Status)
	}

	// Restart clears memoized steps and re-queues, clearing output/error.
	_ = s.PutStep(ctx, "acme", "wf", in.ID, "a", []byte(`1`))
	_ = s.SetOutput(ctx, "acme", "wf", in.ID, []byte(`{"done":true}`))
	if err := s.Restart(ctx, "acme", "wf", in.ID); err != nil {
		t.Fatalf("restart: %v", err)
	}
	got, _ := s.Get(ctx, "acme", "wf", in.ID)
	if got.Status != StatusQueued || got.Output != nil || got.Error != "" {
		t.Fatalf("after restart = %+v", got)
	}
	if _, ok, _ := s.GetStep(ctx, "acme", "wf", in.ID, "a"); ok {
		t.Fatal("restart did not clear memoized steps")
	}
	if list, err := s.List(ctx, "acme", "wf", 10); err != nil || len(list) != 1 {
		t.Fatalf("list = %v, %v", list, err)
	}
}

func TestRunLeaseAndFencing(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	in, _ := s.Create(ctx, "acme", "wf", "", nil)

	tok, gen, st, ok, err := s.ClaimRun(ctx, "acme", "wf", in.ID, 60_000)
	if err != nil || !ok || tok == "" || gen != 1 || st != StatusRunning {
		t.Fatalf("claim1 = %q gen=%d st=%s ok=%v err=%v", tok, gen, st, ok, err)
	}
	// A second claim while the lease is live is refused.
	if _, _, _, ok2, err2 := s.ClaimRun(ctx, "acme", "wf", in.ID, 60_000); ok2 || !errors.Is(err2, ErrRunActive) {
		t.Fatalf("claim2 = ok=%v err=%v, want ErrRunActive", ok2, err2)
	}
	// Stale token is fenced.
	if err := s.RenewRun(ctx, "acme", "wf", in.ID, "bogus", 60_000); !errors.Is(err, ErrStaleRun) {
		t.Fatalf("renew bogus = %v, want ErrStaleRun", err)
	}
	if _, err := s.RunStatus(ctx, "acme", "wf", in.ID, "bogus"); !errors.Is(err, ErrStaleRun) {
		t.Fatalf("status bogus = %v, want ErrStaleRun", err)
	}
	if err := s.RenewRun(ctx, "acme", "wf", in.ID, tok, 60_000); err != nil {
		t.Fatalf("renew: %v", err)
	}
	// Release frees the lease for a new attempt.
	if err := s.ReleaseRun(ctx, "acme", "wf", in.ID, tok); err != nil {
		t.Fatalf("release: %v", err)
	}
	tok2, gen2, _, ok3, _ := s.ClaimRun(ctx, "acme", "wf", in.ID, 60_000)
	if !ok3 || tok2 == tok || gen2 != 2 {
		t.Fatalf("claim after release = %q gen=%d ok=%v", tok2, gen2, ok3)
	}
	// Paused instances are not claimed.
	_ = s.Pause(ctx, "acme", "wf", in.ID)
	if _, _, st4, ok4, _ := s.ClaimRun(ctx, "acme", "wf", in.ID, 60_000); ok4 || st4 != StatusPaused {
		t.Fatalf("claim paused = ok=%v st=%s", ok4, st4)
	}
}

func TestPruneTerminalOlderThanRetention(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	old, _ := s.Create(ctx, "acme", "wf", "", nil)
	recent, _ := s.Create(ctx, "acme", "wf", "", nil)
	_ = s.PutStep(ctx, "acme", "wf", old.ID, "a", []byte(`1`))
	_ = s.SetOutput(ctx, "acme", "wf", old.ID, []byte(`done`)) // terminal
	_ = s.SetOutput(ctx, "acme", "wf", recent.ID, []byte(`done`))

	// Prune with cutoff now: both are "old" (updated_ms <= now), so use a cutoff
	// in the future only for the first by adjusting its updated_ms.
	future := time.Now().Add(time.Hour).UnixMilli()
	if n, err := s.Prune(ctx, "acme", "wf", future); err != nil || n != 2 {
		t.Fatalf("prune all-terminal = %d, %v", n, err)
	}
	if _, err := s.Get(ctx, "acme", "wf", old.ID); err == nil {
		t.Fatal("old instance survived prune")
	}
	if _, ok, _ := s.GetStep(ctx, "acme", "wf", old.ID, "a"); ok {
		t.Fatal("old step survived prune")
	}
	// A non-terminal instance is never pruned.
	live, _ := s.Create(ctx, "acme", "wf", "", nil)
	if n, _ := s.Prune(ctx, "acme", "wf", future); n != 0 {
		t.Fatalf("pruned live instance (%d)", n)
	}
	if _, err := s.Get(ctx, "acme", "wf", live.ID); err != nil {
		t.Fatalf("live instance gone: %v", err)
	}
}

// TestDeleteInstance covers instance.delete(): the instance and all of its
// persisted state (steps/attempts/events/waits) are removed, and deleting a
// missing instance is a no-op.
func TestDeleteInstance(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	in, err := s.Create(ctx, "acme", "wf", "", []byte(`{"n":1}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.PutStep(ctx, "acme", "wf", in.ID, "s1", []byte(`41`)); err != nil {
		t.Fatalf("put step: %v", err)
	}
	if err := s.SetAttempt(ctx, "acme", "wf", in.ID, "s1", 3, "boom"); err != nil {
		t.Fatalf("set attempt: %v", err)
	}
	if err := s.SetWait(ctx, "acme", "wf", in.ID, "e1", time.Now().Add(time.Minute).UnixMilli()); err != nil {
		t.Fatalf("set wait: %v", err)
	}
	if err := s.AppendEvent(ctx, "acme", "wf", in.ID, []byte(`{"go":1}`)); err != nil {
		t.Fatalf("append event: %v", err)
	}

	ok, err := s.Delete(ctx, "acme", "wf", in.ID)
	if !ok || err != nil {
		t.Fatalf("delete = %v, %v; want true, nil", ok, err)
	}
	if _, err := s.Get(ctx, "acme", "wf", in.ID); err == nil {
		t.Fatal("instance still present after delete")
	}
	if _, found, _ := s.GetStep(ctx, "acme", "wf", in.ID, "s1"); found {
		t.Fatal("step still present after delete")
	}
	if _, _, found, _ := s.GetAttempt(ctx, "acme", "wf", in.ID, "s1"); found {
		t.Fatal("attempt still present after delete")
	}
	if _, found, _ := s.GetWait(ctx, "acme", "wf", in.ID, "e1"); found {
		t.Fatal("wait still present after delete")
	}
	if evs, _ := s.Events(ctx, "acme", "wf", in.ID); len(evs) != 0 {
		t.Fatalf("events still present after delete: %d", len(evs))
	}
	// Idempotent: a second delete reports not-found without error.
	if ok, err := s.Delete(ctx, "acme", "wf", in.ID); ok || err != nil {
		t.Fatalf("second delete = %v, %v; want false, nil", ok, err)
	}
}

// TestReadOnlyCallsDoNotAdvanceTxID is the regression for re-running the schema
// and ALTER migrations on every call, which made pure reads advance the cell
// txid and produce capture deltas.
func TestReadOnlyCallsDoNotAdvanceTxID(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	s := New(cs)
	if _, err := s.List(ctx, "acme", "wf", 10); err != nil {
		t.Fatalf("list: %v", err)
	}
	c, err := cs.Cell(ctx, Scope("acme", "wf"))
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	before, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.List(ctx, "acme", "wf", 10); err != nil {
			t.Fatalf("list %d: %v", i, err)
		}
	}
	after, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	if after != before {
		t.Fatalf("read calls advanced txid: %d -> %d", before, after)
	}
}

func TestIsDuplicateColumn(t *testing.T) {
	if !isDuplicateColumn(errors.New("SQL logic error: duplicate column name: consumed")) {
		t.Fatal("duplicate column should be tolerated")
	}
	if isDuplicateColumn(errors.New("database is locked")) {
		t.Fatal("a real error must not be treated as duplicate column")
	}
	if isDuplicateColumn(nil) {
		t.Fatal("nil is not a duplicate column")
	}
}

// TestMigrationFailureNotCached is the regression for swallowing ALTER errors
// and marking the cell migrated anyway: a real migration failure must surface
// and must be retried on the next call.
func TestMigrationFailureNotCached(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	c, err := cs.Cell(ctx, Scope("acme", "wf"))
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	// A view named "events" makes CREATE TABLE IF NOT EXISTS a no-op and the
	// ALTER fail with a non-duplicate error.
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		_, e := tx.ExecContext(ctx, `CREATE VIEW events AS SELECT 1 AS x`)
		return e
	}); err != nil {
		t.Fatalf("seed view: %v", err)
	}
	s := New(cs)
	if _, err := s.List(ctx, "acme", "wf", 10); err == nil {
		t.Fatal("migration error was swallowed")
	}
	if _, err := s.List(ctx, "acme", "wf", 10); err == nil {
		t.Fatal("a failed migration must not be cached as done")
	}
}
