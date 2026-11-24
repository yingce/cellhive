package timer

import (
	"database/sql"
	"errors"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/scheduleoutbox"
)

func TestSchedulingOutboxAtomicRetryAndCancel(t *testing.T) {
	ctx := t.Context()
	st := newStore(t)
	read := func() scheduleoutbox.Snapshot {
		t.Helper()
		s, err := scheduleoutbox.Read(ctx, st.cell.DB, scheduleoutbox.Timer)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	initial := read()
	aborted := errors.New("abort producer transaction")
	tm := New(1000, KindCron, "demo/__cron__/worker", "slot")
	_, err := st.cell.Tx(ctx, func(tx *sql.Tx) error {
		if err := InsertTx(ctx, tx, tm); err != nil {
			return err
		}
		return aborted
	})
	if !errors.Is(err, aborted) {
		t.Fatal(err)
	}
	if got := read(); got != initial {
		t.Fatalf("rollback changed outbox: %+v -> %+v", initial, got)
	}
	if err := st.Upsert(ctx, tm); err != nil {
		t.Fatal(err)
	}
	published := read()
	if !published.Pending || !published.HasDue || published.DueMs != 1000 {
		t.Fatalf("insert: %+v", published)
	}
	if _, _, err := st.ScheduleRetry(ctx, tm.Token, 2000); err != nil {
		t.Fatal(err)
	}
	// A publisher completing an older revision cannot erase the retry.
	if err := scheduleoutbox.Ack(ctx, st.cell, scheduleoutbox.Timer, published.Revision); err != nil {
		t.Fatal(err)
	}
	retry := read()
	if !retry.Pending || retry.DueMs != 7000 || retry.Revision <= published.Revision {
		t.Fatalf("retry: %+v", retry)
	}
	if err := scheduleoutbox.Ack(ctx, st.cell, scheduleoutbox.Timer, retry.Revision); err != nil {
		t.Fatal(err)
	}
	if read().Pending {
		t.Fatal("successful acknowledgement stayed pending")
	}
	if err := st.RemoveByOccurrence(ctx, tm.Occurrence); err != nil {
		t.Fatal(err)
	}
	cancelled := read()
	if !cancelled.Pending || cancelled.HasDue {
		t.Fatalf("cancel: %+v", cancelled)
	}
}

func TestSchedulingOutboxFailedPublisherReopen(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	cs, err := cellstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	scope := cell.Scope{Namespace: "demo", Class: "__timers__", ID: "reopen"}
	c, err := cs.Open(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	st, err := NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Upsert(ctx, New(1000, KindCron, "demo/__cron__/worker", "slot")); err != nil {
		t.Fatal(err)
	}
	before, err := scheduleoutbox.Read(ctx, st.cell.DB, scheduleoutbox.Timer)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate process loss after a failed publication: close SQLite and use a
	// new cellstore instance, retaining only the on-disk database.
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	cs2, err := cellstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := cs2.Open(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	beforeTx, _ := c2.TxID(ctx)
	reopened, err := NewStore(ctx, c2)
	if err != nil {
		t.Fatal(err)
	}
	after, err := scheduleoutbox.Read(ctx, reopened.cell.DB, scheduleoutbox.Timer)
	if err != nil {
		t.Fatal(err)
	}
	afterTx, _ := c2.TxID(ctx)
	if beforeTx != afterTx {
		t.Fatalf("reopen wrote txid %d -> %d", beforeTx, afterTx)
	}
	if after != before || !after.Pending {
		t.Fatalf("lost pending work on reopen: %+v -> %+v", before, after)
	}
}

func TestSchedulingOutboxMigrationProjectsExistingTimer(t *testing.T) {
	ctx := t.Context()
	st := newStore(t)
	tm := New(4000, KindCron, "demo/__cron__/worker", "legacy")
	// Recreate the exact pre-outbox schema state with already persisted work.
	_, err := st.cell.Tx(ctx, func(tx *sql.Tx) error {
		for _, table := range []string{"timers", "timer_retry"} {
			for _, op := range []string{"INSERT", "UPDATE", "DELETE"} {
				if _, err := tx.ExecContext(ctx, "DROP TRIGGER schedule_outbox_"+table+"_"+op); err != nil {
					return err
				}
			}
		}
		if _, err := tx.ExecContext(ctx, `DROP TABLE schedule_outbox`); err != nil {
			return err
		}
		return InsertTx(ctx, tx, tm)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(ctx, st.cell); err != nil {
		t.Fatal(err)
	}
	snap, err := scheduleoutbox.Read(ctx, st.cell.DB, scheduleoutbox.Timer)
	if err != nil || !snap.Pending || !snap.HasDue || snap.DueMs != 4000 {
		t.Fatalf("migration: %+v %v", snap, err)
	}
}
