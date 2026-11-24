package queue

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"cellhive/internal/scheduleoutbox"
)

func TestSchedulingOutboxQueueVisibilityRetryAndDelete(t *testing.T) {
	ctx := t.Context()
	st := newStore(t)
	now := time.UnixMilli(10000)
	st.now = func() time.Time { return now }
	ids, err := st.SendBatch(ctx, "demo", "jobs", []SendMessage{{Body: []byte("hello"), ContentType: "text"}})
	if err != nil {
		t.Fatal(err)
	}
	c, err := st.cell(ctx, "demo", "jobs")
	if err != nil {
		t.Fatal(err)
	}
	read := func() scheduleoutbox.Snapshot {
		t.Helper()
		s, err := scheduleoutbox.Read(ctx, c.DB, scheduleoutbox.Queue)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	sent := read()
	if !sent.Pending || !sent.HasDue || sent.DueMs != 10000 {
		t.Fatalf("send: %+v", sent)
	}
	if _, err := st.Claim(ctx, "demo", "jobs", 10, 5000); err != nil {
		t.Fatal(err)
	}
	claimed := read()
	if claimed.DueMs != 15000 || claimed.Revision <= sent.Revision {
		t.Fatalf("claim: %+v", claimed)
	}
	if err := scheduleoutbox.Ack(ctx, c, scheduleoutbox.Queue, sent.Revision); err != nil {
		t.Fatal(err)
	}
	if !read().Pending {
		t.Fatal("stale ack lost claim")
	}
	if err := st.Retry(ctx, "demo", "jobs", ids[0], 20000); err != nil {
		t.Fatal(err)
	}
	retry := read()
	if retry.DueMs != 30000 || retry.Revision <= claimed.Revision {
		t.Fatalf("retry: %+v", retry)
	}
	aborted := errors.New("rollback")
	_, err = c.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM messages`); err != nil {
			return err
		}
		return aborted
	})
	if !errors.Is(err, aborted) || read() != retry {
		t.Fatalf("rollback lost message/outbox: %v %+v", err, read())
	}
	// Recreate the store after an unavailable publisher; durable pending work stays.
	reopened := New(st.cs)
	beforeTx, _ := c.TxID(ctx)
	if _, err := reopened.cell(ctx, "demo", "jobs"); err != nil {
		t.Fatal(err)
	}
	afterTx, _ := c.TxID(ctx)
	if read() != retry || beforeTx != afterTx {
		t.Fatalf("reopen changed state: %+v txid %d -> %d", read(), beforeTx, afterTx)
	}
	if err := scheduleoutbox.Ack(ctx, c, scheduleoutbox.Queue, retry.Revision); err != nil {
		t.Fatal(err)
	}
	if read().Pending {
		t.Fatal("ack failed")
	}
	if err := st.Ack(ctx, "demo", "jobs", ids[0]); err != nil {
		t.Fatal(err)
	}
	if got := read(); !got.Pending || got.HasDue {
		t.Fatalf("delete: %+v", got)
	}
}
