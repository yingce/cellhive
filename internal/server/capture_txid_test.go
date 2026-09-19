package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/ltx"
	"cellhive/internal/owner"
	"cellhive/internal/queue"
	"cellhive/internal/timer"
	"cellhive/internal/workflow"
)

// TestCapturedStoresAdvanceCellTxID pins the RPO=0 invariant behind
// capturedWrite: every store wrapped by it must advance the cell's txid, because
// capture's durability watermark only covers writes that bump it. Before this
// fix timer/queue/workflow/control wrote straight to c.DB and an ack could be
// released before the write had been captured (critical review finding).
func TestCapturedStoresAdvanceCellTxID(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()

	txid := func(scope cell.Scope) uint64 {
		t.Helper()
		c, err := cs.Cell(ctx, scope)
		if err != nil {
			t.Fatalf("cell %s: %v", scope, err)
		}
		n, err := c.TxID(ctx)
		if err != nil {
			t.Fatalf("txid %s: %v", scope, err)
		}
		return n
	}
	assertBumped := func(name string, scope cell.Scope, write func() error) {
		t.Helper()
		before := txid(scope)
		if err := write(); err != nil {
			t.Fatalf("%s write: %v", name, err)
		}
		if after := txid(scope); after <= before {
			t.Fatalf("%s write did not advance the cell txid (%d -> %d)", name, before, after)
		}
	}

	// control: prime the migration by reading, then write.
	controlStore := control.New(cs, nil)
	if _, err := controlStore.Apps(ctx); err != nil { // opens/migrates the cell
		t.Fatalf("control prime: %v", err)
	}
	assertBumped("control", control.Scope(), func() error {
		_, err := controlStore.CreateApp(ctx, "acme", "ops")
		return err
	})

	// timer: NewStore runs the migration through Cell.Tx, so baseline after it.
	timerScope := cell.Scope{Namespace: "acme", Class: "__timer__", ID: "t1"}
	timerCell, err := cs.Cell(ctx, timerScope)
	if err != nil {
		t.Fatalf("timer cell: %v", err)
	}
	timerStore, err := timer.NewStore(ctx, timerCell)
	if err != nil {
		t.Fatalf("timer store: %v", err)
	}
	assertBumped("timer", timerScope, func() error {
		return timerStore.Upsert(ctx, timer.New(1234, timer.KindCron, "acme/__cron__/web", "slot"))
	})

	// queue
	queueStore := queue.New(cs)
	if _, err := queueStore.Depth(ctx, "acme", "q1"); err != nil {
		t.Fatalf("queue prime: %v", err)
	}
	assertBumped("queue", queue.Scope("acme", "q1"), func() error {
		_, err := queueStore.Send(ctx, "acme", "q1", []byte("hi"), "text/plain", 0, "")
		return err
	})

	// workflow
	wfStore := workflow.New(cs)
	if _, err := wfStore.List(ctx, "acme", "wf1", 1); err != nil { // opens/migrates the cell
		t.Fatalf("workflow prime: %v", err)
	}
	assertBumped("workflow", workflow.Scope("acme", "wf1"), func() error {
		_, err := wfStore.Create(ctx, "acme", "wf1", "", []byte("{}"))
		return err
	})
}

// TestCaptureCommitEpochFence covers the commit-boundary fence: a capture commit
// carrying a stale epoch must be rejected (the write is then not acked), while
// the owner's current epoch commits normally.
func TestCaptureCommitEpochFence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srv, om, _ := newControlForwardServer(t, dir, "epoch-node")
	sc := control.Scope()
	owned, err := om.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if owned.Node != "epoch-node" {
		t.Fatalf("owner = %+v", owned)
	}
	epoch := owned.Epoch
	committer := srv.CaptureCommitter()
	seg := func(tag string, hdrEpoch uint64) []byte {
		return ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: hdrEpoch, StartTxID: 1, EndTxID: 1}, []byte(tag))
	}
	// Stale epoch: must be refused before anything is committed.
	err = committer.Commit(ctx, sc, epoch+1, seg("stale", epoch+1))
	if err == nil {
		t.Fatal("commit with a stale epoch was accepted")
	}
	if !errors.Is(err, owner.ErrEpochMismatch) && !errors.Is(err, owner.ErrNotOwner) {
		t.Fatalf("stale epoch error = %v, want an epoch/ownership error", err)
	}
	// Current epoch: accepted (a valid segment makes it through the fence).
	if err := committer.Commit(ctx, sc, epoch, seg("current", epoch)); err != nil {
		t.Fatalf("commit with the current epoch failed: %v", err)
	}
}
