package cellcapture

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

func testCommitter(count *int) Committer {
	return committerFunc(func(context.Context, cell.Scope, uint64, []byte) error {
		*count++
		return nil
	})
}

func newCaptureManager(t *testing.T, cs *cellstore.Store, durable func(context.Context, cell.Scope, uint64) (uint64, error), commits *int) *Manager {
	t.Helper()
	return &Manager{
		Store: cs, AutoSnapshot: true,
		Owner:     func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: testCommitter(commits),
		Durable:   durable,
	}
}

func TestEnsureContinuesDurableChain(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}
	c, err := cs.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	local, err := c.TxID(ctx)
	if err != nil || local == 0 {
		t.Fatalf("local txid = %d, %v", local, err)
	}
	var commits int
	m := newCaptureManager(t, cs, func(context.Context, cell.Scope, uint64) (uint64, error) {
		return local, nil
	}, &commits)
	defer m.Stop()
	if base, err := m.Ensure(ctx, sc); err != nil || base != local {
		t.Fatalf("ensure = %d, %v; want baseline %d", base, err, local)
	}
	if commits != 0 {
		t.Fatalf("continued capture committed %d snapshot segments, want 0", commits)
	}
	// The capture is live: the next write is proven by a delta commit.
	if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	txid, _ := c.TxID(ctx)
	if err := m.Wait(ctx, sc, txid); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if commits == 0 {
		t.Fatal("delta write produced no commit")
	}
}

func TestEnsureFallsBackToSnapshotWhenBehind(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "snapshot"}
	c, err := cs.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	local, _ := c.TxID(ctx)
	var commits int
	m := newCaptureManager(t, cs, func(context.Context, cell.Scope, uint64) (uint64, error) {
		return local - 1, nil // local is ahead of the durable prefix (unacked tail)
	}, &commits)
	defer m.Stop()
	if _, err := m.Ensure(ctx, sc); err != nil {
		t.Fatal(err)
	}
	if commits == 0 {
		t.Fatal("ahead-of-chain capture must take a snapshot")
	}
}

func TestEnsureFailsClosedWhenLocalBehindDurable(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "stale"}
	c, err := cs.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	local, _ := c.TxID(ctx)
	var commits int
	m := newCaptureManager(t, cs, func(context.Context, cell.Scope, uint64) (uint64, error) {
		return local + 1, nil
	}, &commits)
	if _, err := m.Ensure(ctx, sc); !errors.Is(err, ErrLocalBehindDurable) {
		t.Fatalf("ensure err = %v, want ErrLocalBehindDurable", err)
	}
}

func TestEnsureSnapshotsNewEpochForSmallCell(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "newepoch"}
	c, err := cs.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	local, _ := c.TxID(ctx)
	var commits int
	m := newCaptureManager(t, cs, func(context.Context, cell.Scope, uint64) (uint64, error) {
		return local, nil // a predecessor chain exists in another epoch
	}, &commits)
	m.HasEpoch = func(context.Context, cell.Scope, uint64) (bool, error) { return false, nil }
	m.EpochSnapshotMaxBytes = 1 << 30
	defer m.Stop()
	if _, err := m.Ensure(ctx, sc); err != nil {
		t.Fatal(err)
	}
	if commits == 0 {
		t.Fatal("a new small epoch must open with a snapshot")
	}
}

func TestEnsureResumesLargeNewEpoch(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "largeepoch"}
	c, err := cs.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	local, _ := c.TxID(ctx)
	var commits int
	m := newCaptureManager(t, cs, func(context.Context, cell.Scope, uint64) (uint64, error) {
		return local, nil
	}, &commits)
	m.HasEpoch = func(context.Context, cell.Scope, uint64) (bool, error) { return false, nil }
	m.EpochSnapshotMaxBytes = 1 // DB is larger
	defer m.Stop()
	if _, err := m.Ensure(ctx, sc); err != nil {
		t.Fatal(err)
	}
	if commits != 0 {
		t.Fatalf("large new epoch committed %d snapshot segments, want resume", commits)
	}
}

// TestSweepEvictsCapturedCellWithoutDeadlock guards the eviction contract:
// Store.evictOne holds openMu while calling Drop, so Drop must never reopen the
// cell through Store.Cell (that deadlocked every cold open in production).
func TestSweepEvictsCapturedCellWithoutDeadlock(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	cs.MaxOpenCells = 1
	sc1 := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "evict1"}
	sc2 := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "evict2"}
	var commits int
	m := newCaptureManager(t, cs, func(context.Context, cell.Scope, uint64) (uint64, error) { return 0, nil }, &commits)
	cs.Drop = m.Drop
	defer m.Stop()
	if _, err := m.Ensure(ctx, sc1); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Cell(ctx, sc2); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { cs.Sweep(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Sweep deadlocked in Drop")
	}
}

func TestSameEpochResumeUsesEpochWatermarkOnly(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "epochonly"}
	c, err := cs.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	local, _ := c.TxID(ctx)
	var commits int
	m := newCaptureManager(t, cs, func(context.Context, cell.Scope, uint64) (uint64, error) {
		return 0, nil // the cross-epoch chain must not be consulted here
	}, &commits)
	m.HasEpoch = func(context.Context, cell.Scope, uint64) (bool, error) { return true, nil }
	m.DurableEpoch = func(context.Context, cell.Scope, uint64) (uint64, error) { return local, nil }
	defer m.Stop()
	if _, err := m.Ensure(ctx, sc); err != nil {
		t.Fatal(err)
	}
	if commits != 0 {
		t.Fatalf("same-epoch resume committed %d snapshot segments, want 0", commits)
	}
}

func TestEnsureAfterDropUsesResumeHint(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "hint"}
	c, err := cs.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	local, _ := c.TxID(ctx)
	var commits int
	var durableCalls int
	m := newCaptureManager(t, cs, func(context.Context, cell.Scope, uint64) (uint64, error) {
		durableCalls++
		return local, nil
	}, &commits)
	if _, err := m.Ensure(ctx, sc); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	txid, _ := c.TxID(ctx)
	if err := m.Wait(ctx, sc, txid); err != nil {
		t.Fatal(err)
	}
	if err := m.Drop(ctx, sc); err != nil {
		t.Fatal(err)
	}
	before := durableCalls
	// A hint hit must not consult the bucket at all.
	if _, err := m.Ensure(ctx, sc); err != nil {
		t.Fatal(err)
	}
	if durableCalls != before {
		t.Fatalf("Ensure after Drop consulted Durable %d times, want 0", durableCalls-before)
	}
	m.Stop()
}

func TestEvictionKeepsResumeHint(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	cs.MaxOpenCells = 1
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "evict-hint"}
	other := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "other"}
	c, err := cs.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	local, _ := c.TxID(ctx)
	var commits, durableCalls int
	m := newCaptureManager(t, cs, func(context.Context, cell.Scope, uint64) (uint64, error) {
		durableCalls++
		return local, nil
	}, &commits)
	cs.Drop = m.Drop
	// Eviction must NOT clear the hint; only explicit Forget/ownership loss does.
	if _, err := m.Ensure(ctx, sc); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(ctx, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}
	txid, _ := c.TxID(ctx)
	if err := m.Wait(ctx, sc, txid); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Cell(ctx, other); err != nil {
		t.Fatal(err)
	}
	cs.Sweep(ctx)
	before := durableCalls
	if _, err := m.Ensure(ctx, sc); err != nil {
		t.Fatal(err)
	}
	if durableCalls != before {
		t.Fatalf("Ensure after eviction consulted Durable %d times, want 0", durableCalls-before)
	}
	m.Stop()
}
