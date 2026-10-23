package cellcapture

import (
	"context"
	"database/sql"
	"errors"
	"testing"

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
