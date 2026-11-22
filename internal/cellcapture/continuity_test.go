package cellcapture

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func TestInitialNativeSnapshotHasNoSidecars(t *testing.T) {
	ctx := t.Context()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "first"}
	if _, err := cs.Cell(ctx, sc); err != nil {
		t.Fatal(err)
	}
	m := &Manager{
		Store: cs, AutoSnapshot: true,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: committerFunc(func(ctx context.Context, sc cell.Scope, epoch uint64, data []byte) error {
			_, _, err := rep.Append(ctx, sc, epoch, data)
			return err
		}),
		Durable: rep.DurableWatermark,
	}
	defer m.Stop()
	if _, err := m.Ensure(ctx, sc); err != nil {
		t.Fatal(err)
	}
	segs, err := rep.RestoreChain(ctx, sc, 1)
	if err != nil || len(segs) == 0 {
		t.Fatalf("initial snapshot not recoverable: segments=%d err=%v", len(segs), err)
	}
	for _, seg := range segs {
		h, err := ltx.ValidateNative(seg.Raw)
		if err != nil || !h.IsSnapshot() || h.MaxTXID == 0 {
			t.Fatalf("invalid initial native snapshot: %+v %v", h, err)
		}
	}
	keys, err := b.List(ctx, sc.Key()+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if !strings.HasSuffix(key, ".ltx") {
			t.Fatalf("unexpected snapshot sidecar: %s", key)
		}
	}
}

func TestInitialSnapshotUploadFailureStopsCapture(t *testing.T) {
	ctx := t.Context()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "marker-failure"}
	if _, err := cs.Cell(ctx, sc); err != nil {
		t.Fatal(err)
	}
	var commits int
	m := newCaptureManager(t, cs, func(context.Context, cell.Scope, uint64) (uint64, error) { return 0, nil }, &commits)
	m.Committer = committerFunc(func(context.Context, cell.Scope, uint64, []byte) error {
		commits++
		return errors.New("upload unavailable")
	})
	defer m.Stop()
	if _, err := m.Ensure(ctx, sc); err == nil {
		t.Fatal("failed upload allowed capture to start")
	}
	if commits != 1 {
		t.Fatalf("failed snapshot attempted %d uploads, want 1", commits)
	}
}

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

func TestEnsureLookupFailureDoesNotPublishSnapshot(t *testing.T) {
	for _, stage := range []string{"epoch", "epoch-watermark", "chain-watermark"} {
		for _, stale := range []bool{false, true} {
			name := stage + "/empty"
			if stale {
				name = stage + "/stale"
			}
			t.Run(name, func(t *testing.T) {
				ctx := t.Context()
				cs, err := cellstore.New(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer cs.Close()
				sc := cell.Scope{Namespace: "acme", Class: "__d1__", ID: "lookup-failure"}
				c, err := cs.Cell(ctx, sc)
				if err != nil {
					t.Fatal(err)
				}
				if stale {
					if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
						_, err := tx.Exec("CREATE TABLE business(value TEXT)")
						return err
					}); err != nil {
						t.Fatal(err)
					}
				}
				local, err := c.TxID(ctx)
				if err != nil {
					t.Fatal(err)
				}
				lookupErr := errors.New("storage lookup unavailable")
				failing := true
				durable := local + 10
				var commits int
				m := newCaptureManager(t, cs, func(context.Context, cell.Scope, uint64) (uint64, error) {
					if failing && stage == "chain-watermark" {
						return 0, lookupErr
					}
					return durable, nil
				}, &commits)
				defer m.Stop()
				m.HasEpoch = func(context.Context, cell.Scope, uint64) (bool, error) {
					if failing && stage == "epoch" {
						return false, lookupErr
					}
					return stage == "epoch-watermark", nil
				}
				m.DurableEpoch = func(context.Context, cell.Scope, uint64) (uint64, error) {
					if failing {
						return 0, lookupErr
					}
					return durable, nil
				}
				if _, err := m.Ensure(ctx, sc); !errors.Is(err, lookupErr) {
					t.Fatalf("Ensure error = %v, want lookup failure (commits=%d)", err, commits)
				}
				if commits != 0 {
					t.Fatalf("published %d segments with unknown durable history", commits)
				}
				if err := m.Wait(ctx, sc, local); !errors.Is(err, ErrNotOwner) {
					t.Fatalf("failed capture still serves Wait: %v", err)
				}
				if txid, err := c.TxID(ctx); err != nil || txid != local {
					t.Fatalf("failed lookup changed local txid: %d, %v", txid, err)
				}
				failing = false
				if _, err := m.Ensure(ctx, sc); !errors.Is(err, ErrLocalBehindDurable) {
					t.Fatalf("retry with older local DB: %v", err)
				}
				if commits != 0 {
					t.Fatalf("published %d segments from stale local DB", commits)
				}
				// A successful lookup proving no history must still permit initialization.
				durable = 0
				if _, err := m.Ensure(ctx, sc); err != nil {
					t.Fatalf("retry after confirmed empty history: %v", err)
				}
				if commits == 0 {
					t.Fatal("confirmed empty history did not publish initial snapshot")
				}
			})
		}
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

func TestEnsureRejectsLargeNewEpochWithoutMarker(t *testing.T) {
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
	if _, err := m.Ensure(ctx, sc); err == nil {
		t.Fatal("large cross-epoch capture started without a marker writer")
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

// A continued epoch keeps the predecessor chain. It must not publish another
// full image into this epoch's L0.
func TestEpochContinueDoesNotPublishAnotherSnapshot(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "baseline"}
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
	m.HasEpoch = func(context.Context, cell.Scope, uint64) (bool, error) { return false, nil } // new epoch
	m.EpochSnapshotMaxBytes = 1 << 30
	m.WriteContinuation = func(context.Context, cell.Scope, uint64, ltx.NativeHeader, []ltx.WALPage) error {
		return nil
	}
	doneTx := make(chan uint64, 1)
	m.Committer = committerFunc(func(_ context.Context, _ cell.Scope, _ uint64, raw []byte) error {
		h, err := ltx.ValidateNative(raw)
		if err != nil {
			return err
		}
		if !h.IsSnapshot() {
			return errors.New("background baseline is not a native snapshot")
		}
		doneTx <- h.MaxTXID
		return nil
	})
	defer m.Stop()
	if _, err := m.Ensure(ctx, sc); err != nil {
		t.Fatal(err)
	}
	select {
	case txid := <-doneTx:
		t.Fatalf("epoch continue published a snapshot at txid %d", txid)
	case <-time.After(time.Second):
	}
}
