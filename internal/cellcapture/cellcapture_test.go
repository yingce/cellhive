package cellcapture

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

type recCommitter struct {
	mu   sync.Mutex
	segs int
}

func TestSnapshotFailureCannotStartUnprovenCapture(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	scope := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "snapshot-fail"}
	c, err := cs.Cell(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(context.Background(), "seed", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	var fail atomic.Bool
	fail.Store(true)
	m := &Manager{Store: cs, AutoSnapshot: true, Checkpoint: time.Hour,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: committerFunc(func(context.Context, cell.Scope, uint64, []byte) error {
			if fail.Load() {
				return errors.New("injected bucket failure")
			}
			return nil
		}),
	}
	if _, err := m.Ensure(context.Background(), scope); err == nil {
		t.Fatal("snapshot failure was accepted")
	}
	m.mu.Lock()
	_, started := m.caps[scope.String()]
	m.mu.Unlock()
	if started {
		t.Fatal("failed snapshot left a capture instance")
	}
	fail.Store(false)
	if _, err := m.Ensure(context.Background(), scope); err != nil {
		t.Fatalf("retry snapshot: %v", err)
	}
	txid, err := c.TxID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Wait(context.Background(), scope, txid); err != nil {
		t.Fatalf("baseline not durable: %v", err)
	}
	m.Stop()
}

func TestFailedCaptureRestartsFromDurableSnapshot(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	scope := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "restart-fail"}
	c, err := cs.Cell(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(context.Background(), "seed", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	var fail atomic.Bool
	m := &Manager{Store: cs, AutoSnapshot: true, Checkpoint: time.Hour,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: committerFunc(func(context.Context, cell.Scope, uint64, []byte) error {
			if fail.Load() {
				return errors.New("injected bucket failure")
			}
			return nil
		}),
	}
	if _, err := m.Ensure(context.Background(), scope); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if err := c.Put(context.Background(), "pending", []byte("write"), nil); err != nil {
		t.Fatal(err)
	}
	txid, err := c.TxID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Wait(ctx, scope, txid); err == nil {
		t.Fatal("failed capture acknowledged the write")
	}
	fail.Store(false)
	if _, err := m.Ensure(ctx, scope); err != nil {
		t.Fatalf("restart snapshot: %v", err)
	}
	if err := m.Wait(ctx, scope, txid); err != nil {
		t.Fatalf("restarted capture did not prove pending txid: %v", err)
	}
	m.Stop()
}

func TestFailedCaptureWithoutSnapshotStaysClosed(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	scope := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "no-snapshot"}
	m := &Manager{Store: cs, Checkpoint: time.Hour,
		Owner:     func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: committerFunc(func(context.Context, cell.Scope, uint64, []byte) error { return errors.New("injected failure") }),
	}
	ctx := context.Background()
	if _, err := m.Ensure(ctx, scope); err != nil {
		t.Fatal(err)
	}
	c, err := cs.Cell(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "pending", []byte("write"), nil); err != nil {
		t.Fatal(err)
	}
	txid, err := c.TxID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := m.Wait(wctx, scope, txid); err == nil {
		t.Fatal("failed commit acknowledged")
	}
	// Wait observes the barrier failure before Run's deferred Done close. This
	// test checks a stopped capture, so wait for that lifecycle boundary first.
	m.mu.Lock()
	done := m.caps[scope.String()].cap.Done()
	m.mu.Unlock()
	select {
	case <-done:
	case <-wctx.Done():
		t.Fatal("failed capture did not stop")
	}
	if _, err := m.Ensure(ctx, scope); err == nil {
		t.Fatal("dead capture without snapshot was restarted")
	}
	m.Stop()
}

func (r *recCommitter) Commit(context.Context, cell.Scope, uint64, []byte) error {
	r.mu.Lock()
	r.segs++
	r.mu.Unlock()
	return nil
}

func TestManagerCapturesAndWaits(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	rc := &recCommitter{}
	var owner atomic.Bool
	owner.Store(true)
	m := &Manager{
		Store: cs, Committer: rc, AutoSnapshot: true,
		Owner:      func(context.Context, cell.Scope) (bool, uint64, error) { return owner.Load(), 1, nil },
		Checkpoint: 50 * time.Millisecond,
	}
	scope := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"}
	ctx := context.Background()
	if _, err := m.Ensure(ctx, scope); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	c, err := cs.Cell(ctx, scope)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	if err := c.Put(ctx, "k", []byte("v"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	txid, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := m.Wait(wctx, scope, txid); err != nil {
		t.Fatalf("wait: %v", err)
	}
	rc.mu.Lock()
	segs := rc.segs
	rc.mu.Unlock()
	if segs == 0 {
		t.Fatal("no segments captured")
	}

	// Ownership loss stops the capture.
	owner.Store(false)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		_, ok := m.caps[scope.String()]
		m.mu.Unlock()
		if !ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	m.mu.Lock()
	_, still := m.caps[scope.String()]
	m.mu.Unlock()
	if still {
		t.Fatal("capture not stopped after ownership loss")
	}
}

func TestEnsureRefusesWhenNotOwner(t *testing.T) {
	cs, _ := cellstore.New(t.TempDir())
	defer cs.Close()
	m := &Manager{Store: cs, Committer: &recCommitter{},
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return false, 0, nil }}
	if _, err := m.Ensure(context.Background(), cell.Scope{Namespace: "a", Class: "__kv__", ID: "d"}); err != ErrNotOwner {
		t.Fatalf("ensure = %v, want ErrNotOwner", err)
	}
}

// TestWatermarkSurvivesRequestContextCancel is the regression for the committed
// watermark closure capturing the request context: once the first request ends,
// the accessor returned 0 forever and auto-checkpoint truncated the WAL ahead of
// the durability ticket.
func TestWatermarkSurvivesRequestContextCancel(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	m := &Manager{
		Store: cs, Committer: &recCommitter{}, AutoSnapshot: false,
		Owner:               func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Checkpoint:          time.Hour,
		AutoCheckpointBytes: 1 << 20,
	}
	scope := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "wm"}
	rctx, cancel := context.WithCancel(context.Background())
	if _, err := m.Ensure(rctx, scope); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	bg := context.Background()
	c, err := cs.Cell(bg, scope)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	if err := c.Put(bg, "k", []byte("v"), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	txid, err := c.TxID(bg)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	cancel()

	m.mu.Lock()
	st := m.caps[scope.String()]
	m.mu.Unlock()
	if st == nil {
		t.Fatalf("capture not registered")
	}
	if got := st.cap.CommittedTicket(); got != txid || got == 0 {
		t.Fatalf("committed ticket after request cancel = %d, want %d", got, txid)
	}
}
