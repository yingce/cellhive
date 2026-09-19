package cellcapture

import (
	"context"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

type committerFunc func(context.Context, cell.Scope, uint64, []byte) error

func (f committerFunc) Commit(ctx context.Context, sc cell.Scope, epoch uint64, seg []byte) error {
	return f(ctx, sc, epoch, seg)
}

// TestWaitBlocksUntilCommitted pins the RPO=0 ack contract: Wait(txid) for a
// write must not return before the committer has made that write's segment
// durable. (The invariant depends on every captured write advancing the cell
// txid; see TestCapturedStoresAdvanceCellTxID.)
func TestWaitBlocksUntilCommitted(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	gate := make(chan struct{})
	var once sync.Once
	m := &Manager{
		Store: cs,
		Committer: committerFunc(func(ctx context.Context, sc cell.Scope, epoch uint64, seg []byte) error {
			once.Do(func() { <-gate })
			return nil
		}),
		Owner:      func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Checkpoint: 50 * time.Millisecond,
	}
	ctx := context.Background()
	scope := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "ack"}
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
	waitErr := make(chan error, 1)
	go func() {
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		waitErr <- m.Wait(wctx, scope, txid)
	}()
	select {
	case err := <-waitErr:
		close(gate)
		t.Fatalf("Wait returned before the committer finished: %v", err)
	case <-time.After(400 * time.Millisecond):
	}
	close(gate)
	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Wait did not return after the committer finished")
	}
}

// TestEnsureDoesNotBlockOtherScopes covers the lock-scope fix: while one scope is
// running its (slow) baseline snapshot, Ensure for a different scope must not be
// blocked behind the manager lock.
func TestEnsureDoesNotBlockOtherScopes(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	slow := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "slow"}
	m := &Manager{
		Store: cs,
		Committer: committerFunc(func(ctx context.Context, sc cell.Scope, epoch uint64, seg []byte) error {
			if sc.String() == slow.String() {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release // hold only the slow scope's snapshot/commit
			}
			return nil
		}),
		Owner:        func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		AutoSnapshot: true,
		Checkpoint:   50 * time.Millisecond,
	}
	ctx := context.Background()
	a := slow
	b := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "fast"}
	doneA := make(chan error, 1)
	go func() {
		_, err := m.Ensure(ctx, a)
		doneA <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("scope A never reached its snapshot")
	}
	// Scope B's Ensure must complete while A's snapshot is still blocked.
	doneB := make(chan error, 1)
	go func() {
		_, err := m.Ensure(ctx, b)
		doneB <- err
	}()
	select {
	case err := <-doneB:
		if err != nil {
			t.Fatalf("ensure scope B: %v", err)
		}
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("Ensure(scope B) blocked behind scope A's snapshot (manager lock held too long)")
	}
	close(release)
	select {
	case err := <-doneA:
		if err != nil {
			t.Fatalf("ensure scope A: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("scope A did not finish")
	}
}
