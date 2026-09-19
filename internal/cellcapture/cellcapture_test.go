package cellcapture

import (
	"context"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

type recCommitter struct {
	mu   sync.Mutex
	segs int
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
	owner := true
	m := &Manager{
		Store: cs, Committer: rc, AutoSnapshot: true,
		Owner:      func(context.Context, cell.Scope) (bool, uint64, error) { return owner, 1, nil },
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
	owner = false
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
