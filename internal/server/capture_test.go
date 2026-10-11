package server

import (
	"bytes"
	"cellhive/internal/cellstore"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/control"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
	"cellhive/internal/scopedtoken"
)

func bytesReader(s string) *bytes.Reader { return bytes.NewReader([]byte(s)) }

type replicaCommitter struct{ rep *replica.Manager }

func (r replicaCommitter) Commit(ctx context.Context, sc cell.Scope, epoch uint64, seg []byte) error {
	_, _, err := r.rep.Append(ctx, sc, epoch, seg)
	return err
}

type testCommitterFunc func(context.Context, cell.Scope, uint64, []byte) error

func (f testCommitterFunc) Commit(ctx context.Context, sc cell.Scope, epoch uint64, seg []byte) error {
	return f(ctx, sc, epoch, seg)
}

// TestKVCaptureReplicatesAndRestores covers ADR-092: with capture wired, a KV
// write is captured as LTX, committed durably, and can be restored from the
// bucket on another node.
func TestKVCaptureReplicatesAndRestores(t *testing.T) {
	srv, _ := newTestServer(t)
	if err := registerTestBinding(srv, "acme", "kv", "KV", "default"); err != nil {
		t.Fatalf("binding: %v", err)
	}
	rep := replica.New(srv.Bucket)
	srv.Capture = &cellcapture.Manager{
		Store: srv.Store, Committer: replicaCommitter{rep: rep}, AutoSnapshot: true,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
	}
	h := srv.Handler()
	tok, err := scopedtoken.Mint([]byte(srv.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "kv", Name: "KV"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/kv/put?ns=acme&key=k", bytesReader("hello"))
	req.Header.Set("x-cellhive-scope-token", tok)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("kv put = %d %s", rr.Code, rr.Body.String())
	}

	// Restore the scope from the bucket and read the value back.
	scope := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "default"}
	segs, err := rep.Restore(context.Background(), scope, 1)
	if err != nil || len(segs) == 0 {
		t.Fatalf("restore segments = %d, %v", len(segs), err)
	}
	raw := make([][]byte, 0, len(segs))
	for _, s := range segs {
		raw = append(raw, s.Raw)
	}
	dst := filepath.Join(t.TempDir(), "restored.db")
	if _, err := restore.ApplyFile(dst, raw); err != nil {
		t.Fatalf("apply: %v", err)
	}
	db, err := cellstore.Open(dst)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer db.Close()
	var val string
	if err := db.QueryRow("SELECT value FROM kv WHERE key='k'").Scan(&val); err != nil {
		t.Fatalf("query restored kv: %v", err)
	}
	if val != "hello" {
		t.Fatalf("restored value = %q, want hello", val)
	}
}

// TestCaptureWaitTxIDDoesNotWaitForLaterWrite pins the output-gate boundary:
// a request waits for its own committed transaction, not a later concurrent
// transaction in the same cell. Waiting for the cell's current watermark can
// couple unrelated requests and inflate tail latency.
func TestCaptureWaitTxIDDoesNotWaitForLaterWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondEntered := make(chan struct{})
	releaseSecond := make(chan struct{})
	var commits int
	var mu sync.Mutex
	m := &cellcapture.Manager{
		Store: cs,
		Committer: testCommitterFunc(func(context.Context, cell.Scope, uint64, []byte) error {
			mu.Lock()
			commits++
			n := commits
			mu.Unlock()
			switch n {
			case 1:
				close(firstEntered)
				<-releaseFirst
			case 2:
				close(secondEntered)
				<-releaseSecond
			}
			return nil
		}),
		Owner:           func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		GroupCommitWait: time.Millisecond,
	}
	scope := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "exact-ticket"}
	if _, err := m.Ensure(ctx, scope); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	c, err := cs.Cell(ctx, scope)
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	first, err := c.PutTx(ctx, "first", []byte("1"), nil)
	if err != nil {
		t.Fatalf("first put: %v", err)
	}
	trigger := make(chan error, 1)
	go func() { trigger <- m.Wait(ctx, scope, first) }()
	select {
	case <-firstEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("first durability commit did not start")
	}
	second, err := c.PutTx(ctx, "second", []byte("2"), nil)
	if err != nil {
		t.Fatalf("second put: %v", err)
	}
	if second != first+1 {
		t.Fatalf("tickets = %d, %d; want consecutive", first, second)
	}
	srv := &Server{Store: cs, Capture: m}
	waited := make(chan error, 1)
	go func() { waited <- srv.captureWaitTxID(ctx, scope, first) }()
	close(releaseFirst)
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("wait first: %v", err)
		}
	case <-secondEntered:
		select {
		case err := <-waited:
			if err != nil {
				t.Fatalf("wait first: %v", err)
			}
		case <-time.After(time.Second):
			close(releaseSecond)
			t.Fatal("first request waited for the later transaction")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not pass its durability gate")
	}
	select {
	case <-secondEntered:
	default:
		select {
		case <-secondEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("second durability commit did not start")
		}
	}
	close(releaseSecond)
	if err := <-trigger; err != nil {
		t.Fatalf("trigger wait: %v", err)
	}
}

// registerTestBinding wires a control plane on the test server and registers an
// app resource so binding-scoped endpoints resolve a binding (and its cell id).
func registerTestBinding(srv *Server, ns, kind, name, scope string) error {
	env, err := control.NewEnvelope(bytes.Repeat([]byte{0x3}, 32))
	if err != nil {
		return err
	}
	srv.Control = control.New(srv.Store, env)
	ctx := context.Background()
	if _, err := srv.Control.CreateApp(ctx, ns, "test"); err != nil {
		return err
	}
	_, err = srv.Control.CreateResource(ctx, ns, kind, name, scope, "test")
	return err
}
