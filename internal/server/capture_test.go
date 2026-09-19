package server

import (
	"bytes"
	"cellhive/internal/cellstore"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

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
