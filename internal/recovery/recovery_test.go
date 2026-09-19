package recovery_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/ltx"
	"cellhive/internal/nodelog"
	"cellhive/internal/peer"
	"cellhive/internal/recovery"
	"cellhive/internal/replica"
	"cellhive/internal/server"
)

// TestRecoverUnuploadedAck simulates a node that acknowledged a fleet write
// (follower fsynced) but crashed before uploading it to the bucket. A new owner
// must recover the acknowledged write from the follower.
func TestRecoverUnuploadedAck(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Shared fleet bucket.
	shared, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}

	// Follower node holding the spool.
	spool, err := peer.NewSpool(filepath.Join(dir, "follower-spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	follower := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok"}, Spool: spool})
	fh := httptest.NewServer(follower.Handler())
	defer fh.Close()

	// Owner acknowledges the write to the follower but does not upload it.
	transport := peer.NewHTTPTransport("tok", nil)
	peerMgr := peer.NewManager(transport)
	nl := nodelog.New(shared, "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("acked-data"))

	if _, err := nl.Open(ctx, "s1", 1, []string{fh.URL}); err != nil {
		t.Fatalf("node-log open: %v", err)
	}
	if _, err := peerMgr.Replicate(ctx, []string{fh.URL}, sc, 1, seg); err != nil {
		t.Fatalf("replicate: %v", err)
	}

	// The bucket does not have the segment yet (crash before upload).
	rep := replica.New(shared)
	if keys, _ := rep.ListSegments(ctx, sc, 1); len(keys) != 0 {
		t.Fatalf("bucket unexpectedly has segments before recovery: %v", keys)
	}

	// New owner runs recovery from the node-log.
	rec := recovery.New(rep, transport)
	n, err := rec.Recover(ctx, nl, "node-1", "s1")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n != 1 {
		t.Fatalf("recovered %d segments, want 1", n)
	}

	// The acknowledged write is now durable in the bucket.
	keys, err := rep.ListSegments(ctx, sc, 1)
	if err != nil || len(keys) != 1 {
		t.Fatalf("bucket after recovery = %v, %v", keys, err)
	}
	got, err := rep.ReadSegment(ctx, keys[0])
	if err != nil {
		t.Fatalf("read recovered: %v", err)
	}
	if _, payload, err := ltx.Decode(got); err != nil || string(payload) != "acked-data" {
		t.Fatalf("recovered payload = %q, %v", payload, err)
	}

	// The node-log is sealed so a second recovery is a no-op.
	rec2, _, err := nl.Get(ctx, "node-1", "s1")
	if err != nil || rec2.Status != nodelog.StatusSealed {
		t.Fatalf("node-log not sealed: %+v %v", rec2, err)
	}
	if n2, err := rec.Recover(ctx, nl, "node-1", "s1"); err != nil || n2 != 0 {
		t.Fatalf("second recover = %d, %v; want 0, nil", n2, err)
	}
}

// TestRecoverNodeListsAndSkipsSealed verifies that a new owner can recover every
// unsealed session of a dead node and that sealed sessions take the fast path.
func TestRecoverNodeListsAndSkipsSealed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	shared, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	spool, err := peer.NewSpool(filepath.Join(dir, "follower-spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	follower := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok"}, Spool: spool})
	fh := httptest.NewServer(follower.Handler())
	defer fh.Close()
	transport := peer.NewHTTPTransport("tok", nil)
	peerMgr := peer.NewManager(transport)
	nl := nodelog.New(shared, "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}

	// s1 is open with an acknowledged-but-unuploaded segment on the follower.
	if _, err := nl.Open(ctx, "s1", 1, []string{fh.URL}); err != nil {
		t.Fatalf("open s1: %v", err)
	}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("s1-data"))
	if _, err := peerMgr.Replicate(ctx, []string{fh.URL}, sc, 1, seg); err != nil {
		t.Fatalf("replicate: %v", err)
	}
	// s2 is already sealed (graceful handoff) and has nothing to recover.
	if _, err := nl.Open(ctx, "s2", 1, nil); err != nil {
		t.Fatalf("open s2: %v", err)
	}
	if _, err := nl.Seal(ctx, "node-1", "s2"); err != nil {
		t.Fatalf("seal s2: %v", err)
	}

	rep := replica.New(shared)
	rec := recovery.New(rep, transport)
	segments, sessions, err := rec.RecoverNode(ctx, nl, "node-1")
	if err != nil {
		t.Fatalf("recover node: %v", err)
	}
	if segments != 1 || sessions != 1 {
		t.Fatalf("recover node = %d segments, %d sessions; want 1, 1", segments, sessions)
	}
	if keys, _ := rep.ListSegments(ctx, sc, 1); len(keys) != 1 {
		t.Fatalf("bucket after recovery = %v", keys)
	}
	if r, _, _ := nl.Get(ctx, "node-1", "s1"); r.Status != nodelog.StatusSealed {
		t.Fatalf("s1 not sealed: %+v", r)
	}
	if r, _, _ := nl.Get(ctx, "node-1", "s2"); r.Status != nodelog.StatusSealed {
		t.Fatalf("s2 changed: %+v", r)
	}
	// A second pass is a no-op (all sealed).
	if segments, sessions, err := rec.RecoverNode(ctx, nl, "node-1"); err != nil || segments != 0 || sessions != 0 {
		t.Fatalf("second recover node = %d, %d, %v", segments, sessions, err)
	}
}
