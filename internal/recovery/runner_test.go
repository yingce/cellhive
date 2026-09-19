package recovery_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/lease"
	"cellhive/internal/ltx"
	"cellhive/internal/nodelog"
	"cellhive/internal/peer"
	"cellhive/internal/recovery"
	"cellhive/internal/replica"
	"cellhive/internal/server"
)

// follower starts a follower node with its own spool and returns its URL plus a
// transport that can reach it.
func follower(t *testing.T, dir string) (string, peer.Transport) {
	t.Helper()
	spool, err := peer.NewSpool(filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	srv := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok"}, Spool: spool})
	h := httptest.NewServer(srv.Handler())
	t.Cleanup(h.Close)
	return h.URL, peer.NewHTTPTransport("tok", nil)
}

func ackSegment(t *testing.T, transport peer.Transport, followers []string, sc cell.Scope, epoch uint64, payload string) {
	t.Helper()
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: epoch, StartTxID: 1, EndTxID: 1}, []byte(payload))
	if _, err := peer.NewManager(transport).Replicate(context.Background(), followers, sc, epoch, seg); err != nil {
		t.Fatalf("replicate: %v", err)
	}
}

// TestRunnerRecoversDeadNodeSkipsLiveAndSelf verifies the orchestration pass:
// it recovers a dead node, never touches a live node, and never touches itself.
func TestRunnerRecoversDeadNodeSkipsLiveAndSelf(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	shared, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}

	// node-1: dead (no lease), has an acked-but-unuploaded segment.
	f1, t1 := follower(t, filepath.Join(dir, "f1"))
	nl1 := nodelog.New(shared, "node-1")
	sc1 := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "n1"}
	if _, err := nl1.Open(ctx, "s1", 1, []string{f1}); err != nil {
		t.Fatalf("open node-1: %v", err)
	}
	ackSegment(t, t1, []string{f1}, sc1, 1, "node1-data")

	// node-2: live (fresh lease), must not be recovered.
	f2, t2 := follower(t, filepath.Join(dir, "f2"))
	nl2 := nodelog.New(shared, "node-2")
	sc2 := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "n2"}
	if _, err := nl2.Open(ctx, "s1", 1, []string{f2}); err != nil {
		t.Fatalf("open node-2: %v", err)
	}
	ackSegment(t, t2, []string{f2}, sc2, 1, "node2-data")
	lm2 := lease.NewManager(shared, "node-2", "sess2", "", "", time.Minute)
	if err := lm2.Publish(ctx, lease.Load{}); err != nil {
		t.Fatalf("publish lease: %v", err)
	}

	// node-3: self.
	f3, t3 := follower(t, filepath.Join(dir, "f3"))
	nl3 := nodelog.New(shared, "node-3")
	sc3 := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "n3"}
	if _, err := nl3.Open(ctx, "s1", 1, []string{f3}); err != nil {
		t.Fatalf("open node-3: %v", err)
	}
	ackSegment(t, t3, []string{f3}, sc3, 1, "node3-data")

	rep := replica.New(shared)
	runner := &recovery.Runner{
		Nodelog:  nodelog.New(shared, "waker"),
		Lease:    lease.NewManager(shared, "waker", "ws", "", "", time.Minute),
		Recovery: recovery.New(rep, t1),
		SelfNode: "node-3",
	}

	nodes, segments, sessions, err := runner.Pass(ctx)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if nodes != 1 || segments != 1 || sessions != 1 {
		t.Fatalf("pass = nodes %d segments %d sessions %d; want 1,1,1", nodes, segments, sessions)
	}

	// node-1 recovered+sealed; node-2 and node-3 untouched.
	if r, _, _ := nl1.Get(ctx, "node-1", "s1"); r.Status != nodelog.StatusSealed {
		t.Fatalf("node-1 s1 not sealed: %+v", r)
	}
	if r, _, _ := nl2.Get(ctx, "node-2", "s1"); r.Status != nodelog.StatusOpen {
		t.Fatalf("live node-2 s1 changed: %+v", r)
	}
	if r, _, _ := nl3.Get(ctx, "node-3", "s1"); r.Status != nodelog.StatusOpen {
		t.Fatalf("self node-3 s1 changed: %+v", r)
	}
	if got, _ := rep.ListSegments(ctx, sc1, 1); len(got) != 1 {
		t.Fatalf("node-1 bucket segments = %v", got)
	}
	if got, _ := rep.ListSegments(ctx, sc2, 1); len(got) != 0 {
		t.Fatalf("live node-2 should not be in bucket: %v", got)
	}
	if got, _ := rep.ListSegments(ctx, sc3, 1); len(got) != 0 {
		t.Fatalf("self node-3 should not be in bucket: %v", got)
	}

	// Idempotent: a second pass does nothing.
	if nodes, segments, sessions, err = runner.Pass(ctx); err != nil || nodes != 0 || segments != 0 || sessions != 0 {
		t.Fatalf("second pass = %d,%d,%d,%v; want 0,0,0,nil", nodes, segments, sessions, err)
	}
}

// TestRunnerExpiredLeaseIsDead verifies an expired lease counts as dead.
func TestRunnerExpiredLeaseIsDead(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	shared, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	f, tr := follower(t, filepath.Join(dir, "f"))
	nl := nodelog.New(shared, "node-x")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "nx"}
	if _, err := nl.Open(ctx, "s1", 1, []string{f}); err != nil {
		t.Fatalf("open: %v", err)
	}
	ackSegment(t, tr, []string{f}, sc, 1, "x-data")

	// Publish a lease that is already expired.
	lmx := lease.NewManager(shared, "node-x", "sx", "", "", -time.Minute)
	if err := lmx.Publish(ctx, lease.Load{}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	rep := replica.New(shared)
	runner := &recovery.Runner{
		Nodelog:  nodelog.New(shared, "waker"),
		Lease:    lease.NewManager(shared, "waker", "ws", "", "", time.Minute),
		Recovery: recovery.New(rep, tr),
		SelfNode: "waker",
	}
	nodes, _, sessions, err := runner.Pass(ctx)
	if err != nil || nodes != 1 || sessions != 1 {
		t.Fatalf("expired-lease pass = %d,%d,%v; want 1,1,nil", nodes, sessions, err)
	}
}

// failingBucket makes lease reads for one key fail with a non-notfound error, to
// prove the runner fails safe (skips) rather than recovering on uncertainty.
type failingBucket struct {
	*bucket.FSBucket
	failKey string
}

func (b failingBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if key == b.failKey {
		return nil, "", errors.New("boom")
	}
	return b.FSBucket.Get(ctx, key)
}

func TestRunnerSkipsOnUncertainLeaseRead(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fsb, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	f, tr := follower(t, filepath.Join(dir, "f"))
	nl := nodelog.New(fsb, "node-y")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "ny"}
	if _, err := nl.Open(ctx, "s1", 1, []string{f}); err != nil {
		t.Fatalf("open: %v", err)
	}
	ackSegment(t, tr, []string{f}, sc, 1, "y-data")

	rep := replica.New(fsb)
	runner := &recovery.Runner{
		Nodelog:  nodelog.New(fsb, "waker"),
		Lease:    lease.NewManager(failingBucket{fsb, lease.Key("node-y")}, "waker", "ws", "", "", time.Minute),
		Recovery: recovery.New(rep, tr),
		SelfNode: "waker",
	}
	nodes, _, sessions, err := runner.Pass(ctx)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if nodes != 0 || sessions != 0 {
		t.Fatalf("uncertain lease read should skip, got nodes=%d sessions=%d", nodes, sessions)
	}
	if r, _, _ := nl.Get(ctx, "node-y", "s1"); r.Status != nodelog.StatusOpen {
		t.Fatalf("node-y should be untouched: %+v", r)
	}
}
