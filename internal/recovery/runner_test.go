package recovery_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/lease"
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
	srv := server.New(server.Deps{Cfg: recoveryTestConfig(""), Spool: spool})
	h := httptest.NewServer(srv.Handler())
	t.Cleanup(h.Close)
	return h.URL, peer.NewHTTPTransport(recoveryPeerKey(), nil).WithSession("s1")
}

func ackSegment(t *testing.T, transport peer.Transport, followers []string, sc cell.Scope, epoch uint64, payload string) {
	t.Helper()
	seg := nativeRecoverySegment(t, 1, []byte(payload))
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
	lm2 := lease.NewManager(shared, "node-2", "s1", "", "", time.Minute)
	if err := lm2.Publish(ctx, lease.Load{}); err != nil {
		t.Fatalf("publish lease: %v", err)
	}

	// node-3: self with a live current session.
	if err := lease.NewManager(shared, "node-3", "s1", "", "", time.Minute).Publish(ctx, lease.Load{}); err != nil {
		t.Fatal(err)
	}
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
	// A known live session becoming dead recovers after the bounded observation,
	// without waiting for the historical discovery interval.
	runner.Lease.TTL = 100 * time.Millisecond
	time.Sleep(15 * time.Millisecond)
	expired, _ := json.Marshal(lease.NodeLease{Node: "node-2", Session: "s1", Expiry: time.Now().Add(-time.Second).UnixMilli(), Liveness: lease.NodeLeaseLiveness})
	if _, err := shared.Put(ctx, lease.Key("node-2"), expired); err != nil {
		t.Fatal(err)
	}
	if nodes, segments, sessions, err = runner.Pass(ctx); err != nil || nodes != 1 || segments != 1 || sessions != 1 {
		t.Fatalf("warm death recovery = %d,%d,%d,%v", nodes, segments, sessions, err)
	}
	if got, _ := rep.ListSegments(ctx, sc2, 1); len(got) != 1 {
		t.Fatalf("warm recovery lost acknowledged segment: %v", got)
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

	// Seed an expired historical record. Publish now correctly rejects leases
	// that returned already expired, so it cannot construct this fixture.
	expired, _ := json.Marshal(lease.NodeLease{Node: "node-x", Session: "sx", Expiry: time.Now().Add(-time.Minute).UnixMilli(), Liveness: lease.NodeLeaseLiveness})
	if _, err := shared.Put(ctx, lease.Key("node-x"), expired); err != nil {
		t.Fatal(err)
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

func TestRunnerRecoversOldSessionOfLiveNode(t *testing.T) {
	ctx := context.Background()
	b, _ := bucket.NewFSBucket(t.TempDir())
	f, tr := follower(t, t.TempDir())
	nl := nodelog.New(b, "restarted")
	if _, err := nl.Open(ctx, "s1", 1, []string{f}); err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "test", Class: "kv", ID: "old"}
	ackSegment(t, tr, []string{f}, sc, 1, "old acknowledged data")
	lm := lease.NewManager(b, "restarted", "new-session", "", "", time.Minute)
	if err := lm.Publish(ctx, lease.Load{}); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Nodelog: nl, Lease: lm, Recovery: recovery.New(replica.New(b), tr), SelfNode: "restarted"}
	_, _, _, err := runner.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rec, _, err := nl.Get(ctx, "restarted", "s1")
	if err != nil || rec.Status != nodelog.StatusSealed {
		t.Fatalf("old session must recover while new session lives: %+v %v", rec, err)
	}
}

func TestRunnerBacksOffFailedSession(t *testing.T) {
	ctx := context.Background()
	b, _ := bucket.NewFSBucket(t.TempDir())
	nl := nodelog.New(b, "dead")
	var calls atomic.Int64
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "unavailable", 503) }))
	defer h.Close()
	if _, err := nl.Open(ctx, "old", 1, []string{h.URL}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	runner := &recovery.Runner{Nodelog: nl, Lease: lease.NewManager(b, "w", "current", "", "", time.Minute), Recovery: recovery.New(replica.New(b), peer.NewHTTPTransport(recoveryPeerKey(), nil)), Now: func() time.Time { return now }}
	_, _, _, _ = runner.Pass(ctx)
	if calls.Load() != 1 {
		t.Fatalf("first attempt calls=%d", calls.Load())
	}
	_, _, _, _ = runner.Pass(ctx)
	if calls.Load() != 1 {
		t.Fatal("retried without backoff")
	}
	now = now.Add(2 * time.Second)
	_, _, _, _ = runner.Pass(ctx)
	if calls.Load() != 2 {
		t.Fatal("did not retry after backoff")
	}
	rec, _, _ := nl.Get(ctx, "dead", "old")
	if rec.Status != nodelog.StatusRecovering {
		t.Fatal("failed session lost")
	}
}

// Count at the bucket boundary: a logical production List also resolves every
// authority key. Avoiding the List is necessary to avoid those hidden GETs.
type discoveryMeter struct {
	bucket.Bucket
	lists, gets int
	failList    bool
}

func (b *discoveryMeter) List(ctx context.Context, prefix string) ([]string, error) {
	b.lists++
	if b.failList {
		return nil, errors.New("discovery unavailable")
	}
	return b.Bucket.List(ctx, prefix)
}
func (b *discoveryMeter) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasPrefix(key, "node-logs/") {
		b.gets++
	}
	return b.Bucket.Get(ctx, key)
}

func TestRunnerDiscoveryBoundedAndInvalidated(t *testing.T) {
	ctx := context.Background()
	fs, _ := bucket.NewFSBucket(t.TempDir())
	b := &discoveryMeter{Bucket: fs}
	nl := nodelog.New(b, "n")
	for i := 0; i < 100; i++ {
		session := fmt.Sprintf("old-%d", i)
		if _, err := nl.Open(ctx, session, 1, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := nl.Seal(ctx, "n", session); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	publish := func(session string, expiry time.Time) {
		data, _ := json.Marshal(lease.NodeLease{Node: "n", Session: session, Expiry: expiry.UnixMilli(), Liveness: lease.NodeLeaseLiveness})
		if _, err := fs.Put(ctx, lease.Key("n"), data); err != nil {
			t.Fatal(err)
		}
	}
	publish("current", now.Add(time.Hour))
	leader := true
	runner := &recovery.Runner{Nodelog: nl, Lease: lease.NewManager(b, "w", "s", "", "", 100*time.Millisecond), Recovery: recovery.New(replica.New(b), nil), Now: func() time.Time { return now }, IsLeader: func(context.Context) (bool, error) { return leader, nil }}
	pass := func() {
		t.Helper()
		time.Sleep(15 * time.Millisecond)
		if _, _, _, err := runner.Pass(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Compare the previous two-stage discovery against the same 100 records.
	priorNodes, err := nl.Nodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range priorNodes {
		if _, err := nl.ListNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	priorLists := b.lists
	b.gets, b.lists = 0, 0
	pass()
	lists, gets := b.lists, b.gets
	if priorLists != 2 || gets != 100 {
		t.Fatalf("old lists=%d fresh record reads=%d", priorLists, gets)
	}
	if lists != 1 {
		t.Fatalf("full discovery used %d lists", lists)
	}
	for i := 0; i < 11; i++ {
		now = now.Add(5 * time.Second)
		pass()
	}
	if b.lists != lists || b.gets != gets {
		t.Fatalf("historical reads on warm passes: list %d->%d get %d->%d", lists, b.lists, gets, b.gets)
	}
	// A session change is discovered before the periodic deadline.
	publish("replacement", now.Add(time.Hour))
	pass()
	if got, ok := runner.Lease.CachedNodeLease("n"); !ok || got.Session != "replacement" {
		t.Fatalf("fresh recovery lease was not shared with peer refresh: %+v %v", got, ok)
	}
	if b.lists != lists+1 {
		t.Fatalf("session change did not refresh: %d", b.lists)
	}
	// A newly published log for the same session must be found after expiry,
	// and a failed refresh must not extend the deadline.
	if _, err := nl.Open(ctx, "replacement", 1, nil); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	b.failList = true
	if _, _, _, err := runner.Pass(ctx); err == nil {
		t.Fatal("discovery failure hidden")
	}
	b.failList = false
	pass()
	rec, _, err := nl.Get(ctx, "n", "replacement")
	if err != nil || rec.Status != nodelog.StatusOpen {
		t.Fatalf("live record recovered: %+v %v", rec, err)
	}
	lists = b.lists
	publish("replacement", now.Add(-time.Second))
	pass()
	if b.lists != lists+1 {
		t.Fatal("lease expiry did not refresh records")
	}
	// Leadership loss forces a full discovery on reacquisition.
	leader = false
	pass()
	lists = b.lists
	leader = true
	pass()
	if b.lists != lists+1 {
		t.Fatalf("new leader must discover again: %d -> %d", lists, b.lists)
	}
	// If a lease changes on a global refresh, its node must be listed again
	// after that lease read; the earlier key set can predate the new session.
	now = now.Add(time.Minute)
	if _, err := nl.Open(ctx, "next", 1, nil); err != nil {
		t.Fatal(err)
	}
	publish("next", now.Add(time.Hour))
	lists = b.lists
	pass()
	if b.lists != lists+2 {
		t.Fatalf("lease transition during discovery: lists %d -> %d", lists, b.lists)
	}
}

func TestRunnerCachedLeaseCannotSealNewLiveSession(t *testing.T) {
	ctx := t.Context()
	b, _ := bucket.NewFSBucket(t.TempDir())
	nl := nodelog.New(b, "node")
	lm := lease.NewManager(b, "waker", "self", "", "", time.Minute)
	publish := func(session string) {
		raw, _ := json.Marshal(lease.NodeLease{Node: "node", Session: session, Expiry: time.Now().Add(time.Minute).UnixMilli(), Liveness: lease.NodeLeaseLiveness})
		if _, err := b.Put(ctx, lease.Key("node"), raw); err != nil {
			t.Fatal(err)
		}
	}
	publish("old")
	if _, _, err := lm.NodeLeaseFresh(ctx, "node"); err != nil {
		t.Fatal(err)
	}
	// A new live session appears after the cached observation and before discovery.
	publish("new")
	if _, err := nl.Open(ctx, "new", 1, nil); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Nodelog: nl, Lease: lm, Recovery: recovery.New(replica.New(b), nil)}
	if _, _, sessions, err := runner.Pass(ctx); err != nil || sessions != 0 {
		t.Fatal(sessions, err)
	}
	rec, _, err := nl.Get(ctx, "node", "new")
	if err != nil || rec.Status != nodelog.StatusOpen {
		t.Fatal("new live session sealed by cached predecessor", rec, err)
	}
}
