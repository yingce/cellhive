package recovery_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/coordination"
	"cellhive/internal/lease"
	"cellhive/internal/nodelog"
	"cellhive/internal/recovery"
	"cellhive/internal/replica"
)

type noNodeLogList struct {
	bucket.Bucket
	gets  int
	lists int
}

func (b *noNodeLogList) List(ctx context.Context, p string) ([]string, error) {
	if strings.HasPrefix(p, "node-logs/") {
		b.lists++
		return nil, fmt.Errorf("unexpected node-log COS List")
	}
	return b.Bucket.List(ctx, p)
}
func (b *noNodeLogList) Get(ctx context.Context, k string) ([]byte, string, error) {
	if strings.HasPrefix(k, "node-logs/") {
		b.gets++
	}
	return b.Bucket.Get(ctx, k)
}

func TestRedisDiscoveryRecoversFollowerWithoutNodeLogList(t *testing.T) {
	endpoint := os.Getenv("CELLHIVE_TEST_COORDINATION_URL")
	if endpoint == "" {
		t.Skip("requires isolated Redis")
	}
	ctx := t.Context()
	dir := t.TempDir()
	fs, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatal(err)
	}
	region := fmt.Sprintf("recovery-test-%d", time.Now().UnixNano())
	if err = coordination.Initialize(ctx, fs, endpoint, region); err != nil {
		t.Fatal(err)
	}
	cb, err := coordination.Open(ctx, fs, endpoint, region)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cb.Close() })
	guard := &noNodeLogList{Bucket: fs}
	cb.Base = guard
	url, transport := follower(t, filepath.Join(dir, "follower"))
	nl := nodelog.New(cb, "writer")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "redis-recovery"}
	if _, err = nl.Open(ctx, "s1", 1, []string{url}); err != nil {
		t.Fatal(err)
	}
	ackSegment(t, transport, []string{url}, sc, 1, "durable-from-follower")
	lm := lease.NewManager(cb, "writer", "s1", "", "", time.Minute)
	if err = lm.Publish(ctx, lease.Load{}); err != nil {
		t.Fatal(err)
	}
	rep := replica.New(cb)
	rec := recovery.New(rep, transport)
	runner := &recovery.Runner{Nodelog: nodelog.New(cb, "reader"), Lease: lease.NewManager(cb, "reader", "r1", "", "", time.Minute), Recovery: rec}
	guard.gets = 0
	if _, _, n, err := runner.Pass(ctx); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if guard.gets != 0 || guard.lists != 0 {
		t.Fatalf("live discovery touched COS: get=%d list=%d", guard.gets, guard.lists)
	}
	if err = cb.Delete(ctx, "nodes/writer.json"); err != nil {
		t.Fatal(err)
	}
	// A reconstructed observer has no cached live-lease snapshot.
	runner = &recovery.Runner{Nodelog: nodelog.New(cb, "reader"), Lease: lease.NewManager(cb, "reader", "r2", "", "", time.Minute), Recovery: rec}
	nodes, segments, sessions, err := runner.Pass(ctx)
	if err != nil || nodes != 1 || segments != 1 || sessions != 1 {
		t.Fatal(nodes, segments, sessions, err)
	}
	if guard.gets != 0 || guard.lists != 0 {
		t.Fatalf("Redis recovery must not read session metadata from COS: get=%d list=%d", guard.gets, guard.lists)
	}
	keys, _, err := cb.PendingRecoverySessions(ctx, "node-logs/")
	if err != nil || len(keys) != 0 {
		t.Fatal(keys, err)
	}
	files, err := rep.ListSegments(ctx, sc, 1)
	if err != nil || len(files) != 1 {
		t.Fatal(files, err)
	}
	// A sealed session vanished from the index but cannot be reused at startup.
	if err = rec.RecoverLocal(ctx, nl, "writer", "s1", nil); err == nil || !strings.Contains(err.Error(), "already used") {
		t.Fatal(err)
	}
	if err = rec.RecoverLocal(ctx, nl, "writer", "fresh-session", nil); err != nil {
		t.Fatal(err)
	}
	if guard.lists != 0 {
		t.Fatal("startup listed COS node logs")
	}
}
