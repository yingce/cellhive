package drain

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

func newMgr(t *testing.T, dir, node, session string) *Manager {
	t.Helper()
	b, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	return New(b, node, session, 30*time.Second, time.Millisecond)
}

func TestDrainTokenExclusiveAcquireAndRelease(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	a := newMgr(t, dir, "node-a", "s1")
	b := newMgr(t, dir, "node-b", "s2")

	ok, err := a.Acquire(ctx)
	if err != nil || !ok {
		t.Fatalf("a acquire = %v, %v", ok, err)
	}
	if ok, _ := b.Acquire(ctx); ok {
		t.Fatalf("b must not acquire while a holds a live token")
	}
	// b releasing is a no-op and must not steal the token.
	if err := b.Release(ctx); err != nil {
		t.Fatalf("b release: %v", err)
	}
	if tok, ok, _ := a.Holder(ctx); !ok || tok.Node != "node-a" {
		t.Fatalf("token changed after a non-holder release: %+v ok=%v", tok, ok)
	}
	// a renews.
	if ok, err := a.Acquire(ctx); err != nil || !ok {
		t.Fatalf("a renew = %v, %v", ok, err)
	}
	// a releases, then b can take it.
	if err := a.Release(ctx); err != nil {
		t.Fatalf("a release: %v", err)
	}
	if _, ok, _ := a.Holder(ctx); ok {
		t.Fatalf("token still present after release")
	}
	if ok, err := b.Acquire(ctx); err != nil || !ok {
		t.Fatalf("b acquire after release = %v, %v", ok, err)
	}
}

func TestDrainTokenExpiryAllowsTakeover(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	a := newMgr(t, dir, "node-a", "s1")
	b := newMgr(t, dir, "node-b", "s2")
	base := time.Unix(1_000_000, 0)
	a.Now = func() time.Time { return base }
	if ok, err := a.Acquire(ctx); err != nil || !ok {
		t.Fatalf("a acquire = %v, %v", ok, err)
	}
	// Still inside a's TTL: b cannot take it.
	b.Now = func() time.Time { return base.Add(10 * time.Second) }
	if ok, _ := b.Acquire(ctx); ok {
		t.Fatalf("b must not acquire before expiry")
	}
	// Past the TTL: b steals it.
	b.Now = func() time.Time { return base.Add(31 * time.Second) }
	if ok, err := b.Acquire(ctx); err != nil || !ok {
		t.Fatalf("b steal after expiry = %v, %v", ok, err)
	}
	if tok, _, _ := b.Holder(ctx); tok.Node != "node-b" {
		t.Fatalf("token holder = %q, want node-b", tok.Node)
	}
}

func TestDrainTokenWaitRetriesUntilFree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	dir := t.TempDir()
	a := newMgr(t, dir, "node-a", "s1")
	b := newMgr(t, dir, "node-b", "s2")
	b.RetryDelay = 10 * time.Millisecond
	if ok, _ := a.Acquire(ctx); !ok {
		t.Fatalf("a acquire failed")
	}
	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = a.Release(context.Background())
	}()
	if err := b.Wait(ctx); err != nil {
		t.Fatalf("b wait: %v", err)
	}
	if tok, _, _ := b.Holder(ctx); tok.Node != "node-b" {
		t.Fatalf("holder = %q, want node-b", tok.Node)
	}
}

// takeoverBucket injects a peer takeover on the second Get, simulating our token
// expiring and another node stealing it between Release's check and its delete.
type takeoverBucket struct {
	bucket.Bucket
	gets int
	peer []byte
}

func (b *takeoverBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.gets++
	if b.gets == 2 {
		if _, err := b.Bucket.Put(ctx, key, b.peer); err != nil {
			return nil, "", err
		}
	}
	return b.Bucket.Get(ctx, key)
}

// TestReleaseDoesNotDeletePeerToken is the regression for the TOCTOU: Release
// read the token twice but discarded the second read, so a token taken over in
// between was deleted with the peer's etag.
func TestReleaseDoesNotDeletePeerToken(t *testing.T) {
	ctx := context.Background()
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	m := &Manager{B: base, NodeID: "n1", Session: "s1"}
	if ok, err := m.Acquire(ctx); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	peer, _ := json.Marshal(Token{Node: "n2", Session: "s2", ExpiresMs: 1 << 62})
	m.B = &takeoverBucket{Bucket: base, peer: peer}

	if err := m.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	cur, ok, err := m.Holder(ctx)
	if err != nil || !ok {
		t.Fatalf("peer token gone: ok=%v err=%v", ok, err)
	}
	if cur.Node != "n2" || cur.Session != "s2" {
		t.Fatalf("holder = %+v, want peer n2/s2", cur)
	}
}
