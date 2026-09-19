package drain

import (
	"context"
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
