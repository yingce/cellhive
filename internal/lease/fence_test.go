package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

func TestExpiredLeaseCannotPublishAgain(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(b, "a", "old", "", "", time.Second)
	if err := m.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	// Advance only the local proof deadline; the bucket must remain untouched.
	m.mu.Lock()
	m.publishedExpiry = time.Now().Add(-time.Second)
	m.mu.Unlock()
	_, before, err := b.Get(t.Context(), Key("a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Publish(t.Context(), Load{}); err == nil {
		t.Fatal("expired session republished its lease")
	}
	_, after, err := b.Get(t.Context(), Key("a"))
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("expired publisher changed the lease")
	}
	if m.Healthy(time.Now()) {
		t.Fatal("expired session became healthy again")
	}
}

type delayedLeaseWrite struct {
	bucket.Bucket
	started, release chan struct{}
}

func (b *delayedLeaseWrite) delay() { close(b.started); <-b.release }
func (b *delayedLeaseWrite) Put(ctx context.Context, k string, v []byte) (string, error) {
	b.delay()
	return b.Bucket.Put(ctx, k, v)
}
func (b *delayedLeaseWrite) CAS(ctx context.Context, k string, v []byte, e string) (string, error) {
	b.delay()
	return b.Bucket.CAS(ctx, k, v, e)
}

func TestLateRenewalCannotRestoreAuthority(t *testing.T) {
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(raw, "a", "old", "", "", time.Second)
	if err := m.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	b := &delayedLeaseWrite{Bucket: raw, started: make(chan struct{}), release: make(chan struct{})}
	m.B = b
	done := make(chan error, 1)
	go func() { done <- m.Publish(t.Context(), Load{}) }()
	<-b.started
	m.mu.Lock()
	m.publishedExpiry = time.Now().Add(-time.Second)
	m.mu.Unlock()
	if !m.Fenced() {
		t.Fatal("hung publication prevented independent fencing")
	}
	close(b.release)
	if err := <-done; err == nil {
		t.Fatal("late renewal restored authority")
	}
	if m.Healthy(time.Now()) {
		t.Fatal("late renewal became healthy")
	}
}

func TestRenewalCannotOverwriteReplacementSession(t *testing.T) {
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(raw, "a", "old", "", "", time.Minute)
	if err := m.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	b := &delayedLeaseWrite{Bucket: raw, started: make(chan struct{}), release: make(chan struct{})}
	m.B = b
	done := make(chan error, 1)
	go func() { done <- m.Publish(t.Context(), Load{}) }()
	<-b.started
	data := []byte(`{"node":"a","session":"new","expiry":9999999999999}`)
	if _, err := raw.Put(t.Context(), Key("a"), data); err != nil {
		t.Fatal(err)
	}
	close(b.release)
	if err := <-done; err == nil {
		t.Fatal("old session overwrote replacement")
	}
	got, err := m.Get(t.Context(), "a")
	if err != nil || got.Session != "new" {
		t.Fatalf("replacement lease=%+v %v", got, err)
	}
	if m.Healthy(time.Now()) {
		t.Fatal("replaced session stayed healthy")
	}
}

func TestSafetyMarginCanRenewBeforeActualExpiry(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(b, "a", "old", "", "", time.Minute)
	if err := m.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.publishedExpiry = time.Now().Add(5 * time.Second)
	m.mu.Unlock()
	if m.Healthy(time.Now()) {
		t.Fatal("must pause inside the safety margin")
	}
	if m.Fenced() {
		t.Fatal("margin alone must not permanently fence")
	}
	if err := m.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	if !m.Healthy(time.Now()) {
		t.Fatal("continuous renewal did not restore health")
	}
}

func TestFenceRemainsLatched(t *testing.T) {
	m := &Manager{TTL: time.Second, publishedExpiry: time.Now().Add(-time.Second)}
	if !m.Fenced() {
		t.Fatal("missed expiry")
	}
	// Even a future deadline cannot un-fence this process session.
	m.mu.Lock()
	m.publishedExpiry = time.Now().Add(time.Hour)
	m.mu.Unlock()
	if m.Healthy(time.Now()) || !m.Fenced() {
		t.Fatal("fence was reversible")
	}
}

type failedRenewal struct{ bucket.Bucket }

func (b failedRenewal) CAS(context.Context, string, []byte, string) (string, error) {
	return "", errors.New("temporary write failure")
}

func TestFailedRenewalWithinLeaseCanRetry(t *testing.T) {
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(raw, "a", "s", "", "", time.Minute)
	if err := m.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	m.B = failedRenewal{raw}
	if err := m.Publish(t.Context(), Load{}); err == nil {
		t.Fatal("expected transient failure")
	}
	if !m.Healthy(time.Now()) || m.Fenced() {
		t.Fatal("transient failure prematurely fenced a valid lease")
	}
	m.B = raw
	if err := m.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
}
