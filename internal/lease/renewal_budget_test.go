package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

type renewalFault struct {
	bucket.Bucket
	stall, lostReply, failRead bool
	conflict                   bool
	calls                      int
}

func (b *renewalFault) CAS(ctx context.Context, k string, data []byte, etag string) (string, error) {
	b.calls++
	if b.conflict {
		return "", bucket.ErrPrecondition
	}
	if b.stall {
		<-ctx.Done()
		return "", ctx.Err()
	}
	token, err := b.Bucket.CAS(ctx, k, data, etag)
	if err == nil && b.lostReply {
		return "", context.DeadlineExceeded
	}
	return token, err
}
func (b *renewalFault) Get(ctx context.Context, k string) ([]byte, string, error) {
	if b.failRead {
		return nil, "", errors.New("confirmation unavailable")
	}
	return b.Bucket.Get(ctx, k)
}
func renewalManager(t *testing.T) (*Manager, *renewalFault) {
	t.Helper()
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(raw, "n", "s", "", "", time.Second)
	if err := m.Publish(t.Context(), Load{}); err != nil {
		t.Fatal(err)
	}
	b := &renewalFault{Bucket: raw}
	m.B = b
	return m, b
}
func TestRenewalStallLeavesTimeToRetry(t *testing.T) {
	m, b := renewalManager(t)
	b.stall = true
	start := time.Now()
	if err := m.Publish(t.Context(), Load{OwnedCells: 1}); err == nil {
		t.Fatal("expected timeout")
	}
	if time.Since(start) > 500*time.Millisecond || m.Fenced() {
		t.Fatal("renewal consumed lease")
	}
	b.stall = false
	if err := m.Publish(t.Context(), Load{OwnedCells: 1}); err != nil {
		t.Fatal(err)
	}
	if !m.Healthy(time.Now()) {
		t.Fatal("retry did not restore health")
	}
}
func TestRenewalConfirmsLostReply(t *testing.T) {
	m, b := renewalManager(t)
	b.lostReply = true
	if err := m.Publish(t.Context(), Load{OwnedCells: 1}); err != nil {
		t.Fatal(err)
	}
	if m.Fenced() || b.calls != 1 {
		t.Fatal("lost reply was not confirmed")
	}
}
func TestRenewalRetainsPendingUntilConfirmation(t *testing.T) {
	m, b := renewalManager(t)
	b.lostReply = true
	b.failRead = true
	if err := m.Publish(t.Context(), Load{OwnedCells: 1}); err == nil {
		t.Fatal("expected unresolved result")
	}
	b.failRead = false
	if err := m.Publish(t.Context(), Load{OwnedCells: 42}); err != nil {
		t.Fatal(err)
	}
	if b.calls != 1 {
		t.Fatal("wrote over unresolved publication")
	}
}
func TestRenewalConfirmationRejectsDifferentValueSameSession(t *testing.T) {
	m, b := renewalManager(t)
	b.lostReply = true
	b.failRead = true
	if err := m.Publish(t.Context(), Load{OwnedCells: 1}); err == nil {
		t.Fatal("expected unresolved result")
	}
	if _, err := b.Bucket.Put(t.Context(), Key("n"), []byte(`{"node":"n","session":"s","expiry":9999999999999}`)); err != nil {
		t.Fatal(err)
	}
	b.failRead = false
	if err := m.Publish(t.Context(), Load{OwnedCells: 1}); !errors.Is(err, ErrFenced) {
		t.Fatalf("err=%v", err)
	}
}
func TestRenewalPendingCannotReviveExpiredLease(t *testing.T) {
	m, b := renewalManager(t)
	b.lostReply = true
	b.failRead = true
	if err := m.Publish(t.Context(), Load{OwnedCells: 1}); err == nil {
		t.Fatal("expected unresolved result")
	}
	m.mu.Lock()
	m.publishedExpiry = time.Now().Add(-time.Second)
	m.mu.Unlock()
	b.failRead = false
	if err := m.Publish(t.Context(), Load{OwnedCells: 1}); !errors.Is(err, ErrFenced) {
		t.Fatalf("err=%v", err)
	}
}

func TestRenewalConflictFencesEvenWhenConfirmationUnavailable(t *testing.T) {
	m, b := renewalManager(t)
	b.conflict = true
	b.failRead = true
	if err := m.Publish(t.Context(), Load{}); !errors.Is(err, ErrFenced) {
		t.Fatalf("err=%v", err)
	}
	if !m.Fenced() {
		t.Fatal("CAS conflict retained authority")
	}
}
