package lease

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// Reproduce a remote server committing after the client's timeout and its
// confirming read have both returned. Context cancellation does not undo CAS.
type delayedLeaseCommit struct {
	bucket.Bucket
	key      string
	data     []byte
	expected string
	delayed  bool
}

func (b *delayedLeaseCommit) CAS(ctx context.Context, key string, data []byte, expected string) (string, error) {
	if !b.delayed {
		b.delayed = true
		b.key, b.data, b.expected = key, append([]byte(nil), data...), expected
		return "", context.DeadlineExceeded
	}
	return b.Bucket.CAS(ctx, key, data, expected)
}
func TestRenewalLateSuccessfulCommitMustNotFenceItself(t *testing.T) {
	m, original := renewalManager(t)
	b := &delayedLeaseCommit{Bucket: original.Bucket}
	m.B = b
	if err := m.Publish(t.Context(), Load{OwnedCells: 1}); err == nil {
		t.Fatal("expected first write timeout")
	}
	t.Logf("after old-value confirmation: pending retained=%v fenced=%v", m.pending != nil, m.Fenced())
	// The timed-out server operation commits after the confirmation saw old data.
	if _, err := b.Bucket.CAS(t.Context(), b.key, b.data, b.expected); err != nil {
		t.Fatal(err)
	}
	err := m.Publish(t.Context(), Load{OwnedCells: 2})
	t.Logf("retry error=%v fenced=%v remaining=%s", err, m.Fenced(), time.Until(m.publishedExpiry))
	if err != nil || m.Fenced() {
		t.Fatalf("own delayed successful renewal incorrectly revoked authority: %v", err)
	}
}

// Models cancellation independently from server completion, including a late
// original operation winning the CAS race against its identical retry.
type ambiguousLeaseWrite struct {
	bucket.Bucket
	attempts      int
	key, expected string
	data          []byte
	commitOnRetry bool
	changed       bool
}

func (b *ambiguousLeaseWrite) write(ctx context.Context, key string, data []byte, expected string) (string, error) {
	b.attempts++
	if b.attempts == 1 {
		b.key, b.expected, b.data = key, expected, append([]byte(nil), data...)
	} else if key != b.key || expected != b.expected || !bytes.Equal(data, b.data) {
		b.changed = true
	}
	if b.attempts <= 2 {
		return "", context.DeadlineExceeded
	}
	if b.commitOnRetry {
		if _, err := b.commit(ctx); err != nil {
			return "", err
		}
	}
	if expected == "" {
		return b.Bucket.ConditionalCreate(ctx, key, data)
	}
	return b.Bucket.CAS(ctx, key, data, expected)
}
func (b *ambiguousLeaseWrite) commit(ctx context.Context) (string, error) {
	if b.expected == "" {
		return b.Bucket.ConditionalCreate(ctx, b.key, b.data)
	}
	return b.Bucket.CAS(ctx, b.key, b.data, b.expected)
}
func (b *ambiguousLeaseWrite) CAS(ctx context.Context, key string, data []byte, expected string) (string, error) {
	return b.write(ctx, key, data, expected)
}
func (b *ambiguousLeaseWrite) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	return b.write(ctx, key, data, "")
}

func TestPublicationAmbiguityRetainsExactConditionalOperation(t *testing.T) {
	for _, initial := range []bool{false, true} {
		for _, completion := range []string{"before-confirm", "during-retry", "retry-only"} {
			t.Run(fmt.Sprintf("initial=%v/%s", initial, completion), func(t *testing.T) {
				raw, err := bucket.NewFSBucket(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				m := NewManager(raw, "n", "s", "", "", time.Second)
				if !initial {
					if err := m.Publish(t.Context(), Load{}); err != nil {
						t.Fatal(err)
					}
				}
				b := &ambiguousLeaseWrite{Bucket: raw, commitOnRetry: completion == "during-retry"}
				m.B = b
				for i := 1; i <= 2; i++ {
					if err := m.Publish(t.Context(), Load{OwnedCells: i}); err == nil {
						t.Fatal("expected timeout")
					}
					if m.pending == nil || m.Fenced() {
						t.Fatal("lost unresolved publication")
					}
				}
				attemptedExpiry := m.pending.expiry
				if completion == "before-confirm" {
					if _, err := b.commit(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				if err := m.Publish(t.Context(), Load{OwnedCells: 99}); err != nil {
					t.Fatal(err)
				}
				if b.changed || m.pending != nil || m.Fenced() {
					t.Fatal("retry changed operation or revoked authority")
				}
				if !m.publishedExpiry.Equal(attemptedExpiry) {
					t.Fatal("extended expiry without proof")
				}
				got, _, err := raw.Get(t.Context(), Key("n"))
				if err != nil || !bytes.Equal(got, b.data) {
					t.Fatal("confirmed different data", err)
				}
				// A subsequent, new publication may now carry new load/expiry.
				m.B = raw
				if err := m.Publish(t.Context(), Load{OwnedCells: 100}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
