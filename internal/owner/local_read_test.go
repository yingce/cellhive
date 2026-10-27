package owner

import (
	"context"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/lease"
)

type releaseGateBucket struct {
	bucket.Bucket
	started, proceed chan struct{}
}

func (b *releaseGateBucket) ConditionalDelete(ctx context.Context, key, etag string) error {
	close(b.started)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.proceed:
	}
	return bucket.ErrPrecondition
}

func TestLocalReadGuardReleaseAndCASFailure(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	lm := lease.NewManager(raw, m.NodeID, m.Session, "", "", time.Second)
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	m.LeaseView = lm
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "guard"}
	o, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	check, ok := m.LocalReadGuard(sc)
	if !ok || !check() {
		t.Fatal("missing healthy proof")
	}
	b := &releaseGateBucket{Bucket: raw, started: make(chan struct{}), proceed: make(chan struct{})}
	m.B = b
	done := make(chan error, 1)
	go func() { done <- m.Release(t.Context(), sc, o.Epoch) }()
	<-b.started
	if check() {
		t.Error("in-flight read survived release start")
	}
	// A lookup may see the old record while delete is pending, but cannot authorize a read.
	if _, _, err = m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	if _, ok = m.LocalReadGuard(sc); ok {
		t.Error("accepted route during release")
	}
	if _, ok = m.ReadFence(sc); ok {
		t.Error("accepted fresh read during release")
	}
	close(b.proceed)
	if err := <-done; err != ErrEpochMismatch {
		t.Fatalf("CAS error: %v", err)
	}
	if _, ok = m.LocalReadGuard(sc); ok {
		t.Error("retained proof after failed release")
	}
}

func TestLocalReadGuardLeaseAndSession(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	lm := lease.NewManager(raw, m.NodeID, m.Session, "", "", 100*time.Millisecond)
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	m.LeaseView = lm
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "guard"}
	if _, err := m.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	check, ok := m.LocalReadGuard(sc)
	if !ok {
		t.Fatal("missing proof")
	}
	original := m.Session
	m.Session = "different"
	if check() {
		t.Fatal("accepted other process session")
	}
	m.Session = original
	time.Sleep(110 * time.Millisecond)
	if check() {
		t.Fatal("accepted result after lease expiry")
	}
	if _, ok = m.LocalReadGuard(sc); ok {
		t.Fatal("accepted expired lease")
	}
}

func TestOldSessionCannotRenewOrRelease(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "session"}
	o, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	stale := &Manager{B: raw, NodeID: m.NodeID, Session: "old-session", OwnerTTL: time.Second}
	if _, err := stale.Renew(t.Context(), sc, o.Epoch, time.Now()); err != ErrEpochMismatch {
		t.Fatalf("renew: %v", err)
	}
	if err := stale.Release(t.Context(), sc, o.Epoch); err != ErrEpochMismatch {
		t.Fatalf("release: %v", err)
	}
	got, _, err := m.Resolve(t.Context(), sc)
	if err != nil || got.Session != m.Session {
		t.Fatalf("current owner modified: %+v %v", got, err)
	}
}
