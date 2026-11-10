package dosupervisor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/owner"
)

func TestOwnerLeasesRenewIndependentlyAndStop(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &owner.Manager{B: b, NodeID: "agent", Session: "test"}
	scope := owner.DOScope("app", "worker", "Class", 0)
	ttl := 300 * time.Millisecond
	o, err := m.ClaimAs(t.Context(), scope, "runtime", "http://runtime", cell.RoleCellAgent, ttl, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var renews atomic.Int64
	leases := NewOwnerLeases(t.Context(), func(ctx context.Context, c RecoveryClaim, ttl time.Duration) (LeaseState, error) {
		v, e := m.RenewAs(ctx, c.Scope, c.Node, c.Epoch, ttl, time.Now())
		if e == nil {
			renews.Add(1)
		}
		return LeaseState{Node: v.Node, Epoch: v.Epoch, Expiry: v.Expiry}, e
	})
	defer leases.Close()
	c := RecoveryClaim{Scope: scope, Node: o.Node, Epoch: o.Epoch}
	if _, err = leases.Register(t.Context(), c, ttl); err != nil {
		t.Fatal(err)
	}
	// The workerd event loop is not called at all while more than one TTL passes.
	time.Sleep(2 * ttl)
	state, err := leases.Status(c)
	if err != nil || state.Expiry <= time.Now().UnixMilli() || renews.Load() < 3 {
		t.Fatalf("renewals=%d state=%+v err=%v", renews.Load(), state, err)
	}
	if _, err = m.ClaimAs(t.Context(), scope, "other", "", cell.RoleCellAgent, ttl, time.Now()); !errors.Is(err, owner.ErrOwnerLive) {
		t.Fatalf("live owner lost: %v", err)
	}
	leases.Close()
	if _, err = leases.Status(c); err == nil {
		t.Fatal("closed manager served valid state")
	}
	time.Sleep(2 * ttl)
	if _, err = m.ClaimAs(t.Context(), scope, "other", "", cell.RoleCellAgent, ttl, time.Now()); err != nil {
		t.Fatalf("dead runtime prevented takeover: %v", err)
	}
}

func TestOwnerLeasesDoNotResurrectReleasedIdentity(t *testing.T) {
	b, _ := bucket.NewFSBucket(t.TempDir())
	m := &owner.Manager{B: b, NodeID: "agent", Session: "test"}
	scope := owner.DOScope("app", "worker", "Class", 0)
	o, err := m.ClaimAs(t.Context(), scope, "runtime", "", cell.RoleCellAgent, time.Second, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	failed := make(chan struct{}, 1)
	leases := NewOwnerLeases(t.Context(), func(ctx context.Context, c RecoveryClaim, ttl time.Duration) (LeaseState, error) {
		v, e := m.RenewAs(ctx, c.Scope, c.Node, c.Epoch, ttl, time.Now())
		if e != nil {
			select {
			case failed <- struct{}{}:
			default:
			}
		}
		return LeaseState{Node: v.Node, Epoch: v.Epoch, Expiry: v.Expiry}, e
	})
	defer leases.Close()
	c := RecoveryClaim{Scope: scope, Node: o.Node, Epoch: o.Epoch}
	if _, err = leases.Register(t.Context(), c, 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err = m.ReleaseAs(t.Context(), scope, o.Node, o.Epoch); err != nil {
		t.Fatal(err)
	}
	select {
	case <-failed:
	case <-time.After(2 * time.Second):
		t.Fatal("renew did not observe release")
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, err = leases.Status(c); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failed identity still served")
		}
		time.Sleep(time.Millisecond)
	}
	if _, _, err = m.Resolve(t.Context(), scope); !errors.Is(err, owner.ErrUnowned) {
		t.Fatalf("released owner resurrected: %v", err)
	}
	next, err := m.ClaimAs(t.Context(), scope, "next", "", cell.RoleCellAgent, time.Second, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	newer := RecoveryClaim{Scope: scope, Node: next.Node, Epoch: next.Epoch}
	if _, err = leases.Register(t.Context(), newer, time.Second); err != nil {
		t.Fatal(err)
	}
	leases.Forget(c)
	if _, err = leases.Status(newer); err != nil {
		t.Fatal("old forget removed new identity", err)
	}
	if _, err = leases.Status(c); err == nil {
		t.Fatal("old identity accepted")
	}
}

func TestOwnerLeasesLateFailureCannotRemoveNewEpoch(t *testing.T) {
	started, finish, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var oldCalls atomic.Int64
	leases := NewOwnerLeases(t.Context(), func(ctx context.Context, c RecoveryClaim, ttl time.Duration) (LeaseState, error) {
		if c.Epoch == 1 && oldCalls.Add(1) > 1 {
			close(started)
			<-finish // Simulate a reply already in flight when cancellation arrived.
			defer close(returned)
			return LeaseState{}, errors.New("late old failure")
		}
		return LeaseState{Node: c.Node, Epoch: c.Epoch, Expiry: time.Now().Add(time.Minute).UnixMilli()}, nil
	})
	defer leases.Close()
	c := RecoveryClaim{Scope: owner.DOScope("app", "worker", "Class", 0), Node: "a", Epoch: 1}
	if _, err := leases.Register(t.Context(), c, 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("old renewal not started")
	}
	next := c
	next.Epoch = 2
	next.Node = "b"
	if _, err := leases.Register(t.Context(), next, time.Minute); err != nil {
		t.Fatal(err)
	}
	close(finish)
	<-returned
	// A late failure must not change the new registration after it is observed.
	until := time.NewTimer(200 * time.Millisecond)
	defer until.Stop()
	for {
		if _, err := leases.Status(next); err != nil {
			t.Fatal("late old renewal removed new epoch", err)
		}
		select {
		case <-until.C:
			return
		case <-time.After(time.Millisecond):
		}
	}
}

// A transient transport failure must not discard a still-valid lease or allow
// it to be extended locally. The next successful renewal is authoritative.
func TestOwnerLeasesTransientFailureRetriesWithinExpiry(t *testing.T) {
	var calls atomic.Int64
	failed := make(chan struct{})
	recovered := make(chan struct{})
	c := RecoveryClaim{Scope: owner.DOScope("app", "worker", "Class", 0), Node: "a", Epoch: 1}
	expiry := time.Now().Add(2 * time.Second).UnixMilli()
	leases := NewOwnerLeases(t.Context(), func(ctx context.Context, c RecoveryClaim, ttl time.Duration) (LeaseState, error) {
		switch calls.Add(1) {
		case 2:
			close(failed)
			return LeaseState{}, context.DeadlineExceeded
		case 3:
			close(recovered)
		}
		return LeaseState{Node: c.Node, Epoch: c.Epoch, Expiry: expiry}, nil
	})
	defer leases.Close()
	if _, err := leases.Register(t.Context(), c, 90*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("no renewal")
	}
	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("transient failure abandoned lease")
	}
	if state, err := leases.Status(c); err != nil || state.Expiry != expiry {
		t.Fatalf("state=%+v err=%v", state, err)
	}
}

func TestOwnerLeasesTransientFailureNeverExtendsExpiry(t *testing.T) {
	var calls atomic.Int64
	c := RecoveryClaim{Scope: owner.DOScope("app", "worker", "Class", 0), Node: "a", Epoch: 1}
	expiry := time.Now().Add(150 * time.Millisecond).UnixMilli()
	leases := NewOwnerLeases(t.Context(), func(ctx context.Context, c RecoveryClaim, ttl time.Duration) (LeaseState, error) {
		if calls.Add(1) > 1 {
			return LeaseState{}, context.DeadlineExceeded
		}
		return LeaseState{Node: c.Node, Epoch: c.Epoch, Expiry: expiry}, nil
	})
	defer leases.Close()
	if _, err := leases.Register(t.Context(), c, 60*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(time.UnixMilli(expiry).Add(80 * time.Millisecond)))
	if _, err := leases.Status(c); err == nil {
		t.Fatal("expired lease served")
	}
	before := calls.Load()
	time.Sleep(80 * time.Millisecond)
	if calls.Load() != before {
		t.Fatal("continued renewal after expiry")
	}
}
