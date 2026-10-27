package owner

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/lease"
)

type routeCountBucket struct {
	bucket.Bucket
	gets atomic.Int64
}

func (b *routeCountBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.gets.Add(1)
	return b.Bucket.Get(ctx, key)
}

func TestRouteSurvivesShortTTL(t *testing.T) {
	for _, local := range []bool{true, false} {
		t.Run(map[bool]string{true: "local", false: "remote"}[local], func(t *testing.T) {
			m, raw := newManager(t, t.TempDir(), "a")
			lm := lease.NewManager(raw, m.NodeID, m.Session, "", "", 10*time.Second)
			if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
				t.Fatal(err)
			}
			m.LeaseView = lm
			sc := cell.Scope{Namespace: "app", Class: "kv", ID: "route"}
			if _, err := m.Claim(t.Context(), sc, time.Now()); err != nil {
				t.Fatal(err)
			}
			if !local {
				m.NodeID = "b"
				m.Session = "b-s1"
			}
			b := &routeCountBucket{Bucket: raw}
			m.B = b
			if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
				t.Fatal(err)
			}
			time.Sleep(1100 * time.Millisecond)
			if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
				t.Fatal(err)
			}
			if got := b.gets.Load(); got != 1 {
				t.Fatalf("owner GETs = %d, want 1", got)
			}
		})
	}
}

func TestRouteConcurrentMiss(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "burst"}
	if _, err := m.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	b := &routeCountBucket{Bucket: raw}
	m.B = b
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := b.gets.Load(); got != 1 {
		t.Fatalf("owner GETs = %d, want 1", got)
	}
}

func TestRouteInvalidationDiscardsDelayedResult(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "race"}
	old, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	b := &delayedOwnerGet{Bucket: raw, block: true, started: make(chan struct{}), release: make(chan struct{})}
	m.B = b
	done := make(chan cell.Owner, 1)
	go func() {
		o, _, err := m.ResolveRoute(t.Context(), sc)
		if err != nil {
			t.Error(err)
		}
		done <- o
	}()
	<-b.started
	old.Node = "b"
	old.Session = "b-s1"
	old.Epoch++
	data, _ := cell.MarshalOwner(old)
	if _, err := raw.Put(t.Context(), Key(sc), data); err != nil {
		t.Fatal(err)
	}
	m.Invalidate(sc)
	close(b.release)
	if o := <-done; o.Node != "b" {
		t.Fatalf("returned stale in-flight owner: %+v", o)
	}
}

func TestRouteExpiredLocalEvidenceDoesNotRevive(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	lm := lease.NewManager(raw, m.NodeID, m.Session, "", "", 10*time.Second)
	m.LeaseView = lm
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "revived"}
	old, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	// Simulate a missed safe lease window, followed by a recovered lease and
	// ownership that has moved while the process could not serve.
	m.mu.Lock()
	c := m.routes[sc.String()]
	c.until = time.Now().Add(-time.Second)
	m.routes[sc.String()] = c
	m.mu.Unlock()
	old.Node = "b"
	old.Session = "b-s1"
	old.Epoch++
	data, _ := cell.MarshalOwner(old)
	if _, err := raw.Put(t.Context(), Key(sc), data); err != nil {
		t.Fatal(err)
	}
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	if o, _, err := m.ResolveRoute(t.Context(), sc); err != nil || o.Node != "b" {
		t.Fatalf("revived old local route: %+v %v", o, err)
	}
}

func TestRouteReleaseAndClaimInvalidateImmediately(t *testing.T) {
	m, _ := newManager(t, t.TempDir(), "a")
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "release"}
	o, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(t.Context(), sc, o.Epoch); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err != ErrUnowned {
		t.Fatalf("released route: %v", err)
	}
	next, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got, _, err := m.ResolveRoute(t.Context(), sc); err != nil || got.Epoch != next.Epoch || got.Epoch <= o.Epoch {
		t.Fatalf("reclaimed route: %+v %v", got, err)
	}
}

func TestRouteWaiterCancellation(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "cancel"}
	if _, err := m.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	b := &delayedOwnerGet{Bucket: raw, block: true, started: make(chan struct{}), release: make(chan struct{})}
	m.B = b
	done := make(chan error, 1)
	go func() { _, _, err := m.ResolveRoute(t.Context(), sc); done <- err }()
	<-b.started
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := m.ResolveRoute(ctx, sc); err != context.DeadlineExceeded {
		t.Fatalf("cancelled waiter: %v", err)
	}
	close(b.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRouteRemoteDeadlineFixedAndLiveUsesEvidence(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	lm := lease.NewManager(raw, m.NodeID, m.Session, "", "", 10*time.Second)
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "remote-deadline"}
	if _, err := m.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	m.NodeID = "b"
	m.Session = "b-s1"
	m.LeaseView = lm
	o, _, err := m.ResolveRoute(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	before := m.routes[sc.String()]
	m.mu.Unlock()
	lm.B = failingOwnerLeaseBucket{raw}
	if live, err := m.LiveRoute(t.Context(), sc, o, time.Now()); err != nil || !live {
		t.Fatalf("route evidence not reused: %v %v", live, err)
	}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	after := m.routes[sc.String()]
	m.mu.Unlock()
	if !after.until.Equal(before.until) {
		t.Fatal("foreign route expiry slid forward")
	}
	// Wall-clock expiry also forbids reuse even while monotonic time is valid.
	m.mu.Lock()
	after.wallExpiry = time.Now().Add(-time.Second).UnixMilli()
	m.routes[sc.String()] = after
	m.mu.Unlock()
	m.B = failingOwnerLeaseBucket{raw}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err == nil {
		t.Fatal("reused wall-expired route")
	}
}

func TestRouteLocalRenewalExtendsWithoutOwnerRead(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	lm := lease.NewManager(raw, m.NodeID, m.Session, "", "", 500*time.Millisecond)
	m.LeaseView = lm
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "continuous"}
	if _, err := m.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	b := &routeCountBucket{Bucket: raw}
	m.B = b
	if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		time.Sleep(100 * time.Millisecond)
		if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
			t.Fatal(err)
		}
	}
	if got := b.gets.Load(); got != 1 {
		t.Fatalf("continuous owner lookup count=%d", got)
	}
}

func TestRouteFreshLeaseConfirmsExpiredSnapshot(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "stale-lease"}
	o, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	m.NodeID = "b"
	m.Session = "b-s1"
	m.LeaseView = freshLeaseView{stale: fakeLeaseView{o.Node: {Node: o.Node, Session: o.Session, Expiry: time.Now().Add(-time.Second).UnixMilli()}}, fresh: fakeLeaseView{o.Node: {Node: o.Node, Session: o.Session, Expiry: time.Now().Add(time.Minute).UnixMilli()}}}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	m.B = failingOwnerLeaseBucket{raw}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatalf("fresh lease was not cached: %v", err)
	}
}

func TestRouteSessionMismatchIsNotCached(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "new-session"}
	o, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	m.NodeID = "b"
	m.Session = "b-s1"
	m.LeaseView = fakeLeaseView{o.Node: {Node: o.Node, Session: "restarted", Expiry: time.Now().Add(time.Minute).UnixMilli()}}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err != nil {
		t.Fatal(err)
	}
	if live, err := m.LiveRoute(t.Context(), sc, o, time.Now()); err != nil || live {
		t.Fatalf("old process still live: %v %v", live, err)
	}
	m.B = failingOwnerLeaseBucket{raw}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err == nil {
		t.Fatal("cached route for obsolete session")
	}
}

func TestRouteDelayedLocalReadCannotCrossLeaseGap(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	lm := lease.NewManager(raw, m.NodeID, m.Session, "", "", 200*time.Millisecond)
	m.LeaseView = lm
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "delayed-lease-gap"}
	old, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	b := &delayedOwnerGet{Bucket: raw, block: true, started: make(chan struct{}), release: make(chan struct{})}
	m.B = b
	done := make(chan cell.Owner, 1)
	go func() {
		o, _, err := m.ResolveRoute(t.Context(), sc)
		if err != nil {
			t.Error(err)
		}
		done <- o
	}()
	<-b.started
	time.Sleep(220 * time.Millisecond)
	old.Node = "b"
	old.Session = "b-s1"
	old.Epoch++
	data, _ := cell.MarshalOwner(old)
	if _, err := raw.Put(t.Context(), Key(sc), data); err != nil {
		t.Fatal(err)
	}
	if err := lm.Publish(t.Context(), lease.Load{}); err != lease.ErrFenced {
		t.Fatalf("expired session should be permanently fenced: %v", err)
	}
	close(b.release)
	o := <-done
	if live, err := m.LiveRoute(t.Context(), sc, o, time.Now()); err != nil || live {
		t.Fatalf("late route authorized expired session: %v %v", live, err)
	}
}

func TestForwardHintUsesRenewedPeerLeaseWithoutRevivingAuthority(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "a")
	lm := lease.NewManager(raw, m.NodeID, m.Session, "", "", 10*time.Second)
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	m.LeaseView = lm
	sc := cell.Scope{Namespace: "app", Class: "kv", ID: "forward-renew"}
	if _, err := m.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	m.NodeID = "b"
	m.Session = "b-s1"
	original, _, err := m.ResolveRoute(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	c := m.routes[sc.String()]
	c.until = time.Now().Add(-time.Second)
	c.wallExpiry = c.until.UnixMilli()
	m.routes[sc.String()] = c
	m.mu.Unlock()
	lm.SetCachedForTest([]lease.NodeLease{{Node: original.Node, Session: original.Session, Expiry: time.Now().Add(time.Minute).UnixMilli()}})
	m.B = failingOwnerLeaseBucket{raw}
	if got, _, err := m.ResolveForwardRoute(t.Context(), sc); err != nil || got != original {
		t.Fatalf("forward hint: %+v %v", got, err)
	}
	if _, ok := m.LocalReadGuard(sc); ok {
		t.Fatal("remote hint became local authority")
	}
	if _, _, err := m.ResolveRoute(t.Context(), sc); err == nil {
		t.Fatal("forward hint revived authoritative route")
	}
}

func TestForwardHintRejectsChangedOrExpiredSession(t *testing.T) {
	for _, mode := range []string{"changed", "expired", "missing", "invalidated", "self"} {
		t.Run(mode, func(t *testing.T) {
			m, raw := newManager(t, t.TempDir(), "b")
			lm := lease.NewManager(raw, "b", m.Session, "", "", 10*time.Second)
			m.LeaseView = lm
			sc := cell.Scope{Namespace: "app", Class: "kv", ID: "hint"}
			o := cell.Owner{Node: "a", Session: "a-s1", Liveness: "node-lease"}
			l := lease.NodeLease{Node: o.Node, Session: o.Session, Expiry: time.Now().Add(time.Minute).UnixMilli()}
			switch mode {
			case "changed":
				l.Session = "a-s2"
			case "expired":
				l.Expiry = time.Now().Add(-time.Second).UnixMilli()
			case "self":
				o.Node = m.NodeID
				l.Node = m.NodeID
			}
			m.routes = map[string]ownerRoute{sc.String(): {owner: o, until: time.Now().Add(-time.Second)}}
			if mode != "missing" {
				lm.SetCachedForTest([]lease.NodeLease{l})
			}
			if mode == "invalidated" {
				m.Invalidate(sc)
			}
			m.B = failingOwnerLeaseBucket{raw}
			if _, _, err := m.ResolveForwardRoute(t.Context(), sc); err == nil {
				t.Fatal("unsafe hint accepted")
			}
		})
	}
}
