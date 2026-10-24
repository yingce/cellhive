package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
	"cellhive/internal/lease"
	"cellhive/internal/owner"
)

func TestHotKVAccessRefreshesBeforeCacheExpiry(t *testing.T) {
	s := newScopeServer(t)
	s.Cfg.BindingCacheTTL = time.Second
	s.Owner = &owner.Manager{B: s.Bucket, NodeID: s.Cfg.NodeID, Session: "warm-test", Role: cell.RoleCellAgent, OwnerTTL: 10 * time.Second}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "KV", "main", "test"); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []cell.Scope{control.Scope(), sc} {
		if _, err := s.Owner.Claim(ctx, scope, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.refreshHotKVAccess(ctx, &hotKVAccess{scope: sc, ns: "acme", name: "KV"}); err != nil {
		t.Fatal(err)
	}
	s.bindMu.Lock()
	first := s.bindCache["acme|kv|KV"].exp
	s.bindMu.Unlock()
	s.noteHotKVAccess(sc, "acme", "KV")
	s.noteHotKVAccess(sc, "acme", "KV")
	loopCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.runKVReadWarmLoop(loopCtx); close(done) }()
	defer func() { stop(); <-done }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.bindMu.Lock()
		next := s.bindCache["acme|kv|KV"].exp
		s.bindMu.Unlock()
		if next.After(first.Add(250 * time.Millisecond)) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("hot KV binding cache was not refreshed ahead of expiry")
}

func TestHotKVAccessSetIsBounded(t *testing.T) {
	s := newScopeServer(t)
	s.Cfg.BindingCacheTTL = time.Second
	s.Owner = &owner.Manager{}
	for i := 0; i < hotKVAccessLimit+5; i++ {
		s.noteHotKVAccess(cell.Scope{Namespace: "acme", Class: "__kv__", ID: fmt.Sprintf("c%d", i)}, "acme", "KV")
	}
	s.hotKVAccessMu.Lock()
	defer s.hotKVAccessMu.Unlock()
	if len(s.hotKVAccesses) != hotKVAccessLimit {
		t.Fatalf("hot KV access set = %d, want %d", len(s.hotKVAccesses), hotKVAccessLimit)
	}
	if s.hotKVAccesses["acme/__kv__/c0"] != nil {
		t.Fatal("oldest hot KV scope was not evicted")
	}
}

func TestSuccessfulPutsActivateKVCacheRefresh(t *testing.T) {
	s, _ := newTestServer(t)
	s.Cfg.BindingCacheTTL = time.Second
	if err := registerTestBinding(s, "acme", "kv", "KV", "main"); err != nil {
		t.Fatal(err)
	}
	token := mint(t, s, "acme", "kv", "KV")
	for i := 0; i < 2; i++ {
		if code := kvReq(t, s, http.MethodPost, "/v1/kv/put?ns=acme&key=k", token, []byte("v")); code != http.StatusOK {
			t.Fatalf("put %d returned %d", i, code)
		}
	}
	s.hotKVAccessMu.Lock()
	entry := s.hotKVAccesses["acme/__kv__/main"]
	s.hotKVAccessMu.Unlock()
	if entry == nil || !entry.active {
		t.Fatal("two successful PUTs did not activate background cache refresh")
	}
}

func TestUnpublishedSelfLeaseCannotServeCachedOwner(t *testing.T) {
	s, _ := newTestServer(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}
	if _, err := s.Owner.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Replace the published test lease with a fresh manager for the same
	// identity, simulating startup before its first successful Publish.
	s.Lease = lease.NewManager(s.Bucket, s.Owner.NodeID, s.Owner.Session, "", "", 10*time.Second)
	s.Owner.LeaseView = s.Lease
	req := httptest.NewRequest(http.MethodGet, "/v1/kv/get?ns=acme&key=k", nil)
	rr := httptest.NewRecorder()
	if !s.forwardOrClaim(rr, req, sc, nil) || rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("unpublished local owner was served: %d %s", rr.Code, rr.Body.String())
	}
}
