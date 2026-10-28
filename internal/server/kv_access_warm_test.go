package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/lease"
)

func TestKVWarmLoopDropsLegacyEntries(t *testing.T) {
	s := newScopeServer(t)
	s.hotKVAccesses = map[string]*hotKVAccess{"legacy": {scope: cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.runKVReadWarmLoop(ctx); close(done) }()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done
	s.hotKVAccessMu.Lock()
	defer s.hotKVAccessMu.Unlock()
	if len(s.hotKVAccesses) != 0 {
		t.Fatalf("legacy KV warm entries remain: %d", len(s.hotKVAccesses))
	}
}

func TestSuccessfulPutsDoNotActivateKVCacheRefresh(t *testing.T) {
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
	if entry != nil {
		t.Fatal("successful PUTs activated removed background cache refresh")
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
