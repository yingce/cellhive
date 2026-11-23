package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/lease"
)

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
