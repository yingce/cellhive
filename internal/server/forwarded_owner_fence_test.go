package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/owner"
	"cellhive/internal/ownerclient"
)

func TestAlreadyForwardedRequestCannotRunOnNonOwner(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			s, _ := newTestServer(t)
			scope := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "foreign"}
			remote := &owner.Manager{B: s.Bucket, NodeID: "node-2", Session: "s2", Advertise: "127.0.0.1:1", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
			if _, err := remote.Claim(t.Context(), scope, time.Now()); err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(method, "/v1/kv/get?ns=acme&key=k", strings.NewReader("value"))
			req.Header.Set(ownerclient.ForwardedHeader, "1")
			rr := httptest.NewRecorder()
			if handled := s.forwardOrClaim(rr, req, scope, nil); !handled || rr.Code != http.StatusConflict {
				t.Fatalf("foreign forwarded request admitted: handled=%v status=%d body=%s", handled, rr.Code, rr.Body.String())
			}
		})
	}
}

// A failed second route lookup must not undo the caller's foreign-owner decision.
func TestNonOwnerRouteFallbackCannotRunLocally(t *testing.T) {
	for _, admin := range []bool{false, true} {
		t.Run(map[bool]string{false: "data", true: "admin"}[admin], func(t *testing.T) {
			s, _ := newTestServer(t)
			s.Owner = nil // route disappeared after the caller established a foreign owner
			req := httptest.NewRequest(http.MethodGet, "/v1/kv/get", nil)
			rr := httptest.NewRecorder()
			if handled := s.forwardNonOwner(rr, req, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "foreign"}, nil, admin); !handled || rr.Code != http.StatusConflict {
				t.Fatalf("foreign owner fallback admitted: handled=%v status=%d", handled, rr.Code)
			}
		})
	}
}
