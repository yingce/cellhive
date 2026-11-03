package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
)

// Registry mutation durability is no longer an HTTP contract. Retain the
// no-side-effects boundary for every retired route, even with capture unavailable.
func TestRetiredIssuerRoutesNeverCaptureOrReturnKeys(t *testing.T) {
	s, _, _ := newControlForwardServer(t, t.TempDir(), "issuer-retired")
	s.Owner = nil
	var captures atomic.Int32
	s.Capture = &cellcapture.Manager{Store: s.Store, AutoSnapshot: true,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: testCommitterFunc(func(context.Context, cell.Scope, uint64, []byte) error {
			captures.Add(1)
			return errors.New("capture unavailable")
		}),
	}
	defer s.Capture.Stop()
	for _, path := range []string{"/v1/control/issuers", "/v1/control/issuers/acme"} {
		for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodDelete} {
			for _, body := range []string{`{"name":"valid","namespaces":["acme"],"max_token_ttl_seconds":60}`, strings.Repeat("x", 20000)} {
				w := adminDo(t, s.AdminHandler(), method, path, testRoleJWT(t, "admin"), body)
				if w.Code != 404 || strings.Contains(w.Body.String(), `"key"`) || captures.Load() != 0 {
					t.Fatalf("retired %s %s: %d captures=%d %s", method, path, w.Code, captures.Load(), w.Body.String())
				}
			}
		}
	}
	issuers, err := s.Control.ListIssuers(t.Context())
	if err != nil || len(issuers) != 0 {
		t.Fatalf("retired routes mutated registry: %v %v", issuers, err)
	}
}
