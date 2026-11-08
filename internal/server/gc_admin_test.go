package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func gcAdminReq(t *testing.T, s *Server, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	s.handleControlGCScope(rr, httptest.NewRequest(http.MethodPost, "/v1/control/gc/scope", strings.NewReader(string(raw))))
	return rr
}

func TestGCScopeRequiresSafeUnlessDryRun(t *testing.T) {
	s, _ := newTestServer(t)
	rr := gcAdminReq(t, s, map[string]any{"scope": "acme/__kv__/main"})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rr.Code, rr.Body.String())
	}
}

func TestGCScopeDryRunOnOwner(t *testing.T) {
	s, _ := newTestServer(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "gc-admin"}
	if _, err := s.Owner.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	rr := gcAdminReq(t, s, map[string]any{"scope": sc.String(), "dry_run": true})
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"dry_run":true`) {
		t.Fatalf("response = %s", rr.Body.String())
	}
}

func TestGCScopeRejectsNonOwner(t *testing.T) {
	s, _ := newTestServer(t)
	rr := gcAdminReq(t, s, map[string]any{"scope": "other/__kv__/main", "safe": true, "dry_run": true})
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rr.Code, rr.Body.String())
	}
}
