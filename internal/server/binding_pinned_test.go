package server

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
	"cellhive/internal/scopedtoken"
)

func TestPinnedKVVersionMappingAndRevocation(t *testing.T) {
	s := newScopeServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"old-cell", "new-cell"} {
		if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: id, Bindings: []control.Binding{{Type: "kv", Name: "KV", ID: id}}}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	for version, want := range map[int]string{1: "old-cell", 2: "new-cell"} {
		spec, _ := s.bindingSpecFor(ctx, "acme", "web", version)
		token := spec["KV"].(map[string]any)["token"].(string)
		claims, err := scopedtoken.VerifyIssuer(s.scopeSecret(), token, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if claims.Resource != want || claims.Version != version || claims.Worker != "web" {
			t.Fatalf("claims: %+v", claims)
		}
		if got := kvReq(t, s, "POST", "/v1/kv/put?ns=acme&key=version", token, []byte(want)); got != 200 {
			t.Fatalf("version %d PUT: %d", version, got)
		}
		c, err := s.Store.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: want})
		if err != nil {
			t.Fatal(err)
		}
		var value string
		if err := c.DB.QueryRow("SELECT value FROM kv WHERE key='version'").Scan(&value); err != nil || value != want {
			t.Fatalf("pinned value %q: %v", value, err)
		}
	}
	spec, _ := s.bindingSpecFor(ctx, "acme", "web", 1)
	token := spec["KV"].(map[string]any)["token"].(string)
	if err := s.resourceReplica().BlockResource(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "old-cell"}, "delete-old"); err != nil {
		t.Fatal(err)
	}
	if got := kvReq(t, s, "POST", "/v1/kv/put?ns=acme&key=fenced", token, []byte("bad")); got != 403 {
		t.Fatalf("pinned resource fence: %d", got)
	}
	if err := s.Control.DeleteResource(ctx, "acme", "kv", "KV", "test"); err != nil {
		t.Fatal(err)
	}
	s.invalidateBindings("acme")
	req := httptest.NewRequest("POST", "/v1/kv/put?ns=acme&key=revoked", strings.NewReader("bad"))
	req.Header.Set("x-cellhive-scope-token", token)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "binding_not_registered") {
		t.Fatalf("revoked pinned token: %d %s", rec.Code, rec.Body.String())
	}
}

func TestPinnedKVScopeIsolation(t *testing.T) {
	s := newScopeServer(t)
	for _, ref := range []string{"other/__kv__/x", "acme/__d1__/x", "bad/path"} {
		ctx := context.WithValue(t.Context(), ctxPinnedKV, scopedtoken.Claims{Namespace: "acme", Name: "KV", Resource: ref})
		if _, err := s.kvScopeFor(ctx, "acme", "KV"); err == nil {
			t.Fatalf("accepted %q", ref)
		}
	}
}
