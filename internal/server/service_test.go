package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cellhive/internal/control"
	"cellhive/internal/scopedtoken"
)

// TestServiceFetchProxy covers the cell-agent service.fetch handler: scope-kind
// auth, target resolution from the projection, and dispatch to user-runtime.
func TestServiceFetchProxy(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.Control = control.New(srv.Store, nil)
	ctx := context.Background()
	if _, err := srv.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := srv.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "shaB"}, "ops"); err != nil {
		t.Fatalf("deploy api: %v", err)
	}
	if _, err := srv.Control.Deploy(ctx, "acme", "web", control.DeploySpec{
		BundleSHA: "shaA",
		Bindings:  []control.Binding{{Type: "service", Name: "SVC", ID: "api"}},
	}, "ops"); err != nil {
		t.Fatalf("deploy web: %v", err)
	}

	var got map[string]any
	disp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/services/fetch" || r.Header.Get("x-cellhive-internal-token") != "tok" {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("content-type", "text/plain")
		_, _ = w.Write([]byte("from-B"))
	}))
	defer disp.Close()
	srv.Cfg.DispatchURL = disp.URL
	srv.Cfg.TokenDispatch = "tok"

	tok, err := scopedtoken.Mint([]byte(srv.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "service", Name: "SVC"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/service/fetch?ns=acme&worker=api", bytes.NewReader([]byte("hi")))
	req.Header.Set("x-cellhive-scope-token", tok)
	req.Header.Set("x-cellhive-req-method", "POST")
	req.Header.Set("x-cellhive-req-url", "http://svc/")
	req.Header.Set("x-cellhive-req-content-type", "text/plain")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || rr.Body.String() != "from-B" {
		t.Fatalf("service fetch = %d %q", rr.Code, rr.Body.String())
	}
	if got["worker"] != "api" || got["bundle_sha"] != "shaB" || got["namespace"] != "acme" {
		t.Fatalf("dispatch payload = %+v", got)
	}
	if got["body"] != "aGk=" { // base64("hi")
		t.Fatalf("dispatch body = %v", got["body"])
	}

	// A token of the wrong kind is rejected.
	bad, _ := scopedtoken.Mint([]byte(srv.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "kv", Name: "SVC"})
	req2 := httptest.NewRequest(http.MethodPost, "/v1/service/fetch?ns=acme&worker=api", nil)
	req2.Header.Set("x-cellhive-scope-token", bad)
	rr2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusForbidden {
		t.Fatalf("wrong-kind token = %d, want 403", rr2.Code)
	}
}

// TestServiceBindingACLDeployGate: a cross-namespace service binding is rejected
// at deploy time until the target namespace grants it (ADR-144); same-namespace
// bindings are unaffected.
func TestServiceBindingACLDeployGate(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"team"}`)

	sha := func(src string) string {
		b := mustAdmin(t, admin, http.MethodPost, "/v1/control/bundle", src)
		var bo struct {
			Sha string `json:"sha"`
		}
		_ = json.Unmarshal(b.Body.Bytes(), &bo)
		if bo.Sha == "" {
			t.Fatal("bundle put returned no sha")
		}
		return bo.Sha
	}
	shaTarget := sha("export default {}")
	shaWeb := sha("export default {}")
	mustAdmin(t, admin, http.MethodPost, "/v1/control/deploy",
		`{"namespace":"team","worker":"api","bundle_sha":"`+shaTarget+`"}`)

	body := func(target string) string {
		return `{"namespace":"acme","worker":"web","bundle_sha":"` + shaWeb + `","bindings":[{"type":"service","name":"SVC","id":"` + target + `"}]}`
	}

	// Same namespace: no grant needed.
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", body("api")); rr.Code != http.StatusOK {
		t.Fatalf("same-ns binding rejected: %d %s", rr.Code, rr.Body.String())
	}
	// Cross namespace without a grant: rejected with a structured finding.
	rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", body("team/api"))
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "service_binding_denied") {
		t.Fatalf("cross-ns binding = %d %s, want 400 service_binding_denied", rr.Code, rr.Body.String())
	}
	// Malformed target: rejected.
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", body("team/api/extra")); rr.Code != http.StatusBadRequest {
		t.Fatalf("malformed target accepted: %d", rr.Code)
	}
	// Grant + list, then the cross-ns deploy succeeds.
	mustAdmin(t, admin, http.MethodPost, "/v1/control/service-acl",
		`{"namespace":"team","worker":"api","caller_namespace":"acme"}`)
	lst := mustAdmin(t, admin, http.MethodGet, "/v1/control/service-acls?namespace=team", "")
	if !strings.Contains(lst.Body.String(), `"caller_ns":"acme"`) {
		t.Fatalf("list = %s", lst.Body.String())
	}
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", body("team/api")); rr.Code != http.StatusOK {
		t.Fatalf("granted cross-ns binding rejected: %d %s", rr.Code, rr.Body.String())
	}
	// Revoke: rejected again.
	mustAdmin(t, admin, http.MethodDelete, "/v1/control/service-acl?namespace=team&worker=api&caller_namespace=acme", "")
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", body("team/api")); rr.Code != http.StatusBadRequest {
		t.Fatalf("revoked grant still allowed: %d", rr.Code)
	}
}

// TestServiceFetchCrossNamespaceACL: the runtime call is re-checked, so revoking
// a grant blocks new calls even for an already-deployed binding.
func TestServiceFetchCrossNamespaceACL(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.Control = control.New(srv.Store, nil)
	ctx := context.Background()
	for _, ns := range []string{"acme", "team"} {
		if _, err := srv.Control.CreateApp(ctx, ns, "ops"); err != nil {
			t.Fatalf("app %s: %v", ns, err)
		}
	}
	if _, err := srv.Control.Deploy(ctx, "team", "api", control.DeploySpec{BundleSHA: "shaT"}, "ops"); err != nil {
		t.Fatalf("deploy team/api: %v", err)
	}
	if _, err := srv.Control.Deploy(ctx, "acme", "web", control.DeploySpec{
		BundleSHA: "shaW",
		Bindings:  []control.Binding{{Type: "service", Name: "SVC", ID: "team/api"}},
	}, "ops"); err != nil {
		t.Fatalf("deploy acme/web: %v", err)
	}
	disp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("from-team"))
	}))
	defer disp.Close()
	srv.Cfg.DispatchURL = disp.URL
	srv.Cfg.TokenDispatch = "tok"

	tok, err := scopedtoken.Mint([]byte(srv.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "service", Name: "SVC"})
	if err != nil {
		t.Fatal(err)
	}
	call := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/service/fetch?ns=acme&target_ns=team&worker=api", bytes.NewReader([]byte("hi")))
		req.Header.Set("x-cellhive-scope-token", tok)
		req.Header.Set("x-cellhive-req-method", "POST")
		req.Header.Set("x-cellhive-req-url", "http://svc/")
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
		return rr
	}
	if rr := call(); rr.Code != http.StatusForbidden {
		t.Fatalf("cross-ns call without grant = %d %s, want 403", rr.Code, rr.Body.String())
	}
	if err := srv.Control.PutServiceACL(ctx, "team", "api", "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if rr := call(); rr.Code != http.StatusOK || rr.Body.String() != "from-team" {
		t.Fatalf("granted cross-ns call = %d %q", rr.Code, rr.Body.String())
	}
}

// TestServiceFetchPropagatesTraceparent: cell-agent carries the caller's W3C
// trace context onto its own dispatch to user-runtime (ADR-146).
func TestServiceFetchPropagatesTraceparent(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.Control = control.New(srv.Store, nil)
	ctx := context.Background()
	if _, err := srv.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "shaB"}, "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Control.Deploy(ctx, "acme", "web", control.DeploySpec{
		BundleSHA: "shaA",
		Bindings:  []control.Binding{{Type: "service", Name: "SVC", ID: "api"}},
	}, "ops"); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	disp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte("ok"))
	}))
	defer disp.Close()
	srv.Cfg.DispatchURL = disp.URL
	srv.Cfg.TokenDispatch = "tok"

	tok, err := scopedtoken.Mint([]byte(srv.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "service", Name: "SVC"})
	if err != nil {
		t.Fatal(err)
	}
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	req := httptest.NewRequest(http.MethodPost, "/v1/service/fetch?ns=acme&worker=api", bytes.NewReader([]byte("hi")))
	req.Header.Set("x-cellhive-scope-token", tok)
	req.Header.Set("traceparent", tp)
	req.Header.Set("x-cellhive-req-method", "POST")
	req.Header.Set("x-cellhive-req-url", "http://svc/")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("service fetch = %d %s", rr.Code, rr.Body.String())
	}
	if got["traceparent"] != tp {
		t.Fatalf("dispatch traceparent = %v, want %q", got["traceparent"], tp)
	}
}
