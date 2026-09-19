package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"cellhive/internal/auth"
	"cellhive/internal/control"
)

// scopedServer returns a control server whose admin plane authenticates with an
// HS256 JWT granting only the given namespaces.
func scopedServer(t *testing.T, ns ...string) (*Server, string) {
	t.Helper()
	s := newControlServer(t)
	secret := []byte("authz-test-secret-0123456789ab")
	s.AdminAuth = auth.Any{&auth.JWTBearer{HS256Secret: secret}}
	token := signJWT(t, secret, map[string]any{
		"sub": "tenant@corp", "cellhive_ns": ns, "exp": time.Now().Add(time.Hour).Unix(),
	})
	return s, token
}

// TestScopedCredentialAuthorizationMatrix covers the ADR-131 follow-up: every
// namespace-scoped admin endpoint must reject a credential that is not granted
// the target namespace (rollback/asset/logs/deploy were previously unguarded).
func TestScopedCredentialAuthorizationMatrix(t *testing.T) {
	ctx := context.Background()
	s, token := scopedServer(t, "acme")
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app acme: %v", err)
	}
	if _, err := s.Control.CreateApp(ctx, "other", "ops"); err != nil {
		t.Fatalf("app other: %v", err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "sha1"}, "ops"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	cases := []struct {
		name   string
		method string
		path   string
		body   string
		ownNS  bool
	}{
		{"rollback foreign", http.MethodPost, "/v1/control/rollback", `{"namespace":"other","worker":"web"}`, false},
		{"rollback own", http.MethodPost, "/v1/control/rollback", `{"namespace":"acme","worker":"web"}`, true},
		{"asset foreign", http.MethodPost, "/v1/control/asset?ns=other&worker=web&path=x.txt", "data", false},
		{"asset own", http.MethodPost, "/v1/control/asset?ns=acme&worker=web&path=x.txt", "data", true},
		{"logs foreign", http.MethodGet, "/v1/control/logs?namespace=other&worker=web", "", false},
		{"logs own", http.MethodGet, "/v1/control/logs?namespace=acme&worker=web", "", true},
		{"deploy foreign", http.MethodPost, "/v1/control/deploy", `{"namespace":"other","worker":"web","bundle_sha":"sha1"}`, false},
		{"secret foreign", http.MethodPost, "/v1/control/secret", `{"namespace":"other","worker":"web","key":"K","value":"dg=="}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := adminTokenDo(t, s, tc.method, tc.path, token, tc.body)
			if tc.ownNS {
				if rr.Code == http.StatusForbidden {
					t.Fatalf("own namespace was rejected: %d %s", rr.Code, rr.Body.String())
				}
				return
			}
			if rr.Code != http.StatusForbidden {
				t.Fatalf("foreign namespace = %d, want 403: %s", rr.Code, rr.Body.String())
			}
		})
	}
	// The denied deploy must not have written DO class state for "other".
	aliases, deleted, err := s.Control.DOClassState(ctx, "other", "web")
	if err != nil {
		t.Fatalf("do class state: %v", err)
	}
	if len(aliases) != 0 || len(deleted) != 0 {
		t.Fatalf("denied deploy mutated another namespace: aliases=%v deleted=%v", aliases, deleted)
	}
}

// TestScopedCredentialForwarding covers that authorization happens on the node
// that receives the admin request, BEFORE forwarding to the control owner —
// otherwise the owner would serve it from its internal plane without a
// principal. It also checks the apps list is filtered locally after the fetch.
func TestScopedCredentialForwarding(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srvA, omA, hsA := newControlForwardServer(t, dir, "authz-node-1")
	srvB, _, _ := newControlForwardServer(t, dir, "authz-node-2")
	// Forwarding needs the owner record to carry A's address.
	omA.Advertise = hsA.URL
	// Claim the control scope on A via its HTTP plane.
	if rr := do(t, srvA.Handler(), http.MethodPost, "/v1/control/app", "tok",
		[]byte(`{"namespace":"acme"}`)); rr.Code != http.StatusOK {
		t.Fatalf("claim app = %d %s", rr.Code, rr.Body.String())
	}
	if rr := do(t, srvA.Handler(), http.MethodPost, "/v1/control/app", "tok",
		[]byte(`{"namespace":"other"}`)); rr.Code != http.StatusOK {
		t.Fatalf("other app = %d %s", rr.Code, rr.Body.String())
	}
	// B authenticates with a scope-limited JWT and must refuse foreign namespaces
	// locally (403), never forwarding them.
	secret := []byte("authz-fwd-secret-0123456789abcd")
	srvB.AdminAuth = auth.Any{&auth.JWTBearer{HS256Secret: secret}}
	token := signJWT(t, secret, map[string]any{
		"sub": "tenant@corp", "cellhive_ns": []string{"acme"}, "exp": time.Now().Add(time.Hour).Unix(),
	})
	rr := adminTokenDo(t, srvB, http.MethodPost, "/v1/control/app", token, `{"namespace":"other"}`)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("forwarded foreign app create = %d, want 403: %s", rr.Code, rr.Body.String())
	}
	// An own-namespace request is forwarded to the owner and succeeds.
	rr = adminTokenDo(t, srvB, http.MethodPost, "/v1/control/route", token,
		`{"namespace":"acme","host":"fwd.test","worker":"web"}`)
	if rr.Code == http.StatusForbidden {
		t.Fatalf("own namespace denied on the forwarding node: %s", rr.Body.String())
	}
	// The apps list is fetched from the owner and filtered by the grant.
	rr = adminTokenDo(t, srvB, http.MethodGet, "/v1/control/apps", token, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("apps via forwarding = %d %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "other") || !strings.Contains(rr.Body.String(), "acme") {
		t.Fatalf("apps list not filtered: %s", rr.Body.String())
	}
	// Sanity: the owner still has both apps.
	if _, err := srvA.Control.GetApp(ctx, "other"); err != nil {
		t.Fatalf("owner lost the other app: %v", err)
	}
}

// TestPromoteRollbackInvalidatesBindingCache covers that a pointer switch drops
// the cached binding lookup so a service/scope check sees the new version's
// declaration immediately.
func TestPromoteRollbackInvalidatesBindingCache(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	s.Cfg.BindingCacheTTL = time.Minute
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	v1, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{
		BundleSHA: "sha1",
		Bindings:  []control.Binding{{Type: "kv", Name: "KV", ID: "acme/__kv__/v1"}},
	}, "ops")
	if err != nil {
		t.Fatalf("deploy v1: %v", err)
	}
	v2, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{
		BundleSHA: "sha2",
		Bindings:  []control.Binding{{Type: "kv", Name: "KV", ID: "acme/__kv__/v2"}},
	}, "ops")
	if err != nil {
		t.Fatalf("deploy v2: %v", err)
	}
	info := func() string {
		_, id, err := s.bindingInfo(ctx, "acme", "kv", "KV")
		if err != nil {
			t.Fatalf("bindingInfo: %v", err)
		}
		return id
	}
	if got := info(); got != "acme/__kv__/v2" {
		t.Fatalf("binding after v2 = %q", got)
	}
	// Promote back to v1 through the admin API: the cache must be invalidated.
	if rr := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/promote", "admin-tok",
		`{"namespace":"acme","worker":"web","version":`+strconv.Itoa(v1.Number)+`}`); rr.Code != http.StatusOK {
		t.Fatalf("promote = %d %s", rr.Code, rr.Body.String())
	}
	if got := info(); got != "acme/__kv__/v1" {
		t.Fatalf("binding after promote = %q, want the v1 declaration (cache not invalidated)", got)
	}
	// Rollback to v2 has the same requirement.
	if rr := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/rollback", "admin-tok",
		`{"namespace":"acme","worker":"web"}`); rr.Code != http.StatusOK {
		t.Fatalf("rollback = %d %s", rr.Code, rr.Body.String())
	}
	if got := info(); got != "acme/__kv__/v2" {
		t.Fatalf("binding after rollback = %q", got)
	}
	_ = v2
}

// TestWorkerEnvVersionSHAAndCompat covers ADR-134: the worker env endpoint
// resolves a pinned version (by number or bundle sha) and carries the version's
// compatibility settings to the runtime.
func TestWorkerEnvVersionSHAAndCompat(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{
		BundleSHA: "sha1", CompatDate: "2026-06-22", CompatFlags: []string{"nodejs_compat"},
		Bindings: []control.Binding{{Type: "kv", Name: "KV", ID: "acme/__kv__/v1"}},
	}, "ops"); err != nil {
		t.Fatalf("deploy v1: %v", err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{
		BundleSHA: "sha2", CompatDate: "2026-06-15",
		Bindings: []control.Binding{{Type: "kv", Name: "KV", ID: "acme/__kv__/v2"}},
	}, "ops"); err != nil {
		t.Fatalf("deploy v2: %v", err)
	}
	get := func(path string) *httptest.ResponseRecorder {
		return do(t, s.Handler(), http.MethodGet, path, "tok", nil)
	}
	// Active version.
	rr := get("/v1/control/worker?ns=acme&worker=web")
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"bundle_sha":"sha2"`) {
		t.Fatalf("active worker env = %d %s", rr.Code, rr.Body.String())
	}
	// Pinned by version number.
	rr = get("/v1/control/worker?ns=acme&worker=web&version=1")
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"bundle_sha":"sha1"`) ||
		!strings.Contains(rr.Body.String(), `"compat_date":"2026-06-22"`) ||
		!strings.Contains(rr.Body.String(), "nodejs_compat") {
		t.Fatalf("version=1 env = %d %s", rr.Code, rr.Body.String())
	}
	// Pinned by bundle sha (what a service binding stores, ADR-104).
	rr = get("/v1/control/worker?ns=acme&worker=web&sha=sha1")
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"bundle_sha":"sha1"`) {
		t.Fatalf("sha=sha1 env = %d %s", rr.Code, rr.Body.String())
	}
	if rr := get("/v1/control/worker?ns=acme&worker=web&sha=missing"); rr.Code != http.StatusNotFound {
		t.Fatalf("unknown sha = %d, want 404", rr.Code)
	}
	// Internal dispatch spec carries the compatibility settings too.
	rr = get("/v1/internal/worker/bindings?ns=acme&worker=web&version=1")
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"compat_date":"2026-06-22"`) ||
		!strings.Contains(rr.Body.String(), `"nodejs_compat"`) {
		t.Fatalf("internal bindings = %d %s", rr.Code, rr.Body.String())
	}
	// The derived bindings table follows the active version.
	_, id, err := s.bindingInfo(ctx, "acme", "kv", "KV")
	if err != nil || id != "acme/__kv__/v2" {
		t.Fatalf("active binding = %q, %v", id, err)
	}
}
