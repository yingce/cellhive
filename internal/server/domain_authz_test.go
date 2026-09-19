package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/auth"
	"cellhive/internal/control"
)

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// signJWT mints an HS256 JWT for the admin plane (ADR-131 tests).
func signJWT(t *testing.T, secret []byte, claims map[string]any) string {
	t.Helper()
	head, _ := json.Marshal(map[string]any{"alg": "HS256", "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	signing := b64url(head) + "." + b64url(payload)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signing))
	return signing + "." + b64url(mac.Sum(nil))
}

// TestBuiltinDomainOnDeploy covers ADR-131: a deploy materializes
// <ns>-<worker>.<base> so the worker has an immediate entry point.
func TestBuiltinDomainOnDeploy(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	s.Cfg.BaseDomain = "cell.internal"
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	sha, err := putTestBundle(s, "builtin-domain")
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	if rr := mustAdmin(t, s.AdminHandler(), http.MethodPost, "/v1/control/deploy", `{"namespace":"acme","worker":"web","bundle_sha":"`+sha+`"}`); rr.Code != http.StatusOK {
		t.Fatalf("deploy = %d: %s", rr.Code, rr.Body.String())
	}
	// The built-in host resolves through the loader read path.
	rr := do(t, s.Handler(), http.MethodGet, "/v1/control/host?host=acme-web.cell.internal", "tok", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"worker":"web"`) {
		t.Fatalf("builtin host view = %d %s", rr.Code, rr.Body.String())
	}
	// The built-in host is registered (kind=builtin) with a whole-host route.
	h, err := s.Control.GetHost(ctx, "acme-web.cell.internal")
	if err != nil || h.Kind != "builtin" {
		t.Fatalf("builtin host row = %+v, %v", h, err)
	}
	proj, err := s.Control.Projection(ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	foundRoute := false
	for _, app := range proj.Apps {
		for _, rt := range app.Routes {
			if rt.Host == "acme-web.cell.internal" && rt.Path == "" && rt.Worker == "web" {
				foundRoute = true
			}
		}
	}
	if !foundRoute {
		t.Fatal("builtin route missing from the projection")
	}
}

// TestCustomDomainRegistration covers ADR-133: registering a custom domain is
// authorization — it routes immediately (no DNS challenge), and unregistered
// hosts stay unreachable.
func TestCustomDomainRegistration(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{BundleSHA: "sha1"}, "ops"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	// Unregistered host: unreachable with a distinct reason.
	rr := do(t, s.Handler(), http.MethodGet, "/v1/control/host?host=app.customer.com", "tok", nil)
	if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), "host_not_registered") {
		t.Fatalf("unregistered host view = %d %s", rr.Code, rr.Body.String())
	}
	// Register: routes immediately.
	rr = adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/domain", "admin-tok",
		`{"namespace":"acme","host":"app.customer.com"}`)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"kind":"custom"`) {
		t.Fatalf("domain add = %d %s", rr.Code, rr.Body.String())
	}
	if rr := mustAdmin(t, s.AdminHandler(), http.MethodPost, "/v1/control/route",
		`{"namespace":"acme","host":"app.customer.com","worker":"web"}`); rr.Code != http.StatusOK {
		t.Fatalf("route after register = %d %s", rr.Code, rr.Body.String())
	}
	if rr := do(t, s.Handler(), http.MethodGet, "/v1/control/host?host=app.customer.com", "tok", nil); rr.Code != http.StatusOK {
		t.Fatalf("registered host view = %d %s", rr.Code, rr.Body.String())
	}
	// Removing the domain stops routing.
	if rr := mustAdmin(t, s.AdminHandler(), http.MethodDelete, "/v1/control/domain?namespace=acme&host=app.customer.com", ""); rr.Code != http.StatusOK {
		t.Fatalf("domain rm = %d %s", rr.Code, rr.Body.String())
	}
	if rr := do(t, s.Handler(), http.MethodGet, "/v1/control/host?host=app.customer.com", "tok", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("host after rm = %d, want 404", rr.Code)
	}
}

// TestHostOwnershipConflict covers ADR-131/133: a registered host belongs to one
// namespace (409 for others), and the built-in domain space is not claimable.
func TestHostOwnershipConflict(t *testing.T) {
	s := newControlServer(t)
	s.Cfg.BaseDomain = "cell.internal"
	if rr := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/domain", "admin-tok",
		`{"namespace":"acme","host":"shared.example.com"}`); rr.Code != http.StatusOK {
		t.Fatalf("acme claim = %d %s", rr.Code, rr.Body.String())
	}
	if rr := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/domain", "admin-tok",
		`{"namespace":"other","host":"shared.example.com"}`); rr.Code != http.StatusConflict {
		t.Fatalf("second-ns claim = %d, want 409: %s", rr.Code, rr.Body.String())
	}
	// Built-in hosts are not user-writable.
	if rr := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/domain", "admin-tok",
		`{"namespace":"acme","host":"acme-web.cell.internal"}`); rr.Code == http.StatusOK {
		t.Fatalf("built-in host was user-writable: %s", rr.Body.String())
	}
}

// TestAdminNamespaceAuthorization covers ADR-131: JWT namespace grants scope the
// admin plane; list endpoints filter; platform-wide actions need "*".
func TestAdminNamespaceAuthorization(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	secret := []byte("test-secret-0123456789abcdef")
	s.AdminAuth = auth.Any{&auth.JWTBearer{HS256Secret: secret}}
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app acme: %v", err)
	}
	if _, err := s.Control.CreateApp(ctx, "other", "ops"); err != nil {
		t.Fatalf("app other: %v", err)
	}
	token := signJWT(t, secret, map[string]any{
		"sub": "alice@corp", "cellhive_ns": []string{"acme"}, "exp": time.Now().Add(time.Hour).Unix(),
	})
	deploy := func(ns string) *httptest.ResponseRecorder {
		return adminTokenDo(t, s, http.MethodPost, "/v1/control/app", token, `{"namespace":"`+ns+`"}`)
	}
	if rr := deploy("acme"); rr.Code != http.StatusOK {
		t.Fatalf("own ns = %d %s", rr.Code, rr.Body.String())
	}
	if rr := deploy("other"); rr.Code != http.StatusForbidden {
		t.Fatalf("foreign ns = %d, want 403: %s", rr.Code, rr.Body.String())
	}
	// Lists are filtered to the granted namespaces.
	rr := adminTokenDo(t, s, http.MethodGet, "/v1/control/apps", token, "")
	if rr.Code != http.StatusOK || strings.Contains(rr.Body.String(), "other") || !strings.Contains(rr.Body.String(), "acme") {
		t.Fatalf("apps list = %d %s", rr.Code, rr.Body.String())
	}
	// Platform-wide actions require "*".
	if rr := adminTokenDo(t, s, http.MethodPost, "/v1/control/gc/bundles", token, ""); rr.Code != http.StatusForbidden {
		t.Fatalf("gc with a scoped grant = %d, want 403", rr.Code)
	}
	wild := signJWT(t, secret, map[string]any{
		"sub": "ops@corp", "cellhive_ns": []string{"*"}, "exp": time.Now().Add(time.Hour).Unix(),
	})
	if rr := adminTokenDo(t, s, http.MethodPost, "/v1/control/gc/bundles", wild, ""); rr.Code != http.StatusOK {
		t.Fatalf("gc as platform = %d %s", rr.Code, rr.Body.String())
	}
	// Audit rows carry the JWT subject (ADR-131).
	rr = adminTokenDo(t, s, http.MethodGet, "/v1/control/audit?namespace=acme", token, "")
	if !strings.Contains(rr.Body.String(), "alice@corp") {
		t.Fatalf("audit missing the caller subject: %s", rr.Body.String())
	}
}

// TestRouteMountSemantics locks the control→loader mapping for mounts: a route
// carries its path prefix (the loader always strips it) and the host view
// exposes it unchanged (ADR-131).
func TestRouteMountSemantics(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "sha1"}, "ops"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	mustHost(t, s, "acme", "api.test")
	if err := s.Control.PutRoute(ctx, "acme", control.Route{
		Host: "api.test", Path: "/api", Worker: "api",
	}, "ops"); err != nil {
		t.Fatalf("route: %v", err)
	}
	rr := do(t, s.Handler(), http.MethodGet, "/v1/control/host?host=api.test", "tok", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"path":"/api"`) {
		t.Fatalf("host view = %d %s, want the mounted path", rr.Code, rr.Body.String())
	}
	proj, err := s.Control.Projection(ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	found := false
	for _, app := range proj.Apps {
		for _, rt := range app.Routes {
			if rt.Path == "/api" && rt.Worker == "api" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("projection lost the route")
	}
}

// TestSoftDeleteApp covers ADR-131: deleting an app stops routing at once and
// leaves a purge job that removes the metadata.
func TestSoftDeleteApp(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	s.Cfg.BaseDomain = "cell.internal"
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	sha, err := putTestBundle(s, "soft-delete")
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	if rr := mustAdmin(t, s.AdminHandler(), http.MethodPost, "/v1/control/deploy",
		`{"namespace":"acme","worker":"web","bundle_sha":"`+sha+`"}`); rr.Code != http.StatusOK {
		t.Fatalf("deploy = %d %s", rr.Code, rr.Body.String())
	}
	if rr := mustAdmin(t, s.AdminHandler(), http.MethodDelete, "/v1/control/app?namespace=acme", ""); rr.Code != http.StatusOK {
		t.Fatalf("app delete = %d %s", rr.Code, rr.Body.String())
	}
	// Routing stopped immediately and the worker is hidden.
	if rr := do(t, s.Handler(), http.MethodGet, "/v1/control/host?host=acme-web.cell.internal", "tok", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("host after delete = %d, want 404", rr.Code)
	}
	jobs, err := s.Control.PendingPurges(ctx, 10)
	if err != nil || len(jobs) != 1 || jobs[0].Worker != "" {
		t.Fatalf("purge jobs = %+v, %v; want one namespace job", jobs, err)
	}
	if err := s.Control.PurgeAppRows(ctx, "acme"); err != nil {
		t.Fatalf("purge rows: %v", err)
	}
	if err := s.Control.FinishPurge(ctx, "acme", ""); err != nil {
		t.Fatalf("finish purge: %v", err)
	}
	if _, err := s.Control.GetApp(ctx, "acme"); err == nil {
		t.Fatal("app survived the purge")
	}
}

// --- test helpers -----------------------------------------------------------

// adminTokenDo calls the admin listener with a bearer credential (ADR-131).
func adminTokenDo(t *testing.T, s *Server, method, path, bearer, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("Authorization", "Bearer "+bearer)
	if body != "" {
		req.Header.Set("content-type", "application/json")
	}
	rr := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rr, req)
	return rr
}

// putTestBundle stores a small bundle and returns its sha.
func putTestBundle(s *Server, content string) (string, error) {
	st := s.artifactStore()
	sha, _, err := st.PutBundle(context.Background(), []byte(content))
	return sha, err
}
