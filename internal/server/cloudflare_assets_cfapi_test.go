package server

import (
	"bytes"
	"cellhive/internal/auth"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func cfUploadMetadata(t *testing.T, s *Server, metadata string, includeModule bool) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	p, _ := mw.CreateFormField("metadata")
	_, _ = p.Write([]byte(metadata))
	if includeModule {
		m, _ := mw.CreateFormFile("worker.js", "worker.js")
		_, _ = m.Write([]byte("export default { fetch() { return new Response('cf') } }"))
	}
	_ = mw.Close()
	r := httptest.NewRequest(http.MethodPut, "/client/v4/accounts/acme/workers/scripts/api", &body)
	r.Header.Set("Authorization", "Bearer admin-tok")
	r.Header.Set("Content-Type", mw.FormDataContentType())
	rr := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rr, r)
	return rr
}

func seedAssets(t *testing.T, s *Server, ns, worker, token string) {
	t.Helper()
	st := s.artifactStore()
	if st == nil {
		t.Fatal("artifact store unavailable")
	}
	manifest := []byte(`{"index.html":{"hash":"h1","size":5}}`)
	if _, err := st.PutAssetAt(t.Context(), ns, worker, token, wranglerManifestPath, manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutAssetAt(t.Context(), ns, worker, token, "index.html", []byte("hello")); err != nil {
		t.Fatal(err)
	}
}

func TestCloudflareAssetsBindingDeploys(t *testing.T) {
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	const token = "asset-token"
	seedAssets(t, s, "acme", "api", token)
	jwt := s.signAssetsClaim(wranglerAssetsClaim{NS: "acme", Worker: "api", Token: token, Exp: time.Now().Add(time.Hour).Unix()})
	meta := map[string]any{
		"main_module": "worker.js", "compatibility_date": "2026-09-23",
		"bindings": []any{map[string]any{"type": "assets", "name": "ASSETS"}},
		"assets": map[string]any{"jwt": jwt, "config": map[string]any{
			"binding": "ASSETS", "not_found_handling": "single-page-application", "run_worker_first": []string{"/api/*"},
		}},
	}
	raw, _ := json.Marshal(meta)
	rr := cfUploadMetadata(t, s, string(raw), true)
	if rr.Code != http.StatusOK {
		t.Fatalf("deploy = %d %s", rr.Code, rr.Body.String())
	}
	view, ok, err := s.Control.WorkerEnv(t.Context(), "acme", "api")
	if err != nil || !ok {
		t.Fatalf("worker env: %v %v", ok, err)
	}
	if view.AssetsSHA != token || view.Assets == nil || view.Assets.Binding != "ASSETS" ||
		len(view.Assets.RunWorkerFirstPaths) != 1 || view.Assets.RunWorkerFirstPaths[0] != "/api/*" {
		t.Fatalf("assets = %+v sha=%q", view.Assets, view.AssetsSHA)
	}
	found := false
	for _, b := range view.Bindings {
		if b.Type == "assets" && b.Name == "ASSETS" && b.ID == token {
			found = true
		}
	}
	if !found {
		t.Fatalf("assets binding was not injected with the token: %+v", view.Bindings)
	}
}

func TestCloudflareAssetsOnlyDeploys(t *testing.T) {
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	const token = "asset-only"
	seedAssets(t, s, "acme", "api", token)
	jwt := s.signAssetsClaim(wranglerAssetsClaim{NS: "acme", Worker: "api", Token: token, Exp: time.Now().Add(time.Hour).Unix()})
	meta := map[string]any{
		"compatibility_date": "2026-09-23",
		"assets": map[string]any{"jwt": jwt, "config": map[string]any{
			"not_found_handling": "single-page-application",
		}},
	}
	raw, _ := json.Marshal(meta)
	rr := cfUploadMetadata(t, s, string(raw), false)
	if rr.Code != http.StatusOK {
		t.Fatalf("assets-only deploy = %d %s", rr.Code, rr.Body.String())
	}
	view, ok, err := s.Control.WorkerEnv(t.Context(), "acme", "api")
	if err != nil || !ok || view.Assets == nil || !view.Assets.AssetsOnly || view.AssetsSHA != token {
		t.Fatalf("assets-only view = %+v %v %v", view, ok, err)
	}
}

func TestCloudflareAssetsBindingRejectsMismatch(t *testing.T) {
	s := newControlServer(t)
	meta := map[string]any{
		"main_module": "worker.js", "compatibility_date": "2026-09-23",
		"bindings": []any{map[string]any{"type": "assets", "name": "OTHER"}},
		"assets":   map[string]any{"jwt": "x", "config": map[string]any{"binding": "ASSETS"}},
	}
	raw, _ := json.Marshal(meta)
	rr := cfUploadMetadata(t, s, string(raw), true)
	if rr.Code != http.StatusBadRequest || !bytes.Contains(rr.Body.Bytes(), []byte(`"invalid_binding"`)) || !bytes.Contains(rr.Body.Bytes(), []byte("OTHER")) || !bytes.Contains(rr.Body.Bytes(), []byte("ASSETS")) {
		t.Fatalf("mismatch = %d %s", rr.Code, rr.Body.String())
	}
}

func TestCloudflareUnsupportedBindingCode(t *testing.T) {
	s := newControlServer(t)
	meta := `{"main_module":"worker.js","compatibility_date":"2026-09-23","bindings":[{"type":"foo","name":"X"}]}`
	rr := cfUploadMetadata(t, s, meta, true)
	if rr.Code != http.StatusBadRequest || !bytes.Contains(rr.Body.Bytes(), []byte(`"unsupported_binding"`)) || !bytes.Contains(rr.Body.Bytes(), []byte("foo")) {
		t.Fatalf("unsupported binding = %d %s", rr.Code, rr.Body.String())
	}
}

func TestCloudflareJWTNamespaceAuthorization(t *testing.T) {
	secret := []byte("test-secret")
	s := newControlServer(t)
	s.AdminAuth = auth.Any{&auth.JWTBearer{HS256Secret: secret}}
	if _, err := s.Control.CreateApp(t.Context(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateApp(t.Context(), "other", "test"); err != nil {
		t.Fatal(err)
	}
	token := signJWT(t, secret, map[string]any{
		"sub": "tenant@corp", "cellhive_ns": []string{"acme"}, "exp": time.Now().Add(time.Hour).Unix(),
	})
	do := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		rr := httptest.NewRecorder()
		s.AdminHandler().ServeHTTP(rr, r)
		return rr
	}
	if rr := do("/client/v4/accounts/acme/workers/scripts/api/settings"); rr.Code == http.StatusForbidden || rr.Code == http.StatusUnauthorized {
		t.Fatalf("allowed namespace was rejected: %d %s", rr.Code, rr.Body.String())
	}
	if rr := do("/client/v4/accounts/other/workers/scripts/api/settings"); rr.Code != http.StatusForbidden {
		t.Fatalf("other namespace = %d %s", rr.Code, rr.Body.String())
	}
	for _, path := range []string{"/client/v4/accounts/acme/storage/kv/namespaces", "/client/v4/accounts/acme/d1/database", "/client/v4/accounts/acme/r2/buckets"} {
		if rr := do(path); rr.Code == http.StatusForbidden || rr.Code == http.StatusUnauthorized {
			t.Fatalf("resource path %s rejected for allowed namespace: %d %s", path, rr.Code, rr.Body.String())
		}
	}
	for _, path := range []string{"/client/v4/accounts/other/storage/kv/namespaces", "/client/v4/accounts/other/d1/database", "/client/v4/accounts/other/r2/buckets"} {
		if rr := do(path); rr.Code != http.StatusForbidden {
			t.Fatalf("resource path %s allowed for other namespace: %d %s", path, rr.Code, rr.Body.String())
		}
	}
	accounts := do("/client/v4/accounts")
	if accounts.Code != http.StatusOK || strings.Contains(accounts.Body.String(), `"id":"other"`) || !strings.Contains(accounts.Body.String(), `"id":"acme"`) {
		t.Fatalf("account list leaked namespaces: %d %s", accounts.Code, accounts.Body.String())
	}
	expired := signJWT(t, secret, map[string]any{
		"sub": "tenant@corp", "cellhive_ns": []string{"acme"}, "exp": time.Now().Add(-time.Minute).Unix(),
	})
	r := httptest.NewRequest(http.MethodGet, "/client/v4/accounts/acme/workers/scripts/api/settings", nil)
	r.Header.Set("Authorization", "Bearer "+expired)
	rr := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rr, r)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expired JWT = %d %s", rr.Code, rr.Body.String())
	}
}

func TestCFAPIHandlerExcludesControlPlane(t *testing.T) {
	s := newControlServer(t)
	h := s.CFAPIHandler()
	r := httptest.NewRequest(http.MethodGet, "/v1/control/status", nil)
	r.Header.Set("Authorization", "Bearer admin-tok")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("CF-API exposed a control route: %d %s", rr.Code, rr.Body.String())
	}
}

func TestDecodeAssetsBindingAdoptsWranglerName(t *testing.T) {
	var out cloudflareWorkerMetadata
	meta := `{"main_module":"worker.js","bindings":[{"type":"assets","name":"ASSETS"}],"assets":{"jwt":"x","config":{"not_found_handling":"single-page-application"}}}`
	if err := decodeCloudflareMetadata([]byte(meta), &out); err != nil {
		t.Fatal(err)
	}
	if out.Assets == nil || out.Assets.Binding != "ASSETS" {
		t.Fatalf("assets = %+v", out.Assets)
	}
	if len(out.Bindings) != 0 {
		t.Fatalf("assets binding should be injected by control, got %+v", out.Bindings)
	}
}
