package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"cellhive/internal/artifacts"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/owner"
	"cellhive/internal/queue"
	"cellhive/internal/scopedtoken"
	"cellhive/internal/workerbudget"
	"cellhive/internal/workflow"
)

func newControlServer(t *testing.T) *Server {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	env, err := control.NewEnvelope(bytes.Repeat([]byte{0x9}, 32))
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	return New(Deps{
		Cfg: config.Config{
			NodeID: "node-1", TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok", AdminToken: "admin-tok",
		},
		Bucket:    b,
		Control:   control.New(cs, env),
		Workflows: workflow.New(cs),
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

// mustHost registers and verifies a custom host before routes are written
// (ADR-131).
func mustHost(t *testing.T, s *Server, ns, host string) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Control.PutCustomHost(ctx, ns, host, "ops"); err != nil {
		t.Fatalf("put host %s: %v", host, err)
	}
}

func adminDo(t *testing.T, h http.Handler, method, target, token string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if token != "" {
		req.Header.Set("x-cellhive-admin-token", token)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestControlAdminAuthAndDataPlaneIsolation(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()

	// No admin token -> 401.
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/app", "", `{"namespace":"acme"}`); rr.Code != http.StatusUnauthorized {
		t.Fatalf("no token = %d, want 401", rr.Code)
	}
	// Wrong token -> 401.
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/app", "nope", `{"namespace":"acme"}`); rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token = %d, want 401", rr.Code)
	}
	// Correct token -> 200.
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/app", "admin-tok", `{"namespace":"acme"}`); rr.Code != http.StatusOK {
		t.Fatalf("create app = %d: %s", rr.Code, rr.Body.String())
	}

	// Control writes are also served on the internal listener so a non-owner node
	// can forward them to the owner (ADR-118) — but only with the internal token;
	// tenant/foreign tokens are rejected.
	if rr := do(t, s.Handler(), http.MethodPost, "/v1/control/app", "", []byte(`{"namespace":"evil"}`)); rr.Code != http.StatusUnauthorized {
		t.Fatalf("control write without internal token = %d, want 401", rr.Code)
	}
	if rr := do(t, s.Handler(), http.MethodPost, "/v1/control/app", "tenant-scope-token", []byte(`{"namespace":"evil"}`)); rr.Code != http.StatusUnauthorized {
		t.Fatalf("control write with a foreign token = %d, want 401", rr.Code)
	}
	if rr := do(t, s.Handler(), http.MethodPost, "/v1/control/app", "tok", []byte(`{"namespace":"acme2"}`)); rr.Code != http.StatusOK {
		t.Fatalf("control write with internal token = %d: %s", rr.Code, rr.Body.String())
	}

	// The routing projection is readable on the internal mux.
	rr := do(t, s.Handler(), http.MethodGet, "/v1/control/routes", "tok", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("routes = %d: %s", rr.Code, rr.Body.String())
	}
	etag := rr.Header().Get("etag")
	if etag == "" {
		t.Fatalf("missing etag")
	}
	var proj control.Projection
	hasAcme := false
	if err := json.Unmarshal(rr.Body.Bytes(), &proj); err != nil {
		t.Fatalf("projection: %v", err)
	}
	for _, app := range proj.Apps {
		if app.Namespace == "acme" {
			hasAcme = true
		}
	}
	if !hasAcme {
		t.Fatalf("projection = %+v, want acme", proj)
	}
	// Conditional pull returns 304.
	req := httptest.NewRequest(http.MethodGet, "/v1/control/routes", nil)
	req.Header.Set("x-cellhive-internal-token", "tok")
	req.Header.Set("if-none-match", etag)
	rr2 := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr2, req)
	if rr2.Code != http.StatusNotModified {
		t.Fatalf("if-none-match = %d, want 304", rr2.Code)
	}
}

func TestControlAdminDeployAndSecretRoundTrip(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/resource", `{"namespace":"acme","kind":"kv","name":"KV","scope":"acme/__kv__/main"}`)
	bundle := mustAdmin(t, admin, http.MethodPost, "/v1/control/bundle", "export default {}")
	var bo struct {
		Sha string `json:"sha"`
	}
	_ = json.Unmarshal(bundle.Body.Bytes(), &bo)
	rr := mustAdmin(t, admin, http.MethodPost, "/v1/control/deploy", `{"namespace":"acme","worker":"api","bundle_sha":"`+bo.Sha+`","bindings":[{"type":"kv","name":"KV","id":"acme/__kv__/main"}]}`)
	var v control.Version
	if err := json.Unmarshal(rr.Body.Bytes(), &v); err != nil || v.Number != 1 {
		t.Fatalf("deploy = %+v, %v", v, err)
	}
	mustHost(t, s, "acme", "api.example.com")
	mustAdmin(t, admin, http.MethodPost, "/v1/control/route", `{"namespace":"acme","host":"api.example.com","worker":"api"}`)
	// Secret round trip.
	mustAdmin(t, admin, http.MethodPost, "/v1/control/secret", `{"namespace":"acme","worker":"api","key":"TOKEN","value":"czNjcjN0"}`)
	rr = mustAdmin(t, admin, http.MethodGet, "/v1/control/secret?namespace=acme&worker=api&key=TOKEN", "")
	var got struct {
		Value string `json:"value"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &got)
	if got.Value != "czNjcjN0" {
		t.Fatalf("secret value = %q", got.Value)
	}
}

func TestDeployRejectsFinalWorkerCodeOverBudget(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)

	// The tenant module alone is exactly the upstream WorkerCode limit. Module
	// names and trusted wrapper/facade sources necessarily make the final object
	// larger, even though the uploaded object itself remains a legal artifact.
	sha, _, err := s.artifactStore().PutBundle(context.Background(), bytes.Repeat([]byte{'x'}, int(workerbudget.CodeMaxBytes)))
	if err != nil {
		t.Fatal(err)
	}
	rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok",
		`{"namespace":"acme","worker":"api","bundle_sha":"`+sha+`"}`)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"error":"worker_code_too_large"`) ||
		!strings.Contains(rr.Body.String(), `"actual_bytes":`) ||
		!strings.Contains(rr.Body.String(), `"max_bytes":67108864`) {
		t.Fatalf("response lacks bounded code budget details: %s", rr.Body.String())
	}
	if releases, err := s.Control.Releases(context.Background(), "acme", "api"); err != nil || len(releases) != 0 {
		t.Fatalf("rejected deploy releases = %+v, err=%v", releases, err)
	}
}

func TestDeployRejectsWorkerEnvOverBudget(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	bundle := mustAdmin(t, admin, http.MethodPost, "/v1/control/bundle", "export default {}")
	var uploaded struct {
		Sha string `json:"sha"`
	}
	if err := json.Unmarshal(bundle.Body.Bytes(), &uploaded); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{
		"namespace":  "acme",
		"worker":     "api",
		"bundle_sha": uploaded.Sha,
		"vars":       map[string]string{"large": strings.Repeat("x", int(workerbudget.EnvMaxBytes))},
	})
	if err != nil {
		t.Fatal(err)
	}
	rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", string(body))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"error":"worker_env_too_large"`) ||
		!strings.Contains(rr.Body.String(), `"actual_bytes":`) ||
		!strings.Contains(rr.Body.String(), `"max_bytes":1040384`) {
		t.Fatalf("response lacks bounded env budget details: %s", rr.Body.String())
	}
	if releases, err := s.Control.Releases(context.Background(), "acme", "api"); err != nil || len(releases) != 0 {
		t.Fatalf("rejected deploy releases = %+v, err=%v", releases, err)
	}
}

func TestDeployBudgetExactBoundaries(t *testing.T) {
	t.Run("code exact limit accepted", func(t *testing.T) {
		s := newControlServer(t)
		admin := s.AdminHandler()
		mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
		// Empty binding metadata renders to 79 bytes; the fixed runtime reserve is
		// 64 KiB. This literal is independently derived from that public contract.
		const bundleBytes = 67108864 - 65536 - 79
		sha, _, err := s.artifactStore().PutBundle(context.Background(), bytes.Repeat([]byte{'x'}, bundleBytes))
		if err != nil {
			t.Fatal(err)
		}
		rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok",
			`{"namespace":"acme","worker":"api","bundle_sha":"`+sha+`"}`)
		if rr.Code != http.StatusOK {
			t.Fatalf("exact code limit = %d: %.500s", rr.Code, rr.Body.String())
		}
	})

	t.Run("code limit plus one rejected in dry run", func(t *testing.T) {
		s := newControlServer(t)
		admin := s.AdminHandler()
		mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
		const bundleBytes = 67108864 - 65536 - 79 + 1
		sha, _, err := s.artifactStore().PutBundle(context.Background(), bytes.Repeat([]byte{'x'}, bundleBytes))
		if err != nil {
			t.Fatal(err)
		}
		rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok",
			`{"namespace":"acme","worker":"api","bundle_sha":"`+sha+`","dry_run":true}`)
		if rr.Code != http.StatusRequestEntityTooLarge || !strings.Contains(rr.Body.String(), `"actual_bytes":67108865`) {
			t.Fatalf("code limit+1 = %d: %s", rr.Code, rr.Body.String())
		}
		if releases, err := s.Control.Releases(context.Background(), "acme", "api"); err != nil || len(releases) != 0 {
			t.Fatalf("rejected dry-run releases = %+v, err=%v", releases, err)
		}
	})

	t.Run("env exact limit accepted", func(t *testing.T) {
		s := newControlServer(t)
		admin := s.AdminHandler()
		mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
		bundle := mustAdmin(t, admin, http.MethodPost, "/v1/control/bundle", "export default {}")
		var uploaded struct {
			Sha string `json:"sha"`
		}
		if err := json.Unmarshal(bundle.Body.Bytes(), &uploaded); err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(map[string]any{
			"namespace": "acme", "worker": "api", "bundle_sha": uploaded.Sha,
			// {"v":"..."} contributes eight JSON bytes outside the ASCII value.
			"vars": map[string]string{"v": strings.Repeat("x", int(workerbudget.EnvMaxBytes)-8)},
		})
		if err != nil {
			t.Fatal(err)
		}
		rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", string(body))
		if rr.Code != http.StatusOK {
			t.Fatalf("exact env limit = %d: %.500s", rr.Code, rr.Body.String())
		}
	})
}

func TestControlFixedRuntimeBudgetCoversInjectedSources(t *testing.T) {
	paths := []string{
		"../../workerd/user-runtime/queue-wrapper.js",
		"../../workerd/user-runtime/workflow-wrapper.js",
		"../../workerd/user-runtime/cellhive-workflow.js",
		"../../workerd/platform/facades.js",
		"../../workerd/platform/rpc-codec.js",
		"../../workerd/platform/bindings-wrapper.js",
		"../../workerd/do-runtime/cellhive-do.js",
	}
	var total int64
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}
	if total > controlFixedRuntimeCodeBytes {
		t.Fatalf("injected sources total %d bytes, exceeding fixed control reserve %d", total, controlFixedRuntimeCodeBytes)
	}
}

func TestSecretFormerPlatformNameAllowed(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	value := []byte("tenant-owned platform name")
	encoded := base64.StdEncoding.EncodeToString(value)

	put := adminDo(t, admin, http.MethodPost, "/v1/control/secret", "admin-tok",
		`{"namespace":"acme","worker":"api","key":"CH_PLATFORM","value":"`+encoded+`"}`)
	if put.Code != http.StatusOK {
		t.Fatalf("put secret = %d: %s", put.Code, put.Body.String())
	}

	get := adminDo(t, admin, http.MethodGet, "/v1/control/secret?namespace=acme&worker=api&key=CH_PLATFORM", "admin-tok", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get secret = %d: %s", get.Code, get.Body.String())
	}
	var got struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(get.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode secret response: %v", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(got.Value)
	if err != nil {
		t.Fatalf("decode secret value: %v", err)
	}
	if string(decoded) != string(value) {
		t.Fatalf("secret value = %q, want %q", decoded, value)
	}
}

// TestControlDeployInterception covers the server-side compatibility gate
// (ADR-065): a permissive local dev runtime must not smuggle a deploy the
// platform cannot run.
func TestControlDeployInterception(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/resource", `{"namespace":"acme","kind":"kv","name":"KV","scope":"acme/__kv__/main"}`)
	bundle := mustAdmin(t, admin, http.MethodPost, "/v1/control/bundle", "export default {}")
	var bo struct {
		Sha string `json:"sha"`
	}
	_ = json.Unmarshal(bundle.Body.Bytes(), &bo)
	sha := bo.Sha

	cases := []struct {
		name   string
		body   string
		code   string
		status int
	}{
		{
			name:   "unsupported images binding",
			body:   `{"namespace":"acme","worker":"api","bundle_sha":"` + sha + `","bindings":[{"type":"images","name":"IMAGES"}]}`,
			code:   "unsupported_binding",
			status: http.StatusBadRequest,
		},
		{
			name:   "compat date too new",
			body:   `{"namespace":"acme","worker":"api","bundle_sha":"` + sha + `","compatibility_date":"2026-10-01"}`,
			code:   "compat_date_too_new",
			status: http.StatusBadRequest,
		},
		{
			name:   "unknown flag",
			body:   `{"namespace":"acme","worker":"api","bundle_sha":"` + sha + `","compatibility_flags":["nope"]}`,
			code:   "unknown_flag",
			status: http.StatusBadRequest,
		},
		{
			name:   "unregistered binding",
			body:   `{"namespace":"acme","worker":"api","bundle_sha":"` + sha + `","bindings":[{"type":"kv","name":"NOPE","id":"x"}]}`,
			code:   "binding_unregistered",
			status: http.StatusBadRequest,
		},
		{
			name:   "missing bundle",
			body:   `{"namespace":"acme","worker":"api","bundle_sha":"deadbeef"}`,
			code:   "missing_bundle",
			status: http.StatusBadRequest,
		},
		{
			name:   "invalid cron rejected",
			body:   `{"namespace":"acme","worker":"api","bundle_sha":"` + sha + `","crons":["not a cron"]}`,
			code:   "invalid_cron",
			status: http.StatusBadRequest,
		},
		{
			name:   "cross-worker transfer rejected",
			body:   `{"namespace":"acme","worker":"api","bundle_sha":"` + sha + `","migrations":[{"tag":"v2","transferred_classes":[{"from":"A","to":"W2","script_name":"other"}]}]}`,
			code:   "invalid_migration",
			status: http.StatusBadRequest,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", tc.body)
			if rr.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", rr.Code, tc.status, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), tc.code) {
				t.Fatalf("body %s missing code %q", rr.Body.String(), tc.code)
			}
		})
	}

	// A valid deploy still succeeds.
	ok := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok",
		`{"namespace":"acme","worker":"api","bundle_sha":"`+sha+`","compatibility_date":"2026-06-15","compatibility_flags":["nodejs_compat"],"bindings":[{"type":"kv","name":"KV","id":"acme/__kv__/main"}]}`)
	if ok.Code != http.StatusOK {
		t.Fatalf("valid deploy = %d: %s", ok.Code, ok.Body.String())
	}
}

func mustAdmin(t *testing.T, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	rr := adminDo(t, h, method, target, "admin-tok", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("%s %s = %d: %s", method, target, rr.Code, rr.Body.String())
	}
	return rr
}

func TestBundleAssetUploadAndRead(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()

	body := []byte("export default {}")
	rr := adminDo(t, admin, http.MethodPost, "/v1/control/bundle", "admin-tok", string(body))
	if rr.Code != http.StatusOK {
		t.Fatalf("bundle put = %d: %s", rr.Code, rr.Body.String())
	}
	var out struct {
		Sha  string `json:"sha"`
		Size int    `json:"size"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if out.Sha != artifacts.Hash(body) || out.Size != len(body) {
		t.Fatalf("bundle out = %+v", out)
	}
	// Read back through the internal listener.
	got := do(t, s.Handler(), http.MethodGet, "/v1/internal/bundle?sha="+out.Sha, "tok", nil)
	if got.Code != http.StatusOK || got.Body.String() != string(body) {
		t.Fatalf("bundle get = %d %q", got.Code, got.Body.String())
	}

	// Asset upload + read.
	ar := adminDo(t, admin, http.MethodPost, "/v1/control/asset?ns=acme&worker=api&path=index.html", "admin-tok", "<html>")
	if ar.Code != http.StatusOK {
		t.Fatalf("asset put = %d: %s", ar.Code, ar.Body.String())
	}
	var aout struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(ar.Body.Bytes(), &aout)
	ag := do(t, s.Handler(), http.MethodGet, "/v1/internal/asset?ns=acme&worker=api&token="+aout.Token+"&path=index.html", "tok", nil)
	if ag.Code != http.StatusOK || ag.Body.String() != "<html>" {
		t.Fatalf("asset get = %d %q", ag.Code, ag.Body.String())
	}
	// Optional asset metadata files (_redirects/_headers) are probed before the
	// requested asset. A missing object must be a normal 404, not a presign 500.
	missingURL := do(t, s.Handler(), http.MethodGet, "/v1/internal/asset-url?ns=acme&worker=api&token="+aout.Token+"&path=_redirects", "tok", nil)
	if missingURL.Code != http.StatusNotFound {
		t.Fatalf("missing asset URL = %d: %s; want 404", missingURL.Code, missingURL.Body.String())
	}
	// Manual path traversal is rejected.
	bad := adminDo(t, admin, http.MethodPost, "/v1/control/asset?ns=acme&worker=api&path=../x", "admin-tok", "x")
	if bad.Code == http.StatusOK {
		t.Fatalf("unsafe asset path accepted")
	}
}

// presignWithoutLookup models cloud signing, which does not check that a key exists.
type presignWithoutLookup struct {
	bucket.Bucket
	bucket.Statter
}

func (presignWithoutLookup) PresignGet(_ context.Context, key string, _ time.Duration) (string, error) {
	return "https://example.invalid/" + key, nil
}

func TestAssetURLRejectsUnsafePath(t *testing.T) {
	s := newControlServer(t)
	rr := do(t, s.Handler(), http.MethodGet, "/v1/internal/asset-url?ns=acme&worker=api&token=version&path=../other", "tok", nil)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unsafe asset URL = %d: %s; want 400", rr.Code, rr.Body.String())
	}
}

func TestAssetURLDoesNotSignMissingCloudObject(t *testing.T) {
	s := newControlServer(t)
	s.Bucket = presignWithoutLookup{Bucket: s.Bucket, Statter: s.Bucket.(bucket.Statter)}
	put := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/asset?ns=acme&worker=api&path=index.html", "admin-tok", "<html>")
	if put.Code != http.StatusOK {
		t.Fatalf("asset upload = %d: %s", put.Code, put.Body.String())
	}
	var asset struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(put.Body.Bytes(), &asset); err != nil {
		t.Fatal(err)
	}
	url := "/v1/internal/asset-url?ns=acme&worker=api&path=index.html&token="
	if rr := do(t, s.Handler(), http.MethodGet, url+asset.Token, "tok", nil); rr.Code != http.StatusOK {
		t.Fatalf("existing asset URL = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := do(t, s.Handler(), http.MethodGet, url+"unknown", "tok", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("missing asset URL = %d: %s; want 404", rr.Code, rr.Body.String())
	}
}

func TestControlDeployAssetsAndConsumersProjected(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/resource", `{"namespace":"acme","kind":"kv","name":"KV","scope":"acme/__kv__/main"}`)
	bundle := mustAdmin(t, admin, http.MethodPost, "/v1/control/bundle", "export default {}")
	var bo struct {
		Sha string `json:"sha"`
	}
	_ = json.Unmarshal(bundle.Body.Bytes(), &bo)

	body := `{"namespace":"acme","worker":"web","bundle_sha":"` + bo.Sha + `",
	  "consumers":[{"queue":"jobs","max_retries":5,"dead_letter_queue":"jobs-dlq","max_batch_size":4}],
	  "assets":{"not_found_handling":"single-page-application","run_worker_first":true,"run_worker_first_paths":["/api"]}}`
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", body); rr.Code != http.StatusOK {
		t.Fatalf("deploy = %d: %s", rr.Code, rr.Body.String())
	}

	rr := do(t, s.Handler(), http.MethodGet, "/v1/control/routes", "tok", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("routes = %d", rr.Code)
	}
	var proj control.Projection
	if err := json.Unmarshal(rr.Body.Bytes(), &proj); err != nil {
		t.Fatalf("decode projection: %v", err)
	}
	if len(proj.Apps) != 1 || len(proj.Apps[0].Workers) != 1 {
		t.Fatalf("projection = %+v", proj)
	}
	v := proj.Apps[0].Workers[0].Version
	if v.Assets == nil || v.Assets.NotFoundHandling != "single-page-application" || !v.Assets.RunWorkerFirst ||
		len(v.Assets.RunWorkerFirstPaths) != 1 || v.Assets.RunWorkerFirstPaths[0] != "/api" {
		t.Fatalf("assets config = %+v", v.Assets)
	}
	if len(v.Consumers) != 1 {
		t.Fatalf("consumers = %+v", v.Consumers)
	}
	c := v.Consumers[0]
	if c.Queue != "jobs" || c.MaxRetries != 5 || c.DeadLetterQueue != "jobs-dlq" || c.MaxBatchSize != 4 {
		t.Fatalf("consumer = %+v", c)
	}
	targets := proj.QueueTargets()
	if len(targets) != 1 || targets[0].Consumer.DeadLetterQueue != "jobs-dlq" {
		t.Fatalf("queue targets = %+v", targets)
	}
}

// TestRoleTokenIsolation verifies Tier-1 role separation (ADR-075): a peer token
// cannot call internal endpoints and vice versa; each role's own token works.
func TestRoleTokenIsolation(t *testing.T) {
	s := newControlServer(t)
	s.Cfg.TokenPeer = "peer-tok"
	s.Cfg.TokenInternal = "internal-tok"
	s.Cfg.TokenDispatch = "dispatch-tok"
	h := s.Handler()

	// Correct role -> routed (200 for the cheap internal route).
	if rr := do(t, h, http.MethodGet, "/v1/control/routes", "internal-tok", nil); rr.Code != http.StatusOK {
		t.Fatalf("internal token on internal route = %d, want 200", rr.Code)
	}
	// Wrong role: peer token on an internal route -> 401.
	if rr := do(t, h, http.MethodGet, "/v1/control/routes", "peer-tok", nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("peer token on internal route = %d, want 401", rr.Code)
	}
	// Wrong role: internal token on a peer route -> 401.
	if rr := do(t, h, http.MethodGet, "/v1/peer/held?followers=", "internal-tok", nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("internal token on peer route = %d, want 401", rr.Code)
	}
	// Peer token on a peer route -> not 401 (routed; body may be rejected).
	if rr := do(t, h, http.MethodGet, "/v1/peer/held?followers=", "peer-tok", nil); rr.Code == http.StatusUnauthorized {
		t.Fatalf("peer token on peer route = 401, want routed")
	}
}

// TestRoleTokensRequired verifies fail-closed behavior: with role tokens unset,
// platform endpoints reject every call (no fallback).
func TestRoleTokensRequired(t *testing.T) {
	s := newControlServer(t)
	s.Cfg.TokenInternal = ""
	s.Cfg.TokenPeer = ""
	s.Cfg.TokenDispatch = ""
	h := s.Handler()
	if rr := do(t, h, http.MethodGet, "/v1/control/routes", "", nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("empty internal token = %d, want 401", rr.Code)
	}
	if rr := do(t, h, http.MethodGet, "/v1/control/routes", "anything", nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong internal token = %d, want 401", rr.Code)
	}
}

// TestAssetVersionTokenRoundTrip covers the --assets-dir contract: every file of
// a version is stored under one token (version.assets_sha), which the loader uses
// to read each path.
func TestAssetVersionTokenRoundTrip(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	data := s.Handler()
	const token = "v1token"

	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/asset?ns=acme&worker=web&path=index.html&token="+token, "admin-tok", "<h1>hi</h1>"); rr.Code != http.StatusOK {
		t.Fatalf("asset put = %d %s", rr.Code, rr.Body.String())
	}
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/asset?ns=acme&worker=web&path=sub/a.css&token="+token, "admin-tok", "body{}"); rr.Code != http.StatusOK {
		t.Fatalf("asset put 2 = %d %s", rr.Code, rr.Body.String())
	}
	// Both files are readable under the shared token.
	for path, want := range map[string]string{"index.html": "<h1>hi</h1>", "sub/a.css": "body{}"} {
		rr := do(t, data, http.MethodGet, "/v1/internal/asset?ns=acme&worker=web&token="+token+"&path="+path, "tok", nil)
		if rr.Code != http.StatusOK || rr.Body.String() != want {
			t.Fatalf("asset %s = %d %q (want %q)", path, rr.Code, rr.Body.String(), want)
		}
	}
	// A wrong token is not found.
	if rr := do(t, data, http.MethodGet, "/v1/internal/asset?ns=acme&worker=web&token=other&path=index.html", "tok", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("wrong token = %d, want 404", rr.Code)
	}
}

// TestWorkerDeletePurgesAssets covers worker delete cleanup: assets under
// assets/<ns>/<worker>/ are deleted and cease to resolve.
func TestWorkerDeletePurgesAssets(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	data := s.Handler()
	if _, err := s.Control.CreateApp(context.Background(), "acme", "ops"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Control.Deploy(context.Background(), "acme", "web", control.DeploySpec{BundleSHA: "sha1"}, "ops"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/asset?ns=acme&worker=web&path=index.html&token=tok1", "admin-tok", "hi"); rr.Code != http.StatusOK {
		t.Fatalf("asset put = %d", rr.Code)
	}
	if rr := do(t, data, http.MethodGet, "/v1/internal/asset?ns=acme&worker=web&token=tok1&path=index.html", "tok", nil); rr.Code != http.StatusOK {
		t.Fatalf("asset get before delete = %d", rr.Code)
	}
	if rr := adminDo(t, admin, http.MethodDelete, "/v1/control/worker?namespace=acme&worker=web", "admin-tok", ""); rr.Code != http.StatusOK {
		t.Fatalf("worker delete = %d %s", rr.Code, rr.Body.String())
	}
	if rr := do(t, data, http.MethodGet, "/v1/internal/asset?ns=acme&worker=web&token=tok1&path=index.html", "tok", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("asset get after delete = %d, want 404", rr.Code)
	}
}

// TestGCAdminEndpoints covers the bundle/assets GC admin endpoints end to end
// (ADR-110/ADR-111): an unreferenced upload is marked on the first pass and
// deleted on the next once the grace period is zero; a referenced upload
// survives.
func TestGCAdminEndpoints(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	admin := s.AdminHandler()
	st := s.artifactStore()

	refSHA, _, err := st.PutBundle(ctx, []byte("referenced"))
	if err != nil {
		t.Fatalf("put referenced: %v", err)
	}
	orphanSHA, _, err := st.PutBundle(ctx, []byte("orphan"))
	if err != nil {
		t.Fatalf("put orphan: %v", err)
	}
	if _, err := s.Control.CreateApp(ctx, "acme", "me"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "w", control.DeploySpec{BundleSHA: refSHA}, "me"); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	s.Cfg.BundleGCGrace = 0 // delete on the pass after marking

	// Pass 1 marks the orphan; nothing is deleted yet.
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/gc/bundles", "admin-tok", ""); rr.Code != http.StatusOK {
		t.Fatalf("gc pass 1 = %d: %s", rr.Code, rr.Body.String())
	} else if !strings.Contains(rr.Body.String(), `"marked":1`) {
		t.Fatalf("pass 1 body = %s, want 1 marked", rr.Body.String())
	}
	if _, err := st.GetBundle(ctx, orphanSHA); err != nil {
		t.Fatalf("orphan deleted on the marking pass: %v", err)
	}

	// Pass 2 deletes it; the referenced bundle stays.
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/gc/bundles", "admin-tok", ""); rr.Code != http.StatusOK {
		t.Fatalf("gc pass 2 = %d: %s", rr.Code, rr.Body.String())
	} else if !strings.Contains(rr.Body.String(), `"deleted":1`) {
		t.Fatalf("pass 2 body = %s, want 1 deleted", rr.Body.String())
	}
	if _, err := st.GetBundle(ctx, orphanSHA); err == nil {
		t.Fatal("orphan survived the second pass")
	}
	if _, err := st.GetBundle(ctx, refSHA); err != nil {
		t.Fatalf("referenced bundle was deleted: %v", err)
	}

	// Assets endpoint: an unreferenced asset version is marked then deleted.
	if _, err := st.PutAssetAt(ctx, "acme", "w", "tok9", "index.html", []byte("x")); err != nil {
		t.Fatalf("put asset: %v", err)
	}
	for i := 0; i < 2; i++ {
		if rr := adminDo(t, admin, http.MethodPost, "/v1/control/gc/assets", "admin-tok", ""); rr.Code != http.StatusOK {
			t.Fatalf("asset gc pass %d = %d: %s", i+1, rr.Code, rr.Body.String())
		}
	}
	if _, err := st.GetAsset(ctx, "acme", "w", "tok9", "index.html"); err == nil {
		t.Fatal("unreferenced asset survived GC")
	}
}

// TestControlHostAndWorkerEndpoints covers the two-level routing read path
// (ADR-115): a small per-host pointer view plus an immutable per-version worker
// view, both with ETag/304 and negative (404) caching support.
func TestControlHostAndWorkerEndpoints(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(ctx, "acme", "me"); err != nil {
		t.Fatalf("app: %v", err)
	}
	v1, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{
		BundleSHA: "sha1", AssetsSHA: "tok1",
		Bindings: []control.Binding{{Type: "kv", Name: "KV", ID: "acme/__kv__/main"}},
		Vars:     map[string]string{"MODE": "prod"},
	}, "me")
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	mustHost(t, s, "acme", "app.test")
	if err := s.Control.PutRoute(ctx, "acme", control.Route{Host: "app.test", Worker: "web"}, "me"); err != nil {
		t.Fatalf("route: %v", err)
	}

	// Host view: pointer only (no bindings/vars), host normalized (case + port).
	rr := do(t, s.Handler(), http.MethodGet, "/v1/control/host?host=APP.Test:443", "tok", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("host view = %d: %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"ns":"acme"`) || !strings.Contains(body, `"worker":"web"`) {
		t.Fatalf("host view body = %s", body)
	}
	if !strings.Contains(body, `"version":1`) || !strings.Contains(body, `"bundle_sha":"sha1"`) {
		t.Fatalf("host view missing worker pointer: %s", body)
	}
	if strings.Contains(body, "bindings") || strings.Contains(body, "MODE") || strings.Contains(body, "consumers") {
		t.Fatalf("host view must not carry worker details: %s", body)
	}
	hostEtag := rr.Header().Get("etag")
	if hostEtag == "" {
		t.Fatal("host view has no etag")
	}

	// If-None-Match revalidation.
	req := httptest.NewRequest(http.MethodGet, "/v1/control/host?host=app.test", nil)
	req.Header.Set("x-cellhive-internal-token", "tok")
	req.Header.Set("if-none-match", hostEtag)
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotModified {
		t.Fatalf("host revalidate = %d, want 304", rr.Code)
	}

	// Unknown host: 404 with an ETag so the loader can cache the negative result.
	rr = do(t, s.Handler(), http.MethodGet, "/v1/control/host?host=nope.test", "tok", nil)
	if rr.Code != http.StatusNotFound || rr.Header().Get("etag") == "" {
		t.Fatalf("unknown host = %d etag=%q, want 404 + etag", rr.Code, rr.Header().Get("etag"))
	}
	if rr := do(t, s.Handler(), http.MethodGet, "/v1/control/host", "tok", nil); rr.Code != http.StatusBadRequest {
		t.Fatalf("missing host = %d, want 400", rr.Code)
	}

	// Worker view: carries bindings/vars, drops fields the loader never uses.
	rr = do(t, s.Handler(), http.MethodGet, "/v1/control/worker?ns=acme&worker=web", "tok", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("worker view = %d: %s", rr.Code, rr.Body.String())
	}
	wbody := rr.Body.String()
	if !strings.Contains(wbody, `"bundle_sha":"sha1"`) || !strings.Contains(wbody, `"MODE":"prod"`) || !strings.Contains(wbody, `"bindings"`) {
		t.Fatalf("worker view body = %s", wbody)
	}
	if strings.Contains(wbody, "consumers") || strings.Contains(wbody, "session_policy") {
		t.Fatalf("worker view must drop loader-irrelevant fields: %s", wbody)
	}
	workerEtag := rr.Header().Get("etag")
	req = httptest.NewRequest(http.MethodGet, "/v1/control/worker?ns=acme&worker=web", nil)
	req.Header.Set("x-cellhive-internal-token", "tok")
	req.Header.Set("if-none-match", workerEtag)
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotModified {
		t.Fatalf("worker revalidate = %d, want 304", rr.Code)
	}
	if rr := do(t, s.Handler(), http.MethodGet, "/v1/control/worker?ns=acme&worker=ghost", "tok", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("unknown worker = %d, want 404", rr.Code)
	}
	if rr := do(t, s.Handler(), http.MethodGet, "/v1/control/worker?ns=acme", "tok", nil); rr.Code != http.StatusBadRequest {
		t.Fatalf("missing worker = %d, want 400", rr.Code)
	}

	// A binding-only deploy keeps the same bundle but allocates a new version, so
	// the host pointer ETag must change (the loader then refreshes the env).
	if _, err := s.Control.Deploy(ctx, "acme", "web", control.DeploySpec{
		BundleSHA: "sha1", AssetsSHA: "tok1",
		Bindings: []control.Binding{{Type: "kv", Name: "KV", ID: "acme/__kv__/other"}},
		Vars:     map[string]string{"MODE": "dev"},
	}, "me"); err != nil {
		t.Fatalf("redeploy: %v", err)
	}
	// Control writes bump the store revision, so the cached projection refreshes
	// on the next read (no explicit invalidation needed).
	rr = do(t, s.Handler(), http.MethodGet, "/v1/control/host?host=app.test", "tok", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("host view after redeploy = %d", rr.Code)
	}
	if rr.Header().Get("etag") == hostEtag {
		t.Fatal("host etag unchanged after a new version (binding-only deploy)")
	}
	if !strings.Contains(rr.Body.String(), `"version":2`) {
		t.Fatalf("host view did not pick up the new version: %s", rr.Body.String())
	}
	_ = v1
}

func TestInternalBindingsIncludeManagedSecrets(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(ctx, "acme", "me"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{
		BundleSHA: "sha1", Vars: map[string]string{"MODE": "prod"},
	}, "me"); err != nil {
		t.Fatal(err)
	}
	if err := s.Control.PutSecret(ctx, "acme", "api", "TOKEN", []byte("s3cr3t"), "me"); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/internal/worker/bindings?ns=acme&worker=api", nil)
	req.Header.Set("x-cellhive-internal-token", "tok")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bindings = %d: %s", rr.Code, rr.Body.String())
	}
	var got struct {
		Vars map[string]string `json:"vars"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Vars["MODE"] != "prod" || got.Vars["TOKEN"] != "s3cr3t" {
		t.Fatalf("runtime env = %#v", got.Vars)
	}
}

func TestWorkerExecutionViewIncludesManagedSecrets(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(ctx, "acme", "me"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{
		BundleSHA: "sha1", Vars: map[string]string{"MODE": "prod"},
	}, "me"); err != nil {
		t.Fatal(err)
	}
	if err := s.Control.PutSecret(ctx, "acme", "api", "TOKEN", []byte("s3cr3t"), "me"); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/control/worker?ns=acme&worker=api", nil)
	req.Header.Set("x-cellhive-internal-token", "tok")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("worker view = %d: %s", rr.Code, rr.Body.String())
	}
	var got control.WorkerView
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Vars["MODE"] != "prod" || got.Vars["TOKEN"] != "s3cr3t" {
		t.Fatalf("runtime env = %#v", got.Vars)
	}
}

func TestSecretPutRejectsRuntimeEnvOverBudget(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(ctx, "acme", "me"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "sha1"}, "me"); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(secretPutReq{
		Namespace: "acme", Worker: "api", Key: "TOKEN",
		Value: base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'x'}, int(workerbudget.EnvMaxBytes+1))),
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/control/secret", bytes.NewReader(body))
	req.Header.Set("x-cellhive-admin-token", "admin-tok")
	rr := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), `"error":"worker_env_too_large"`) {
		t.Fatalf("oversized secret = %d: %s", rr.Code, rr.Body.String())
	}
	if _, err := s.Control.GetSecret(ctx, "acme", "api", "TOKEN"); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("rejected secret was stored: %v", err)
	}
}

func TestWorkflowCreateRejectsPayloadOverLimit(t *testing.T) {
	s := newControlServer(t)
	ctx := context.Background()
	if _, err := s.Control.CreateApp(ctx, "acme", "me"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "workflow", "WF", "acme/__workflow__/WF", "me"); err != nil {
		t.Fatal(err)
	}
	tok, err := scopedtoken.Mint([]byte(s.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "workflow", Name: "WF"})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/workflow/create?ns=acme&workflow=WF", bytes.NewReader(bytes.Repeat([]byte{'x'}, (1<<20)+1)))
	req.Header.Set("x-cellhive-scope-token", tok)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized workflow payload = %d: %s", rr.Code, rr.Body.String())
	}
}

// TestInternalBindingsVersionPin covers ADR-128: the binding-spec endpoint (used
// by queue/scheduled/workflow dispatch and the do-runtime) resolves an exact
// version so a loaded worker's env cannot drift from the code the dispatcher
// asked for. Absent version resolves the active one; unknown versions fail open.
func TestInternalBindingsVersionPin(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(ctx, "acme", "me"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "w", control.DeploySpec{
		BundleSHA: "sha1",
		Bindings:  []control.Binding{{Type: "kv", Name: "KV", ID: "acme/__kv__/main"}},
		Vars:      map[string]string{"MODE": "v1"},
	}, "me"); err != nil {
		t.Fatalf("deploy v1: %v", err)
	}
	v2, err := s.Control.Deploy(ctx, "acme", "w", control.DeploySpec{
		BundleSHA: "sha2",
		Bindings:  []control.Binding{{Type: "kv", Name: "KV", ID: "acme/__kv__/main"}},
		Vars:      map[string]string{"MODE": "v2"},
	}, "me")
	if err != nil {
		t.Fatalf("deploy v2: %v", err)
	}
	if v2.Number != 2 {
		t.Fatalf("second deploy number = %d, want 2", v2.Number)
	}

	// Pinned older version: v1's immutable env, not the active v2.
	rr := do(t, s.Handler(), http.MethodGet, "/v1/internal/worker/bindings?ns=acme&worker=w&version=1", "tok", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"MODE":"v1"`) {
		t.Fatalf("version=1 = %d %s, want v1 env", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), `"MODE":"v2"`) {
		t.Fatalf("version=1 leaked the active env: %s", rr.Body.String())
	}
	// Absent version: active (v2). The do-runtime alias serves the same handler.
	rr = do(t, s.Handler(), http.MethodGet, "/v1/internal/do/bindings?ns=acme&worker=w", "tok", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"MODE":"v2"`) {
		t.Fatalf("active = %d %s, want v2 env", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"kind":"kv"`) {
		t.Fatalf("active env lost bindings: %s", rr.Body.String())
	}
	// Unknown version: 200 + empty spec (fail open; the runtime loads without
	// bindings rather than wedging dispatch).
	rr = do(t, s.Handler(), http.MethodGet, "/v1/internal/worker/bindings?ns=acme&worker=w&version=99", "tok", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"bindings":{}`) {
		t.Fatalf("unknown version = %d %s, want empty spec", rr.Code, rr.Body.String())
	}
	// Malformed version and missing params are rejected.
	if rr := do(t, s.Handler(), http.MethodGet, "/v1/internal/worker/bindings?ns=acme&worker=w&version=x", "tok", nil); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad version = %d, want 400", rr.Code)
	}
	if rr := do(t, s.Handler(), http.MethodGet, "/v1/internal/worker/bindings?ns=acme", "tok", nil); rr.Code != http.StatusBadRequest {
		t.Fatalf("missing worker = %d, want 400", rr.Code)
	}
}

// TestHyperdriveResourceAndBinding covers ADR-129: the origin URL is registered
// as a sealed hyperdrive resource (never plaintext) and resolved for the loaded
// worker's env; an inline URL id keeps working.
func TestHyperdriveResourceAndBinding(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	if _, err := s.Control.CreateApp(ctx, "acme", "me"); err != nil {
		t.Fatalf("app: %v", err)
	}
	const origin = "postgres://dbuser:s3cret@db.internal:5432/appdb"

	// Registering seals the origin URL.
	rr := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/resource", "admin-tok",
		`{"namespace":"acme","kind":"hyperdrive","name":"HYDR","connection_string":"`+origin+`"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("resource create = %d: %s", rr.Code, rr.Body.String())
	}
	// Bad kind / bad scheme are rejected.
	if rr := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/resource", "admin-tok",
		`{"namespace":"acme","kind":"kv","name":"KV","connection_string":"`+origin+`"}`); rr.Code != http.StatusBadRequest {
		t.Fatalf("connection_string for kv = %d, want 400", rr.Code)
	}
	if rr := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/resource", "admin-tok",
		`{"namespace":"acme","kind":"hyperdrive","name":"BAD","connection_string":"sqlite://x"}`); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad scheme = %d, want 400", rr.Code)
	}

	// The deploy gate accepts the binding now that the resource is registered.
	st := s.artifactStore()
	shaA, _, err := st.PutBundle(ctx, []byte("hyperdrive-a"))
	if err != nil {
		t.Fatalf("put bundle: %v", err)
	}
	if rr := mustAdmin(t, s.AdminHandler(), http.MethodPost, "/v1/control/deploy",
		`{"namespace":"acme","worker":"api","bundle_sha":"`+shaA+`","bindings":[{"type":"hyperdrive","name":"HYDR","id":"HYDR"}]}`); rr.Code != http.StatusOK {
		t.Fatalf("deploy with hyperdrive = %d: %s", rr.Code, rr.Body.String())
	}
	// A hyperdrive binding with no registered resource is still rejected.
	shaB, _, err := st.PutBundle(ctx, []byte("hyperdrive-b"))
	if err != nil {
		t.Fatalf("put bundle: %v", err)
	}
	if rr := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/deploy", "admin-tok",
		`{"namespace":"acme","worker":"nope","bundle_sha":"`+shaB+`","bindings":[{"type":"hyperdrive","name":"MISSING","id":"MISSING"}]}`); rr.Code != http.StatusBadRequest ||
		!strings.Contains(rr.Body.String(), "binding_unregistered") {
		t.Fatalf("unregistered hyperdrive = %d %s, want binding_unregistered", rr.Code, rr.Body.String())
	}

	// Internal endpoint resolves it (cold path for the loaded worker).
	rr = do(t, s.Handler(), http.MethodGet, "/v1/internal/hyperdrive?ns=acme&name=HYDR", "tok", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"connection_string":"`+origin+`"`) {
		t.Fatalf("internal hyperdrive = %d %s", rr.Code, rr.Body.String())
	}
	if rr := do(t, s.Handler(), http.MethodGet, "/v1/internal/hyperdrive?ns=acme&name=NOPE", "tok", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("missing resource = %d, want 404", rr.Code)
	}

	// The binding spec injected into a loaded worker carries the resolved URL.
	rr = do(t, s.Handler(), http.MethodGet, "/v1/internal/worker/bindings?ns=acme&worker=api", "tok", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"kind":"hyperdrive"`) ||
		!strings.Contains(rr.Body.String(), `"connectionString":"`+origin+`"`) {
		t.Fatalf("binding spec = %d %s, want resolved hyperdrive", rr.Code, rr.Body.String())
	}

	// Inline URLs keep working (back-compat with --hyperdrive NAME=URL).
	if _, err := s.Control.Deploy(ctx, "acme", "api2", control.DeploySpec{
		BundleSHA: shaA,
		Bindings:  []control.Binding{{Type: "hyperdrive", Name: "HYDR", ID: origin}},
	}, "me"); err != nil {
		t.Fatalf("deploy inline: %v", err)
	}
	rr = do(t, s.Handler(), http.MethodGet, "/v1/internal/worker/bindings?ns=acme&worker=api2", "tok", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"connectionString":"`+origin+`"`) {
		t.Fatalf("inline binding spec = %d %s", rr.Code, rr.Body.String())
	}
}

// TestSecretDeleteAndListEndpoints: /v1/control/secrets lists keys (no values)
// and /v1/control/secret DELETE removes one, audited (ADR-147).
func TestSecretDeleteAndListEndpoints(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/secret",
		`{"namespace":"acme","worker":"api","key":"TOKEN_A","value":"`+base64.StdEncoding.EncodeToString([]byte("va"))+`"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/secret",
		`{"namespace":"acme","worker":"api","key":"TOKEN_B","value":"`+base64.StdEncoding.EncodeToString([]byte("vb"))+`"}`)

	lst := mustAdmin(t, admin, http.MethodGet, "/v1/control/secrets?namespace=acme&worker=api", "")
	if !strings.Contains(lst.Body.String(), `"key":"TOKEN_A"`) || !strings.Contains(lst.Body.String(), `"key":"TOKEN_B"`) {
		t.Fatalf("list = %s", lst.Body.String())
	}
	if strings.Contains(lst.Body.String(), "va") || strings.Contains(lst.Body.String(), "wrapped_dek") {
		t.Fatalf("list leaked secret material: %s", lst.Body.String())
	}

	if rr := adminDo(t, admin, http.MethodDelete, "/v1/control/secret?namespace=acme&worker=api&key=TOKEN_A", "admin-tok", ""); rr.Code != http.StatusOK {
		t.Fatalf("delete = %d %s", rr.Code, rr.Body.String())
	}
	if rr := adminDo(t, admin, http.MethodGet, "/v1/control/secret?namespace=acme&worker=api&key=TOKEN_A", "admin-tok", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("get deleted = %d, want 404", rr.Code)
	}
	lst2 := mustAdmin(t, admin, http.MethodGet, "/v1/control/secrets?namespace=acme&worker=api", "")
	if strings.Contains(lst2.Body.String(), "TOKEN_A") || !strings.Contains(lst2.Body.String(), "TOKEN_B") {
		t.Fatalf("after delete = %s", lst2.Body.String())
	}
	// Audited.
	audit := mustAdmin(t, admin, http.MethodGet, "/v1/control/audit?namespace=acme&limit=10", "")
	if !strings.Contains(audit.Body.String(), "secret.delete") {
		t.Fatalf("audit missing secret.delete: %s", audit.Body.String())
	}
}

// TestDeployDryRun: dry_run validates (compatibility gate + bundle + ACL) and
// returns without creating a version or touching state (ADR-148).
func TestDeployDryRun(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	b := mustAdmin(t, admin, http.MethodPost, "/v1/control/bundle", "export default {}")
	var bo struct {
		Sha string `json:"sha"`
	}
	_ = json.Unmarshal(b.Body.Bytes(), &bo)

	rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok",
		`{"namespace":"acme","worker":"api","bundle_sha":"`+bo.Sha+`","dry_run":true}`)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"dry_run":true`) {
		t.Fatalf("dry-run = %d %s", rr.Code, rr.Body.String())
	}
	// No version was created.
	rel := mustAdmin(t, admin, http.MethodGet, "/v1/control/releases?namespace=acme&worker=api", "")
	var out struct {
		Releases []any `json:"releases"`
	}
	_ = json.Unmarshal(rel.Body.Bytes(), &out)
	if len(out.Releases) != 0 {
		t.Fatalf("dry-run created versions: %s", rel.Body.String())
	}
	// An invalid dry-run still reports the findings.
	bad := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok",
		`{"namespace":"acme","worker":"api","bundle_sha":"deadbeef","dry_run":true}`)
	if bad.Code != http.StatusBadRequest || !strings.Contains(bad.Body.String(), "missing_bundle") {
		t.Fatalf("invalid dry-run = %d %s", bad.Code, bad.Body.String())
	}
	// And still created nothing.
	rel2 := mustAdmin(t, admin, http.MethodGet, "/v1/control/releases?namespace=acme&worker=api", "")
	if !strings.Contains(rel2.Body.String(), `"releases":[]`) && !strings.Contains(rel2.Body.String(), `"releases":null`) {
		t.Fatalf("invalid dry-run created versions: %s", rel2.Body.String())
	}
}

// TestResourceDeleteEndpoint: revoking a resource is refused while a worker
// still binds it (409 + referenced_by) unless force=1, and after a revoke a
// deploy declaring that binding fails the compatibility gate (ADR-156).
func TestResourceDeleteEndpoint(t *testing.T) {
	s := newControlServer(t)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/resource",
		`{"namespace":"acme","kind":"kv","name":"KV","scope":"acme/__kv__/main"}`)
	b := mustAdmin(t, admin, http.MethodPost, "/v1/control/bundle", "export default {}")
	var bo struct {
		Sha string `json:"sha"`
	}
	_ = json.Unmarshal(b.Body.Bytes(), &bo)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/deploy",
		`{"namespace":"acme","worker":"web","bundle_sha":"`+bo.Sha+`","bindings":[{"type":"kv","name":"KV","id":"acme/__kv__/main"}]}`)

	// In use -> 409 with the referencing worker.
	rr := adminDo(t, admin, http.MethodDelete, "/v1/control/resource?namespace=acme&kind=kv&name=KV", "admin-tok", "")
	if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), `"referenced_by":["web"]`) {
		t.Fatalf("delete in use = %d %s", rr.Code, rr.Body.String())
	}
	// Force -> 200.
	rr = adminDo(t, admin, http.MethodDelete, "/v1/control/resource?namespace=acme&kind=kv&name=KV&force=1", "admin-tok", "")
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"forced":true`) {
		t.Fatalf("forced delete = %d %s", rr.Code, rr.Body.String())
	}
	lst := mustAdmin(t, admin, http.MethodGet, "/v1/control/resources?namespace=acme", "")
	if strings.Contains(lst.Body.String(), `"name":"KV"`) {
		t.Fatalf("resource still listed: %s", lst.Body.String())
	}
	// A new deploy declaring the revoked binding is rejected by the gate.
	rr = adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok",
		`{"namespace":"acme","worker":"web2","bundle_sha":"`+bo.Sha+`","bindings":[{"type":"kv","name":"KV","id":"acme/__kv__/main"}]}`)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "binding_unregistered") {
		t.Fatalf("deploy after revoke = %d %s, want binding_unregistered", rr.Code, rr.Body.String())
	}
	// Unknown resource -> 404; audit records the revoke.
	rr = adminDo(t, admin, http.MethodDelete, "/v1/control/resource?namespace=acme&kind=kv&name=NOPE", "admin-tok", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unknown resource = %d, want 404", rr.Code)
	}
	audit := mustAdmin(t, admin, http.MethodGet, "/v1/control/audit?namespace=acme&limit=20", "")
	if !strings.Contains(audit.Body.String(), "resource.delete") {
		t.Fatalf("audit missing resource.delete: %s", audit.Body.String())
	}
}

// TestQueueStatusAndReplayDLQ: operator queue status reports the DLQ depth and
// replay-dlq moves dead letters back onto the main queue (ADR-156).
func TestQueueStatusAndReplayDLQ(t *testing.T) {
	ctx := context.Background()
	s := newControlServer(t)
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.Store = cs
	s.Queue = queue.New(cs)
	admin := s.AdminHandler()
	mustAdmin(t, admin, http.MethodPost, "/v1/control/app", `{"namespace":"acme"}`)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/resource",
		`{"namespace":"acme","kind":"queue","name":"q","scope":"acme/__queue__/q"}`)
	b := mustAdmin(t, admin, http.MethodPost, "/v1/control/bundle", "export default {}")
	var bo struct {
		Sha string `json:"sha"`
	}
	_ = json.Unmarshal(b.Body.Bytes(), &bo)
	mustAdmin(t, admin, http.MethodPost, "/v1/control/deploy",
		`{"namespace":"acme","worker":"consumer","bundle_sha":"`+bo.Sha+`","bindings":[{"type":"queue","name":"q","id":"q"}],"consumers":[{"queue":"q","dead_letter_queue":"qdlq","max_retries":2}]}`)

	for _, body := range []string{"m1", "m2"} {
		if _, err := s.Queue.Send(ctx, "acme", "qdlq", []byte(body), "text/plain", 0, ""); err != nil {
			t.Fatalf("seed dlq: %v", err)
		}
	}
	st := mustAdmin(t, admin, http.MethodGet, "/v1/control/queue/status?namespace=acme&queue=q", "")
	for _, want := range []string{`"queue":"q"`, `"dead_letter_queue":"qdlq"`, `"dead_letter_depth":2`, `"depth":0`} {
		if !strings.Contains(st.Body.String(), want) {
			t.Fatalf("status %s missing %s", st.Body.String(), want)
		}
	}

	// Production replay runs with an owner manager: the main queue may already
	// have a live lease on this same node after normal consumer activity.
	om := &owner.Manager{B: s.Bucket, NodeID: s.Cfg.NodeID, Session: "replay-test", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	s.Owner = om
	if _, err := om.Claim(ctx, queue.Scope("acme", "q"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := om.Claim(ctx, queue.Scope("acme", "qdlq"), time.Now()); err != nil {
		t.Fatal(err)
	}
	rr := mustAdmin(t, admin, http.MethodPost, "/v1/control/queue/replay-dlq?namespace=acme&queue=q&limit=10", "")
	if !strings.Contains(rr.Body.String(), `"replayed":2`) {
		t.Fatalf("replay = %s", rr.Body.String())
	}
	// The main queue has the two messages; the DLQ is drained.
	main := mustAdmin(t, admin, http.MethodGet, "/v1/control/queue/status?namespace=acme&queue=q", "")
	if !strings.Contains(main.Body.String(), `"depth":2`) || !strings.Contains(main.Body.String(), `"visible":2`) {
		t.Fatalf("main queue after replay = %s", main.Body.String())
	}
	dlq := mustAdmin(t, admin, http.MethodGet, "/v1/control/queue/status?namespace=acme&queue=qdlq", "")
	if !strings.Contains(dlq.Body.String(), `"depth":0`) {
		t.Fatalf("dlq after replay = %s", dlq.Body.String())
	}
	// A queue without a DLQ consumer is a clear bad request.
	rr = adminDo(t, admin, http.MethodPost, "/v1/control/queue/replay-dlq?namespace=acme&queue=nope", "admin-tok", "")
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "no_dead_letter_queue") {
		t.Fatalf("replay without dlq = %d %s", rr.Code, rr.Body.String())
	}
}
