package server

import (
	"bytes"
	"cellhive/internal/control"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func reviewCFUpload(t *testing.T, s *Server, metadata string, deploy ...bool) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	p, _ := mw.CreateFormField("metadata")
	p.Write([]byte(metadata))
	p, _ = mw.CreateFormFile("worker.js", "worker.js")
	p.Write([]byte("export default {fetch(){return new Response('new')}}"))
	mw.Close()
	method, path := http.MethodPost, "/client/v4/accounts/acme/workers/scripts/api/versions"
	if len(deploy) > 0 {
		method, path = http.MethodPut, "/client/v4/accounts/acme/workers/scripts/api"
	}
	r := httptest.NewRequest(method, path, &body)
	r.Header.Set("Authorization", "Bearer admin-tok")
	r.Header.Set("Content-Type", mw.FormDataContentType())
	rr := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rr, r)
	return rr
}
func TestReviewWranglerUploadMustNotPublish(t *testing.T) {
	s := newControlServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "oldsha"}, "ops"); err != nil {
		t.Fatal(err)
	}
	rr := reviewCFUpload(t, s, `{"main_module":"worker.js","compatibility_date":"2026-09-23"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("upload=%d %s", rr.Code, rr.Body.String())
	}
	active, _, err := s.Control.WorkerEnv(ctx, "acme", "api")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("upload HTTP=%d active_version=%d", rr.Code, active.Version)
	if active.Version != 1 {
		t.Fatal("versions upload unexpectedly changed active worker")
	}
}
func TestReviewWranglerBindingMustNotDisappear(t *testing.T) {
	for _, typ := range []string{"hyperdrive", "ai", "vectorize", "secret_text", "json", "unsupported_binding"} {
		t.Run(typ, func(t *testing.T) {
			s := newControlServer(t)
			meta, _ := json.Marshal(map[string]any{"main_module": "worker.js", "compatibility_date": "2026-09-23", "bindings": []any{map[string]any{"type": typ, "name": "CONFIG", "id": "resource", "text": "secret-test-value", "json": map[string]any{"flag": true}}}})
			rr := reviewCFUpload(t, s, string(meta), true)
			if rr.Code >= 400 {
				if rr.Code != http.StatusBadRequest {
					t.Fatalf("unexpected failure: %d %s", rr.Code, rr.Body.String())
				}
				return
			}
			v, _, err := s.Control.WorkerEnv(t.Context(), "acme", "api")
			if err != nil {
				t.Fatal(err)
			}
			sec, err := s.Control.VersionSecrets(t.Context(), "acme", "api", v.Version)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("HTTP=%d bindings=%d vars=%d secrets=%d", rr.Code, len(v.Bindings), len(v.Vars), len(sec))
			if len(v.Bindings) == 0 && len(v.Vars) == 0 && len(sec) == 0 {
				t.Fatal("successful upload silently dropped binding")
			}
		})
	}
}
func TestReviewWranglerSettingsMustReflectWorker(t *testing.T) {
	s := newControlServer(t)
	ctx := t.Context()
	s.Control.CreateApp(ctx, "acme", "ops")
	v, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "sha", Vars: map[string]string{"FLAG": "value"}, CompatDate: "2026-09-01"}, "ops")
	if err != nil {
		t.Fatal(err)
	}
	_ = v
	r := httptest.NewRequest(http.MethodGet, "/client/v4/accounts/acme/workers/scripts/api/settings", nil)
	r.Header.Set("Authorization", "Bearer admin-tok")
	rr := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(rr, r)
	t.Logf("settings HTTP=%d body=%s", rr.Code, rr.Body.String())
	if !bytes.Contains(rr.Body.Bytes(), []byte("2026-09-01")) || !bytes.Contains(rr.Body.Bytes(), []byte("FLAG")) {
		t.Fatal("settings differ from deployed configuration")
	}
}

func TestReviewWranglerRealCLI(t *testing.T) {
	cli := os.Getenv("CELLHIVE_TEST_WRANGLER_CLI")
	if cli == "" {
		t.Skip("set CELLHIVE_TEST_WRANGLER_CLI to run real Wrangler")
	}
	s := newControlServer(t)
	ctx := t.Context()
	s.Control.CreateApp(ctx, "acme", "ops")
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "oldsha"}, "ops"); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(s.AdminHandler())
	defer httpServer.Close()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "wrangler.jsonc"), []byte(`{"name":"api","main":"worker.js","compatibility_date":"2026-09-23"}`), 0600)
	os.WriteFile(filepath.Join(dir, "worker.js"), []byte(`export default {fetch(){return new Response("cli-new")}}`), 0600)
	env := []string{}
	for _, v := range os.Environ() {
		k, _, _ := strings.Cut(v, "=")
		if !strings.HasPrefix(k, "CLOUDFLARE_") && !strings.HasSuffix(strings.ToUpper(k), "_PROXY") {
			env = append(env, v)
		}
	}
	env = append(env, "CLOUDFLARE_API_BASE_URL="+httpServer.URL+"/client/v4", "CLOUDFLARE_API_TOKEN=admin-tok", "CLOUDFLARE_ACCOUNT_ID=acme", "WRANGLER_SEND_METRICS=false", "CI=true", "NO_PROXY=*")
	for _, args := range [][]string{{"versions", "upload"}, {"secret", "put", "TOKEN"}, {"secret", "list"}, {"versions", "secret", "list"}, {"versions", "secret", "put", "TOKEN"}, {"versions", "deploy", "4@100%", "--yes"}, {"deploy"}} {
		runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		cmd := exec.CommandContext(runCtx, "node", append([]string{cli}, args...)...)
		cmd.Dir = dir
		cmd.Env = env
		cmd.Stdin = strings.NewReader("fake-review-secret\n")
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Errorf("command failed: %s", out)
		}
		active, _, _ := s.Control.WorkerEnv(ctx, "acme", "api")
		t.Logf("command=%v error=%v active=%d output=\n%s", args, err, active.Version, out)
		if args[0] == "deploy" && active.Version != 5 {
			t.Errorf("deploy active=%d", active.Version)
		}
		if len(args) > 1 && args[1] == "deploy" && active.Version != 4 {
			t.Errorf("versions deploy active=%d", active.Version)
		}
		if len(args) == 3 && args[0] == "versions" && args[2] == "list" && !bytes.Contains(out, []byte("TOKEN")) {
			t.Error("version secret list omitted stored secret")
		}
		if args[0] == "versions" && args[1] == "secret" && len(args) == 4 && active.Version != 3 {
			t.Error("versioned secret update changed active version")
		}
		if args[0] == "versions" && len(args) == 2 && active.Version != 1 {
			t.Error("CLI versions upload changed active")
		}
	}
}

func TestCloudflareBindingTranslation(t *testing.T) {
	var meta cloudflareWorkerMetadata
	err := decodeCloudflareMetadata([]byte(`{"bindings":[{"type":"hyperdrive","name":"DB","id":"hd"},{"type":"vectorize","name":"INDEX","index_name":"idx"},{"type":"ai","name":"AI"}]}`), &meta)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Bindings) != 3 || meta.Bindings[0].ID != "hd" || meta.Bindings[1].ID != "idx" || meta.Bindings[2].Type != "ai" {
		t.Fatalf("bindings: %+v", meta.Bindings)
	}
}

func TestCloudflareVersionSecretMetadataIsolation(t *testing.T) {
	s := newControlServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "sha"}, "ops"); err != nil {
		t.Fatal(err)
	}
	call := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, "/client/v4/accounts/acme/workers/scripts/api"+path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer admin-tok")
		rr := httptest.NewRecorder()
		s.AdminHandler().ServeHTTP(rr, r)
		return rr
	}
	if rr := call("PUT", "/secrets", `{"name":"TOKEN","text":"do-not-return-this","type":"secret_text"}`); rr.Code != 200 {
		t.Fatal(rr.Body.String())
	}
	for _, path := range []string{"/versions/1", "/versions/2", "/settings", "/secrets"} {
		rr := call("GET", path, "")
		if rr.Code != 200 {
			t.Fatalf("%s: %d %s", path, rr.Code, rr.Body.String())
		}
		has := strings.Contains(rr.Body.String(), "TOKEN")
		if has != (path != "/versions/1") || strings.Contains(rr.Body.String(), "do-not-return-this") {
			t.Fatalf("incorrect secret metadata: %s", rr.Body.String())
		}
	}
	if rr := call("PATCH", "/script-settings", `{"compatibility_date":"2020-01-01"}`); rr.Code != 501 {
		t.Fatalf("PATCH: %d", rr.Code)
	}
	if rr := call("GET", "/versions/999", ""); rr.Code != 404 {
		t.Fatalf("missing: %d", rr.Code)
	}
}

func TestWranglerStagingRejectsMigrationAndInvalidSecretPatch(t *testing.T) {
	s := newControlServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "old"}, "ops"); err != nil {
		t.Fatal(err)
	}
	rr := reviewCFUpload(t, s, `{"main_module":"worker.js","compatibility_date":"2026-09-23","migrations":[{"tag":"v1","new_classes":["Counter"]}]}`)
	if rr.Code != 400 {
		t.Fatalf("migration stage=%d %s", rr.Code, rr.Body.String())
	}
	for _, body := range []string{`{"env":{"TOKEN":{"type":"plain_text","text":"bad"}}}`, `{"env":{"TOKEN":{"type":"secret_text"}}}`, `{"env":{}}`} {
		r := httptest.NewRequest("PATCH", "/client/v4/accounts/acme/workers/workers/api/versions/latest", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer admin-tok")
		rr := httptest.NewRecorder()
		s.AdminHandler().ServeHTTP(rr, r)
		if rr.Code != 400 {
			t.Fatalf("invalid patch=%d %s", rr.Code, rr.Body.String())
		}
	}
	releases, err := s.Control.Releases(ctx, "acme", "api")
	if err != nil || len(releases) != 1 {
		t.Fatalf("failed request created versions: %+v %v", releases, err)
	}
}
