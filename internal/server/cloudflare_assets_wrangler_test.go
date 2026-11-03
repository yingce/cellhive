package server

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestReviewWranglerAssetsRealCLI exercises the real Wrangler 4 asset upload
// path (assets-upload-session -> asset upload -> deploy) against the CF-API.
// Set CELLHIVE_TEST_WRANGLER_CLI to the wrangler/bin/wrangler.js path.
func TestReviewWranglerAssetsRealCLI(t *testing.T) {
	cli := os.Getenv("CELLHIVE_TEST_WRANGLER_CLI")
	if cli == "" {
		t.Skip("set CELLHIVE_TEST_WRANGLER_CLI to run real Wrangler")
	}
	s := newControlServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(s.AdminHandler())
	defer httpServer.Close()
	dir := t.TempDir()
	public := filepath.Join(dir, "public")
	if err := os.MkdirAll(filepath.Join(public, "assets"), 0o700); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("worker.js", `export default { fetch(req) { return new Response("api:" + new URL(req.url).pathname) } }`)
	write("public/index.html", `<!doctype html><div id=root>spa</div>`)
	write("public/assets/app.js", `console.log("asset")`)
	write("wrangler.jsonc", `{
  "name": "assets-app",
  "main": "worker.js",
  "compatibility_date": "2026-09-23",
  "assets": {
    "directory": "./public",
    "binding": "ASSETS",
    "not_found_handling": "single-page-application",
    "run_worker_first": ["/api/*"]
  }
}`)
	env := []string{}
	for _, v := range os.Environ() {
		k, _, _ := strings.Cut(v, "=")
		if !strings.HasPrefix(k, "CLOUDFLARE_") && !strings.HasSuffix(strings.ToUpper(k), "_PROXY") {
			env = append(env, v)
		}
	}
	env = append(env,
		"CLOUDFLARE_API_BASE_URL="+httpServer.URL+"/client/v4",
		"CLOUDFLARE_API_TOKEN="+testRoleJWT(t, "admin"),
		"CLOUDFLARE_ACCOUNT_ID=acme",
		"WRANGLER_SEND_METRICS=false", "CI=true", "NO_PROXY=*",
	)
	runCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(runCtx, "node", cli, "deploy")
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("wrangler assets deploy failed: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte("Deployed")) && !bytes.Contains(out, []byte("Published")) {
		t.Logf("wrangler output:\\n%s", out)
	}
	view, ok, err := s.Control.WorkerEnv(ctx, "acme", "assets-app")
	if err != nil || !ok {
		t.Fatalf("worker env: %v %v", ok, err)
	}
	if view.AssetsSHA == "" || view.Assets == nil || view.Assets.Binding != "ASSETS" || view.Assets.NotFoundHandling != "single-page-application" {
		t.Fatalf("assets config = %+v sha=%q", view.Assets, view.AssetsSHA)
	}
	if len(view.Assets.RunWorkerFirstPaths) != 1 || view.Assets.RunWorkerFirstPaths[0] != "/api/*" {
		t.Fatalf("run_worker_first = %+v", view.Assets.RunWorkerFirstPaths)
	}
	found := false
	for _, b := range view.Bindings {
		if b.Type == "assets" && b.Name == "ASSETS" && b.ID == view.AssetsSHA {
			found = true
		}
	}
	if !found {
		t.Fatalf("assets binding missing: %+v", view.Bindings)
	}
	asset, err := s.artifactStore().GetAsset(ctx, "acme", "assets-app", view.AssetsSHA, "index.html")
	if err != nil || !bytes.Contains(asset, []byte("spa")) {
		t.Fatalf("index asset = %q %v", asset, err)
	}
	// A repeat deploy must inherit the active asset token instead of failing
	// when Wrangler omits assets.jwt for unchanged content.
	runCtx2, cancel2 := context.WithTimeout(ctx, 120*time.Second)
	defer cancel2()
	cmd2 := exec.CommandContext(runCtx2, "node", cli, "deploy")
	cmd2.Dir = dir
	cmd2.Env = env
	out2, err2 := cmd2.CombinedOutput()
	if err2 != nil {
		t.Fatalf("repeat wrangler deploy failed: %v\n%s", err2, out2)
	}
	view2, _, _ := s.Control.WorkerEnv(ctx, "acme", "assets-app")
	if view2.AssetsSHA != view.AssetsSHA {
		t.Fatalf("repeat deploy changed assets token: %q -> %q", view.AssetsSHA, view2.AssetsSHA)
	}
	// Assets-only: no main module, fixed 404 Worker created by the platform.
	write("wrangler-only.jsonc", `{
  "name": "assets-only",
  "compatibility_date": "2026-09-23",
  "assets": {
    "directory": "./public",
    "not_found_handling": "single-page-application"
  }
}`)
	runCtx3, cancel3 := context.WithTimeout(ctx, 120*time.Second)
	defer cancel3()
	cmd3 := exec.CommandContext(runCtx3, "node", cli, "deploy", "--config", "wrangler-only.jsonc")
	cmd3.Dir = dir
	cmd3.Env = env
	out3, err3 := cmd3.CombinedOutput()
	if err3 != nil {
		t.Fatalf("assets-only wrangler deploy failed: %v\n%s", err3, out3)
	}
	only, ok, err := s.Control.WorkerEnv(ctx, "acme", "assets-only")
	if err != nil || !ok || only.Assets == nil || !only.Assets.AssetsOnly || only.AssetsSHA == "" {
		t.Fatalf("assets-only view = %+v %v %v", only, ok, err)
	}

	// Tenant JWT on the same CF-API surface: no admin token required.
	jwt := testNamespaceJWT(t, s, "tenant@corp", "acme")
	cfServer := httptest.NewServer(s.AdminHandler())
	defer cfServer.Close()
	jwtEnv := append([]string{}, env...)
	for i, v := range jwtEnv {
		if strings.HasPrefix(v, "CLOUDFLARE_API_TOKEN=") {
			jwtEnv[i] = "CLOUDFLARE_API_TOKEN=" + jwt
		}
		if strings.HasPrefix(v, "CLOUDFLARE_API_BASE_URL=") {
			jwtEnv[i] = "CLOUDFLARE_API_BASE_URL=" + cfServer.URL + "/client/v4"
		}
	}
	write("wrangler-jwt.jsonc", `{
  "name": "jwt-app",
  "main": "worker.js",
  "compatibility_date": "2026-09-23",
  "assets": {"directory": "./public", "binding": "ASSETS"}
}`)
	runCtx4, cancel4 := context.WithTimeout(ctx, 120*time.Second)
	defer cancel4()
	cmd4 := exec.CommandContext(runCtx4, "node", cli, "deploy", "--config", "wrangler-jwt.jsonc")
	cmd4.Dir = dir
	cmd4.Env = jwtEnv
	out4, err4 := cmd4.CombinedOutput()
	if err4 != nil {
		t.Fatalf("JWT wrangler deploy failed: %v\n%s", err4, out4)
	}
	jwtView, ok, err := s.Control.WorkerEnv(ctx, "acme", "jwt-app")
	if err != nil || !ok || jwtView.AssetsSHA == "" {
		t.Fatalf("JWT deploy view = %+v %v %v", jwtView, ok, err)
	}
}
