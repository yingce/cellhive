package server

import (
	"bytes"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/queue"
)

// Run with CELLHIVE_WRANGLER_BIN=/absolute/path/to/wrangler/bin/wrangler.js.
// The test uses an isolated admin server and the stock Wrangler executable.
func TestStockWranglerControlE2E(t *testing.T) {
	bin := os.Getenv("CELLHIVE_WRANGLER_BIN")
	if bin == "" {
		t.Skip("set CELLHIVE_WRANGLER_BIN for stock Wrangler E2E")
	}
	s := newControlServer(t)
	qcs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.Queue = queue.New(qcs)
	if _, err := s.Control.CreateApp(t.Context(), "acme", "e2e"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(t.Context(), "acme", "web", control.DeploySpec{BundleSHA: strings.Repeat("a", 64)}, "e2e"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(t.Context(), "acme", "queue", "jobs", "acme/__queue__/jobs", "e2e"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(t.Context(), "acme", "workflow", "flow", "acme/__workflow__/flow", "e2e"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(t.Context(), "acme", "web", control.DeploySpec{BundleSHA: strings.Repeat("a", 64), Bindings: []control.Binding{{Type: "workflow", Name: "flow", ID: "flow", ClassName: "Flow"}}}, "e2e"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Workflows.Create(t.Context(), "acme", "flow", "run-one", []byte(`{"hello":"世界"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Workflows.PutStep(t.Context(), "acme", "flow", "run-one", "saved", []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	h := httptest.NewServer(s.AdminHandler())
	defer h.Close()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wrangler.jsonc"), []byte(`{"name":"web","main":"index.js","account_id":"acme","compatibility_date":"2026-09-23"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte(`export default {fetch(){return new Response("ok")}}`), 0600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "node", append([]string{bin}, args...)...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "CLOUDFLARE_API_BASE_URL="+h.URL+"/client/v4", "CLOUDFLARE_API_TOKEN=admin-tok", "CLOUDFLARE_ACCOUNT_ID=acme", "WRANGLER_SEND_METRICS=false")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("wrangler %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	if out := run("deployments", "list", "--name", "web", "--json"); !strings.Contains(out, `"version_id": "1"`) {
		t.Fatalf("deployments: %s", out)
	}
	if out := run("deployments", "status", "--name", "web", "--json"); !strings.Contains(out, `"version_id": "2"`) {
		t.Fatalf("deployment status: %s", out)
	}
	if out := run("versions", "list", "--name", "web", "--json"); !strings.Contains(out, `"id": "2"`) {
		t.Fatalf("versions list: %s", out)
	}
	run("queues", "consumer", "add", "jobs", "web")
	if out := run("queues", "consumer", "list", "jobs"); !strings.Contains(out, "web") {
		t.Fatalf("consumer list: %s", out)
	}
	if _, err := s.Queue.Send(t.Context(), "acme", "jobs", []byte(`{"n":1}`), "json", 0, ""); err != nil {
		t.Fatal(err)
	}
	run("queues", "purge", "jobs", "--force")
	if depth, err := s.Queue.Depth(t.Context(), "acme", "jobs"); err != nil || depth != 0 {
		t.Fatalf("purge depth=%d err=%v", depth, err)
	}
	if out := run("workflows", "instances", "list", "flow", "--json"); !strings.Contains(out, "run-one") {
		t.Fatalf("instance list: %s", out)
	}
	if out := run("workflows", "instances", "describe", "flow", "run-one", "--json"); !bytes.Contains([]byte(out), []byte(`"saved"`)) {
		t.Fatalf("instance describe: %s", out)
	}
	if out := run("workflows", "trigger", "flow", "--params", `{"hello":"world"}`, "--id", "run-two", "--json"); !strings.Contains(out, "run-two") {
		t.Fatalf("trigger: %s", out)
	}
	if _, err := s.Workflows.Get(t.Context(), "acme", "flow", "run-two"); err != nil {
		t.Fatalf("trigger did not persist: %v", err)
	}
}

func TestStockWranglerVectorizeE2E(t *testing.T) {
	bin := os.Getenv("CELLHIVE_WRANGLER_BIN")
	if bin == "" {
		t.Skip("set CELLHIVE_WRANGLER_BIN for stock Wrangler E2E")
	}
	s := newVectorizeServer(t)
	if _, err := s.Control.CreateApp(t.Context(), "acme", "e2e"); err != nil {
		t.Fatal(err)
	}
	h := httptest.NewServer(s.AdminHandler())
	defer h.Close()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wrangler.jsonc"), []byte(`{"name":"web","main":"index.js","account_id":"acme","compatibility_date":"2026-09-23"}`), 0600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "node", append([]string{bin}, args...)...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "CLOUDFLARE_API_BASE_URL="+h.URL+"/client/v4", "CLOUDFLARE_API_TOKEN=admin-tok", "CLOUDFLARE_ACCOUNT_ID=acme", "WRANGLER_SEND_METRICS=false")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("wrangler %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	if out := run("vectorize", "create", "docs", "--dimensions", "3", "--metric", "cosine", "--json"); !strings.Contains(out, "docs") {
		t.Fatalf("create: %s", out)
	}
	if out := run("vectorize", "list", "--json"); !strings.Contains(out, "docs") {
		t.Fatalf("list: %s", out)
	}
	if out := run("vectorize", "get", "docs", "--json"); !strings.Contains(out, "cosine") {
		t.Fatalf("get: %s", out)
	}
	if out := run("vectorize", "info", "docs", "--json"); !strings.Contains(out, "vectorCount") {
		t.Fatalf("info: %s", out)
	}
}
