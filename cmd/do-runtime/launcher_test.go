package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"cellhive/internal/bearertoken"
	"cellhive/internal/config"
	"cellhive/internal/doruntime"
)

// TestLauncherBindingInsideDO catches a missing BindingSigningKey in main's
// Config: rendering a hand-built Config would bypass the production wiring.
func TestLauncherBindingInsideDO(t *testing.T) {
	workerd, err := doruntime.FindWorkerd()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	workerd, err = filepath.Abs(workerd)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "do-runtime")
	args := []string{"build", "-o", binary}
	// Preserve explicit test build tags (notably SQLite) and race mode. GOFLAGS,
	// including a mutation-test overlay, is also inherited by the Go subprocess.
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "-tags" {
				args = append(args, "-tags="+setting.Value)
			}
			if setting.Key == "-race" && setting.Value == "true" {
				args = append(args, "-race")
			}
		}
	}
	args = append(args, "./cmd/do-runtime")
	t.Logf("build launcher: go %s", strings.Join(args, " "))
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	build := exec.CommandContext(buildCtx, "go", args...)
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build launcher: %v\n%s", err, out)
	}

	// Public test fixture only; never inherit the developer's platform secrets.
	rootKey := strings.Repeat("71", 32)
	creds := config.DeriveCredentials(rootKey)
	bindingKey, err := base64.RawURLEncoding.DecodeString(creds.Binding)
	if err != nil {
		t.Fatal(err)
	}
	var verified atomic.Int32
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/kv/get-batch" {
			var request struct {
				Keys []string `json:"keys"`
			}
			decodeErr := json.NewDecoder(r.Body).Decode(&request)
			kid, claims, err := bearertoken.Verify(bindingKey, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), time.Now())
			if err != nil || kid != "token/scope" || claims.Type != "binding" ||
				claims.Namespace != "launcher-test" || claims.Kind != "kv" || claims.Name != "KV" ||
				r.Method != http.MethodPost || r.URL.Query().Get("ns") != "launcher-test" ||
				decodeErr != nil || len(request.Keys) != 1 || request.Keys[0] != "greeting" {
				t.Error("KV request did not carry the expected signed binding scope and resource")
				http.Error(w, "invalid binding authorization", http.StatusUnauthorized)
				return
			}
			verified.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []map[string]any{
				{"found": true, "value": base64.StdEncoding.EncodeToString([]byte("launcher-binding-ok"))},
			}})
			return
		}
		if err := bearertoken.VerifyInternal(r.Header, creds.Internal, "internal", time.Now()); err != nil {
			t.Errorf("invalid internal authorization on %s", r.URL.Path)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/internal/bundle":
			_, _ = io.WriteString(w, `import { DurableObject } from "cloudflare:workers";
export class Tenant extends DurableObject {
  async fetch() { return new Response(await this.env.KV.get("greeting")); }
}`)
		case "/v1/internal/do/bindings":
			_, _ = io.WriteString(w, `{"bindings":{"KV":{"kind":"kv","ns":"launcher-test","name":"KV","authorization":{"ns":"launcher-test","kind":"kv","name":"KV"}}},"vars":{}}`)
		case "/v1/internal/do/claim", "/v1/internal/do/renew":
			_ = json.NewEncoder(w).Encode(map[string]any{"epoch": 1, "expiry_ms": time.Now().Add(time.Minute).UnixMilli(), "node": "launcher-test"})
		case "/v1/internal/do/release":
			_, _ = io.WriteString(w, `{"released":true}`)
		default:
			// Registration, telemetry and alarm bookkeeping are not under test.
			_, _ = io.WriteString(w, `{"ok":true}`)
		}
	}))
	t.Cleanup(agent.Close)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	base := "http://" + addr
	cmd := exec.Command(binary, "-gate=false")
	cmd.Dir = root
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"CELLHIVE_ROOT_KEY=" + rootKey,
		"CELLHIVE_CELL_URL=" + agent.URL,
		"CELLHIVE_DO_ADDR=" + addr,
		"CELLHIVE_DO_ADVERTISE=" + base,
		"CELLHIVE_DO_NODE=launcher-test",
		"CELLHIVE_DATA_DIR=" + filepath.Join(dir, "data"),
		"CELLHIVE_RUNTIME_DIR=" + filepath.Join(dir, "runtime"),
		"CELLHIVE_DO_RUNTIME_JS=" + filepath.Join(root, "workerd/do-runtime"),
		"CELLHIVE_WORKERD=" + workerd,
		"CELLHIVE_TENANT_LOG_CAPTURE=off",
	}
	// A private process group lets failure cleanup kill both launcher and workerd.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	logPath := filepath.Join(dir, "launcher.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logFile.Close() })
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("launcher did not stop after SIGTERM")
		}
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
		if t.Failed() {
			if out, err := os.ReadFile(logPath); err == nil {
				t.Logf("launcher output:\n%s", out)
			}
		}
	})
	transport := &http.Transport{Proxy: nil}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case <-done:
			t.Fatal("launcher exited before becoming healthy")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("launcher did not become healthy")
		}
		time.Sleep(100 * time.Millisecond)
	}
	token, err := bearertoken.MintInternal(creds.Internal, "internal", "launcher-test", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/v1/do/invoke", strings.NewReader(`{"namespace":"launcher-test","worker":"worker","bundle_sha":"test-bundle","storage_id":"launcher_storage","class":"Tenant","id":"one","request":{"method":"GET","path":"/"}}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "launcher-binding-ok" || verified.Load() != 1 {
		t.Fatalf("DO binding call: status=%d body=%q verified KV calls=%d", resp.StatusCode, body, verified.Load())
	}
}
