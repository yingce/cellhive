package doruntime

import (
	"cellhive/internal/runtimelog"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDOFileLogsAndNonBlockingSpans(t *testing.T) {
	bin, err := FindWorkerd()
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/traces" {
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
	}))
	defer collector.Close()
	defer close(release)
	dir := t.TempDir()
	spool, err := runtimelog.OpenSpool(runtimelog.SpoolConfig{Dir: dir, Stream: "do-test", Endpoint: collector.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = spool.Close(ctx)
	}()
	o := &ownerStub{}
	stub := httptest.NewServer(o.handler(doLogTailTenant))
	defer stub.Close()
	port := freePort(t)
	cfg := Config{LogSpool: spool, CellURL: stub.URL, CellToken: testJWTKey("internal"), BindingSigningKey: testJWTKey("scope"), Addr: fmt.Sprintf("*:%d", port), DiskDir: t.TempDir(), PlatformJS: "../../workerd/do-runtime", NodeID: "file-test", PreventEviction: true, TailCapture: "all"}
	path, err := Render(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	env, err := WorkerdEnv(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(runtimelog.WithSpool(context.Background(), spool))
	done := make(chan error, 1)
	go func() { done <- Run(ctx, bin, path, env) }()
	defer func() { cancel(); <-done }()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := noProxyClient()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r, e := client.Get(base + "/healthz")
		if e == nil {
			r.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(e)
		}
		time.Sleep(20 * time.Millisecond)
	}
	result := make(chan int, 1)
	go func() {
		r, _ := invokeSpec(t, base, map[string]any{"namespace": "demo", "worker": "logs", "bundle_sha": "sha1", "class": "Tenant", "id": "one", "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01", "request": map[string]any{"path": "/"}})
		result <- r.StatusCode
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("span export not exercised")
	}
	select {
	case status := <-result:
		if status != 200 {
			t.Fatal(status)
		}
	case <-time.After(time.Second):
		t.Fatal("DO response waited on telemetry")
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		found := false
		paths, _ := filepath.Glob(filepath.Join(dir, "*.ndjson"))
		for _, p := range paths {
			b, _ := os.ReadFile(p)
			for _, line := range strings.Split(string(b), "\n") {
				var x struct {
					Kind, Message string
					Attributes    map[string]any
				}
				if json.Unmarshal([]byte(line), &x) == nil && x.Kind == "log" && x.Message == "do-tail-line" && x.Attributes["cellhive.namespace"] == "demo" {
					found = true
				}
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("trusted DO console record missing")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
