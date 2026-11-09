package userruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

const fetchTailBundle = `export default {
  async fetch(req) {
    console.log("fetch-console-log");
    return new Response("ok");
  }
};`

func TestUserRuntimeFetchTailCapturesConsole(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/host":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"routes":  []map[string]any{{"ns": "demo", "worker": "web", "path": "/"}},
				"workers": map[string]any{"demo/web": map[string]any{"version": 1, "bundle_sha": "sha1"}},
			})
		case "/v1/control/worker":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"version": 1, "bundle_sha": "sha1", "vars": map[string]string{},
				"class_storage": map[string]any{}, "deleted_classes": []string{},
			})
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(fetchTailBundle))
		case "/v1/internal/worker/bindings":
			_, _ = w.Write([]byte(`{"bindings":{},"vars":{}}`))
		case "/v1/internal/log-capture":
			w.Header().Set("content-type", "application/json")
			_, _ = w.Write([]byte(`{"mode":"all","workers":[]}`))
		case "/v1/internal/request-log":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL: stub.URL, CellToken: testJWTKey("internal"), DispatchToken: testJWTKey("dispatch"),
		LogToken: testJWTKey("log"), BindingSigningKey: testJWTKey("scope"),
		InternalPort: internalPort, PublicPort: publicPort,
		PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()
	client := noProxyClient()
	base := fmt.Sprintf("http://127.0.0.1:%d", internalPort)
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	tailBase := public
	deadline := time.Now().Add(20 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime unhealthy: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	req, _ := http.NewRequest(http.MethodGet, public+"/", nil)
	req.Host = "demo.test"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fetch status %d", resp.StatusCode)
	}

	var since uint64
	var events int
	waitUntil := time.Now().Add(5 * time.Second)
	for events == 0 && time.Now().Before(waitUntil) {
		query, _ := http.NewRequest(http.MethodGet, tailBase+"/v1/internal/logs/events?ns=demo&worker=web&since="+strconv.FormatUint(since, 10), nil)
		query.Header.Set("Authorization", testJWTAuthorization("log"))
		res, err := client.Do(query)
		if err != nil {
			t.Fatalf("tail query: %v", err)
		}
		var page struct {
			Events []struct {
				Seq uint64 `json:"seq"`
			} `json:"events"`
		}
		_ = json.NewDecoder(res.Body).Decode(&page)
		res.Body.Close()
		for _, item := range page.Events {
			since = item.Seq
			events++
		}
		time.Sleep(100 * time.Millisecond)
	}
	if events == 0 {
		t.Fatal("fetch console log was not captured by Tail")
	}
}
