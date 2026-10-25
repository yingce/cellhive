package userruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise the actual workerd loader, including resident descriptors and vars,
// not only the extracted cache function. Each generation reuses version 1.
func TestUserRuntimeRecreatedWorkerIdentity(t *testing.T) {
	workerd, err := FindWorkerd()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	var generation atomic.Int32
	generation.Store(1)
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g := generation.Load()
		sha := "same"
		if g == 3 {
			sha = "changed"
		}
		switch r.URL.Path {
		case "/v1/control/routes":
			w.Header().Set("etag", fmt.Sprintf("g%d", g))
			_, _ = w.Write([]byte(`{}`))
		case "/v1/control/host":
			w.Header().Set("etag", fmt.Sprintf("g%d", g))
			json.NewEncoder(w).Encode(map[string]any{"routes": []any{map[string]any{"ns": "acme", "worker": "web"}}, "workers": map[string]any{"acme/web": map[string]any{"version": 1, "bundle_sha": sha, "storage_id": fmt.Sprintf("generation-%d", g)}}})
		case "/v1/control/worker":
			json.NewEncoder(w).Encode(map[string]any{"version": 1, "bundle_sha": sha, "storage_id": fmt.Sprintf("generation-%d", g), "vars": map[string]string{"VALUE": fmt.Sprintf("generation-%d", g)}})
		case "/v1/internal/bundle":
			if r.URL.Query().Get("sha") == "changed" {
				_, _ = w.Write([]byte(`export default {fetch(r,env){return new Response("new-code:"+env.VALUE)}};`))
			} else {
				_, _ = w.Write([]byte(`export default {fetch(r,env){return new Response(env.VALUE)}};`))
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()
	port := freePort(t)
	path, err := Render(t.TempDir(), Config{CellURL: stub.URL, CellToken: "tok", InternalPort: freePort(t), PublicPort: port, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js", HostCacheTTLMs: 25, RevPollMs: 25})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, path) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("workerd exit timeout")
		}
	}()
	client := noProxyClient()
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	for deadline := time.Now().Add(25 * time.Second); ; {
		r, e := client.Get(url + "/healthz")
		if e == nil {
			r.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(e)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for g := int32(1); g <= 3; g++ {
		generation.Store(g)
		want := fmt.Sprintf("generation-%d", g)
		if g == 3 {
			want = "new-code:" + want
		}
		var body string
		for deadline := time.Now().Add(3 * time.Second); ; {
			req, _ := http.NewRequest("GET", url+"/", nil)
			req.Host = "recreated.test"
			res, e := client.Do(req)
			if e != nil {
				t.Fatal(e)
			}
			data, _ := io.ReadAll(res.Body)
			res.Body.Close()
			body = string(data)
			if res.StatusCode == 200 && body == want {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("generation %d: got %q, want %q", g, body, want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}
