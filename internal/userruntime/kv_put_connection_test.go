package userruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// Exercise the actual workerd HTTP pool: an unread successful acknowledgement
// closes its connection even though the tenant has awaited KV.put().
func TestUserRuntimeKVPutReusesAgentConnections(t *testing.T) {
	workerd, err := FindWorkerd()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	projection := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme", "routes": []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "put-reuse", "bindings": []any{map[string]any{"type": "kv", "name": "KV", "id": "values"}},
		}}},
	}}}
	const bundle = `export default {async fetch(req, env) {await env.KV.put("key", "value");return new Response("ok");}};`
	var mu sync.Mutex
	peers := map[string]bool{}
	requests := 0
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(projection, w, r)
		case "/v1/internal/bundle":
			io.WriteString(w, bundle)
		case "/v1/kv/put":
			body, err := io.ReadAll(r.Body)
			if err != nil || string(body) != "value" {
				http.Error(w, "invalid value", 400)
				return
			}
			if _, err := verifyTestBindingJWT(r.Header.Get("Authorization")); err != nil {
				http.Error(w, "invalid auth", 403)
				return
			}
			mu.Lock()
			peers[r.RemoteAddr] = true
			requests++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "applied": true, "txid": 1})
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()
	internalPort, publicPort := freePort(t), freePort(t)
	capnp, err := Render(t.TempDir(), Config{CellURL: stub.URL, CellToken: testJWTKey("internal"), BindingSigningKey: testJWTKey("scope"), InternalPort: internalPort, PublicPort: publicPort, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, capnp) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("workerd did not exit")
		}
	}()
	client := noProxyClient()
	defer client.CloseIdleConnections()
	public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	deadline := time.Now().Add(25 * time.Second)
	for {
		res, err := client.Get(public + "/healthz")
		if err == nil {
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	const count = 16
	for i := 0; i < count; i++ {
		res, body := fetchHost(t, client, public, "/")
		if res.StatusCode != 200 || body != "ok" {
			t.Fatalf("request %d: status=%d body=%q", i, res.StatusCode, body)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != count {
		t.Fatalf("writes=%d, want %d", requests, count)
	}
	if len(peers) > 2 {
		t.Fatalf("%d sequential KV PUTs used %d agent connections; successful acknowledgement bodies must be consumed", requests, len(peers))
	}
}

func TestUserRuntimeAckReusesAgentConnections(t *testing.T) {
	workerd, err := FindWorkerd()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	for _, tc := range []struct {
		name, sha, bundle, path string
		bindings                []any
	}{
		{
			name: "kv-delete", sha: "delete-reuse", path: "/v1/kv/delete",
			bundle:   `export default {async fetch(req, env) {await env.KV.delete("key");return new Response("ok");}};`,
			bindings: []any{map[string]any{"type": "kv", "name": "KV", "id": "values"}},
		},
		{
			name: "r2-delete", sha: "r2-delete-reuse", path: "/v1/r2/object",
			bundle:   `export default {async fetch(req, env) {await env.BUCKET.delete("key");return new Response("ok");}};`,
			bindings: []any{map[string]any{"type": "r2", "name": "BUCKET", "id": "bucket"}},
		},
		{
			name: "r2-abort", sha: "r2-abort-reuse", path: "/v1/r2/multipart",
			bundle:   `export default {async fetch(req, env) {const u=await env.BUCKET.createMultipartUpload("key");await u.abort();return new Response("ok");}};`,
			bindings: []any{map[string]any{"type": "r2", "name": "BUCKET", "id": "bucket"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			projection := map[string]any{"apps": []any{map[string]any{
				"namespace": "acme", "routes": []any{map[string]any{"host": "app.test", "worker": "web"}},
				"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
					"number": 1, "bundle_sha": tc.sha, "bindings": tc.bindings,
				}}},
			}}}
			var mu sync.Mutex
			peers := map[string]bool{}
			requests := 0
			stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
					serveProjection(projection, w, r)
				case "/v1/internal/bundle":
					io.WriteString(w, tc.bundle)
				case "/v1/r2/multipart/create":
					if _, err := verifyTestBindingJWT(r.Header.Get("Authorization")); err != nil {
						http.Error(w, "invalid auth", 403)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(map[string]any{"upload_id": "u1"})
				default:
					if r.URL.Path != tc.path || r.Method != http.MethodDelete {
						http.NotFound(w, r)
						return
					}
					if _, err := verifyTestBindingJWT(r.Header.Get("Authorization")); err != nil {
						http.Error(w, "invalid auth", 403)
						return
					}
					mu.Lock()
					peers[r.RemoteAddr] = true
					requests++
					mu.Unlock()
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(map[string]any{"ok": true})
				}
			}))
			defer stub.Close()
			internalPort, publicPort := freePort(t), freePort(t)
			capnp, err := Render(t.TempDir(), Config{CellURL: stub.URL, CellToken: testJWTKey("internal"), BindingSigningKey: testJWTKey("scope"), InternalPort: internalPort, PublicPort: publicPort, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js"})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- Run(ctx, workerd, capnp) }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("workerd did not exit")
				}
			}()
			client := noProxyClient()
			defer client.CloseIdleConnections()
			public := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
			deadline := time.Now().Add(25 * time.Second)
			for {
				res, err := client.Get(public + "/healthz")
				if err == nil {
					io.Copy(io.Discard, res.Body)
					res.Body.Close()
					break
				}
				if time.Now().After(deadline) {
					t.Fatal(err)
				}
				time.Sleep(50 * time.Millisecond)
			}
			const count = 16
			for i := 0; i < count; i++ {
				res, body := fetchHost(t, client, public, "/")
				if res.StatusCode != 200 || body != "ok" {
					t.Fatalf("request %d: status=%d body=%q", i, res.StatusCode, body)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if requests != count {
				t.Fatalf("acks=%d, want %d", requests, count)
			}
			if len(peers) > 2 {
				t.Fatalf("%d sequential %s used %d agent connections; successful acknowledgement bodies must be consumed", requests, tc.name, len(peers))
			}
		})
	}
}
