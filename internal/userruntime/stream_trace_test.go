package userruntime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// Real workerd RPC boundaries: the upstream waits for the client to observe
// the first chunk, and request A resumes only after request B has completed.
func TestUserRuntimeStreamingAndConcurrentTrace(t *testing.T) {
	workerd, err := FindWorkerd()
	if err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme", "routes": []any{map[string]any{"host": "app.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{"number": 1, "bundle_sha": "sha-stream-trace", "limits": map[string]any{"subrequests": 10}, "bindings": []any{map[string]any{"type": "kv", "name": "KV", "id": "acme/__kv__/main"}, map[string]any{"type": "r2", "name": "BUCKET", "id": "acme/bucket"}}}}},
	}}}
	const tenant = `import { env as importedEnv } from "cloudflare:workers";
const capturedBucket = importedEnv.BUCKET;
export default {async fetch(req,env){const path=new URL(req.url).pathname;
 if(path==='/stream'){const obj=await env.BUCKET.get('stream');return new Response(obj.body);}
 if(path==='/consume'){const obj=await capturedBucket.get('small');const first=await obj.text();let rejected=false;try{await obj.arrayBuffer()}catch{rejected=true}return Response.json({first,used:obj.bodyUsed,rejected});}
 if(path==='/a')await env.KV.get('hold');
 if(path==='/a'||path==='/b')await env.KV.get(path.slice(1));
 return new Response('ok');}};`
	releaseStream, releaseA, enteredA := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var streamOnce, aOnce sync.Once
	defer streamOnce.Do(func() { close(releaseStream) })
	defer aOnce.Do(func() { close(releaseA) })
	var mu sync.Mutex
	traces := map[string]string{}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			io.WriteString(w, tenant)
		case "/v1/r2/object":
			if r.URL.Query().Get("key") == "small" {
				io.WriteString(w, "small")
				return
			}
			io.WriteString(w, "first")
			w.(http.Flusher).Flush()
			select {
			case <-releaseStream:
				io.WriteString(w, "last")
			case <-r.Context().Done():
			}
		case "/v1/kv/get":
			key := r.URL.Query().Get("key")
			if key == "hold" {
				close(enteredA)
				select {
				case <-releaseA:
				case <-r.Context().Done():
					return
				}
			}
			mu.Lock()
			traces[key] = r.Header.Get("traceparent")
			mu.Unlock()
			io.WriteString(w, "v")
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()
	port := freePort(t)
	path, err := Render(t.TempDir(), Config{CellURL: stub.URL, CellToken: testJWTKey("internal"), BindingSigningKey: testJWTKey("scope"), InternalPort: freePort(t), PublicPort: port, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, path) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("workerd did not exit")
		}
	}()
	client := noProxyClient()
	client.Timeout = 5 * time.Second
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(20 * time.Second)
	for {
		r, e := client.Get(base + "/healthz")
		if e == nil {
			r.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(e)
		}
		time.Sleep(50 * time.Millisecond)
	}
	request := func(path, tp string) (*http.Response, error) {
		req, _ := http.NewRequest("GET", base+path, nil)
		req.Host = "app.test"
		req.Header.Set("traceparent", tp)
		return client.Do(req)
	}
	const traceA = "00-11111111111111111111111111111111-1111111111111111-01"
	const traceB = "00-22222222222222222222222222222222-2222222222222222-01"
	// Warm up and check local, single-use R2 consumption after stream transfer.
	r, err := request("/consume", traceA)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if string(b) != `{"first":"small","used":true,"rejected":true}` {
		t.Fatalf("R2 consumption: %d %s", r.StatusCode, b)
	}
	r, err = request("/stream", traceA)
	if err != nil {
		t.Fatalf("response waited for upstream EOF: %v", err)
	}
	first := make([]byte, 5)
	_, err = io.ReadFull(r.Body, first)
	if err != nil || string(first) != "first" {
		r.Body.Close()
		t.Fatalf("first chunk %q: %v", first, err)
	}
	streamOnce.Do(func() { close(releaseStream) })
	rest, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil || string(rest) != "last" {
		t.Fatalf("remaining stream %q: %v", rest, err)
	}
	aDone := make(chan error, 1)
	go func() {
		r, e := request("/a", traceA)
		if e == nil {
			_, e = io.Copy(io.Discard, r.Body)
			r.Body.Close()
			if r.StatusCode != 200 {
				e = fmt.Errorf("A status %d", r.StatusCode)
			}
		}
		aDone <- e
	}()
	select {
	case <-enteredA:
	case <-time.After(5 * time.Second):
		t.Fatal("A did not reach wait")
	}
	r, err = request("/b", traceB)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatalf("B status %d", r.StatusCode)
	}
	aOnce.Do(func() { close(releaseA) })
	if err = <-aDone; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if traces["a"] != traceA || traces["b"] != traceB {
		t.Fatalf("crossed trace: %v", traces)
	}
}
