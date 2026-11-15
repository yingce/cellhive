package userruntime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Only readiness probes run between the first request and the next cold load.
func TestPublicReadyReclaimsIdleWorker(t *testing.T) {
	bin, err := FindWorkerd()
	if err != nil {
		t.Skip(err)
	}
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes":
			io.WriteString(w, `{}`)
		case "/v1/control/host", "/v1/control/worker":
			proj := map[string]any{"apps": []any{map[string]any{"namespace": "test", "routes": []any{map[string]any{"host": "idle.test", "worker": "web"}}, "workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{"number": 1, "bundle_sha": "sha1"}}}}}}
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			io.WriteString(w, `let n=0; export default {fetch(){return new Response(String(++n));}};`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer agent.Close()
	port := freePort(t)
	path, err := Render(t.TempDir(), Config{CellURL: agent.URL, CellToken: testJWTKey("internal"), BindingSigningKey: testJWTKey("scope"), InternalPort: freePort(t), PublicPort: port, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js", DisableTenantLogs: true})
	if err != nil {
		t.Fatal(err)
	}
	// Shorten only this temporary runtime's idle TTL, not the production default.
	config, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	configText := strings.Replace(string(config), `(name = "CH_ISOLATE_MAX",`, `(name = "CH_ISOLATE_IDLE_MS", text = "100"), (name = "CH_ISOLATE_MAX",`, 1)
	if err := os.WriteFile(path, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, bin, path) }()
	defer func() { cancel(); <-done }()
	client := noProxyClient()
	client.Timeout = 2 * time.Second
	defer client.CloseIdleConnections()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	for deadline := time.Now().Add(20 * time.Second); ; {
		r, err := client.Get(base + "/ready")
		if err == nil {
			r.Body.Close()
			if r.StatusCode != 200 {
				t.Fatalf("ready: %d", r.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	get := func(path string) string {
		req, _ := http.NewRequest("GET", base+path, nil)
		req.Host = "idle.test"
		r, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		b, err := io.ReadAll(r.Body)
		if err != nil || r.StatusCode != 200 {
			t.Fatalf("GET %s: status=%d body=%s err=%v", path, r.StatusCode, b, err)
		}
		return string(b)
	}
	if got := get("/"); got != "1" {
		t.Fatalf("initial counter=%s", got)
	}
	// Exceed both the idle TTL and scheduler throttle; no /healthz or tenant traffic.
	for i := 0; i < 3; i++ {
		time.Sleep(600 * time.Millisecond)
		get("/ready")
	}
	if got := get("/"); got != "1" {
		t.Fatalf("readiness did not retire idle isolate: counter=%s, want fresh counter=1", got)
	}
}

// v1 remains alive while its background task runs; v2 uses a new isolate.
// Revisiting v1 after retirement reconstructs its module-scoped counter.
func TestPublicWorkerVersionRetirement(t *testing.T) {
	for _, kind := range []string{"object", "class", "imported"} {
		t.Run(kind, func(t *testing.T) {
			if _, err := FindWorkerd(); err != nil {
				t.Skip(err)
			}
			var finished atomic.Bool
			body := `let n=0; export default {async fetch(req,env,ctx){
			  if(new URL(req.url).pathname==='/background') ctx.waitUntil(new Promise(r=>setTimeout(r,500)).then(()=>fetch("MARKER")));
			  return new Response(String(++n)); }};`
			if kind == "class" {
				body = `import {WorkerEntrypoint} from 'cloudflare:workers';let n=0;export default class extends WorkerEntrypoint {async fetch(req){if(new URL(req.url).pathname==='/background')this.ctx.waitUntil(new Promise(r=>setTimeout(r,500)).then(()=>fetch("MARKER")));return new Response(String(++n));}}`
			}
			if kind == "imported" {
				body = strings.Replace(body, "let n=0;", "import {waitUntil} from 'cloudflare:workers';let n=0;", 1)
				body = strings.Replace(body, "ctx.waitUntil(", "waitUntil(", 1)
			}
			agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/control/routes":
					io.WriteString(w, `{}`)
				case "/v1/control/host", "/v1/control/worker":
					v := 1
					if r.URL.Query().Get("host") == "v2.test" || r.URL.Query().Get("version") == "2" {
						v = 2
					}
					proj := map[string]any{"apps": []any{map[string]any{"namespace": "test", "routes": []any{map[string]any{"host": fmt.Sprintf("v%d.test", v), "worker": "web"}}, "workers": []any{map[string]any{"worker": "web", "active": v, "version": map[string]any{"number": v, "bundle_sha": fmt.Sprintf("sha%d", v)}}}}}}
					serveProjection(proj, w, r)
				case "/v1/internal/bundle":
					io.WriteString(w, strings.ReplaceAll(body, "MARKER", "http://marker.test/done"))
				default:
					w.WriteHeader(404)
				}
			}))
			defer agent.Close()
			// A service target on localhost cannot be reached through tenant's
			// public outbound; provide marker via a test-only outbound network.
			marker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { finished.Store(true); io.WriteString(w, "ok") }))
			defer marker.Close()
			body = strings.ReplaceAll(body, "MARKER", marker.URL)
			port := freePort(t)
			path, err := Render(t.TempDir(), Config{CellURL: agent.URL, CellToken: testJWTKey("internal"), BindingSigningKey: testJWTKey("scope"), InternalPort: freePort(t), PublicPort: port, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js", DisableTenantLogs: true, OutboundAllow: []string{"public", "private", "local"}})
			if err != nil {
				t.Fatal(err)
			}
			bin, _ := FindWorkerd()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- Run(ctx, bin, path) }()
			defer func() { cancel(); <-done }()
			client := noProxyClient()
			defer client.CloseIdleConnections()
			base := fmt.Sprintf("http://127.0.0.1:%d", port)
			for deadline := time.Now().Add(20 * time.Second); ; {
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
			get := func(host, path string) string {
				req, _ := http.NewRequest("GET", base+path, nil)
				req.Host = host
				r, e := client.Do(req)
				if e != nil {
					t.Fatal(e)
				}
				b, _ := io.ReadAll(r.Body)
				r.Body.Close()
				if r.StatusCode != 200 {
					t.Fatalf("%s: %d %s", host, r.StatusCode, b)
				}
				return string(b)
			}
			if s := get("v1.test", "/background"); s != "1" {
				t.Fatal(s)
			}
			if s := get("v2.test", "/"); s != "1" {
				t.Fatal(s)
			}
			if finished.Load() {
				t.Fatal("background test did not overlap version switch")
			}
			for deadline := time.Now().Add(5 * time.Second); !finished.Load(); {
				if time.Now().After(deadline) {
					t.Fatal("old version background was interrupted")
				}
				time.Sleep(20 * time.Millisecond)
			}
			time.Sleep(200 * time.Millisecond)
			if s := get("v1.test", "/"); s != "1" {
				t.Fatalf("old isolate retained: counter=%s", s)
			}
		})
	}
}
