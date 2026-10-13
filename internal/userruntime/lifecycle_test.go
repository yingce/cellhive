package userruntime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

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
			path, err := Render(t.TempDir(), Config{CellURL: agent.URL, CellToken: "tok", InternalPort: freePort(t), PublicPort: port, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js", DisableTenantLogs: true, OutboundAllow: []string{"public", "private", "local"}})
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
