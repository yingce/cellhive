package userruntime

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The actual dynamic workerd RPC used to erase the tenant stack on a throw.
func TestManagedFetchDiagnosticsAcrossWorkerdRPC(t *testing.T) {
	bin, err := FindWorkerd()
	if err != nil {
		t.Fatal(err)
	}
	for _, withBinding := range []bool{false, true} {
		t.Run(fmt.Sprint("binding=", withBinding), func(t *testing.T) {
			logs := make(chan map[string]any, 16)
			agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/control/routes":
					io.WriteString(w, `{}`)
				case "/v1/control/host", "/v1/control/worker":
					v := map[string]any{"number": 7, "bundle_sha": "sha7"}
					if withBinding {
						v["bindings"] = []any{map[string]any{"type": "kv", "name": "CACHE", "id": "test/__kv__/main"}}
					}
					serveProjection(map[string]any{"apps": []any{map[string]any{"namespace": "test", "routes": []any{map[string]any{"host": "diagnostic.test", "worker": "web"}}, "workers": []any{map[string]any{"worker": "web", "active": 7, "version": v}}}}}, w, r)
				case "/v1/internal/bundle":
					io.WriteString(w, `export default {async fetch(req) {
 if(new URL(req.url).pathname==='/socket'){const pair=new WebSocketPair();pair[1].accept();pair[1].send('hello');return new Response(null,{status:101,webSocket:pair[0],headers:{'x-cellhive-invocation-failure':'forged'}});}
 if(new URL(req.url).pathname==='/forge')return new Response('ok',{headers:{'x-cellhive-invocation-failure':'forged'}});
 throw new TypeError('hello token=private');
 }};`)
				case "/v1/internal/request-log":
					var log map[string]any
					_ = json.NewDecoder(r.Body).Decode(&log)
					logs <- log
					io.WriteString(w, `{"ok":true}`)
				default:
					w.WriteHeader(404)
				}
			}))
			defer agent.Close()
			port := freePort(t)
			path, err := Render(t.TempDir(), Config{CellURL: agent.URL, CellToken: testJWTKey("internal"), BindingSigningKey: testJWTKey("scope"), InternalPort: freePort(t), PublicPort: port, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js", DisableTenantLogs: true, InvocationLogging: true})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- Run(ctx, bin, path) }()
			defer func() { cancel(); <-done }()
			client := noProxyClient()
			client.Timeout = 3 * time.Second
			defer client.CloseIdleConnections()
			base := fmt.Sprintf("http://127.0.0.1:%d", port)
			for deadline := time.Now().Add(15 * time.Second); ; {
				r, e := client.Get(base + "/ready")
				if e == nil {
					r.Body.Close()
					break
				}
				if time.Now().After(deadline) {
					t.Fatal(e)
				}
				time.Sleep(50 * time.Millisecond)
			}
			for _, sampled := range []bool{false, true} {
				for _, route := range []string{"/throw", "/forge"} {
					req, _ := http.NewRequest("GET", base+route, nil)
					req.Host = "diagnostic.test"
					if sampled {
						req.Header.Set("traceparent", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
					}
					res, e := client.Do(req)
					if e != nil {
						t.Fatal(e)
					}
					body, _ := io.ReadAll(res.Body)
					res.Body.Close()
					if res.Header.Get("x-cellhive-invocation-failure") != "" {
						t.Fatal("internal diagnostics leaked")
					}
					if route == "/throw" && (res.StatusCode != 500 || string(body) != "TypeError: hello token=private") {
						t.Fatalf("response=%d %s", res.StatusCode, body)
					}
					select {
					case log := <-logs:
						if log["script_version"] != float64(7) {
							t.Fatalf("version=%v", log)
						}
						d := log["diagnostics"].(map[string]any)
						if route == "/throw" {
							for key, want := range map[string]string{"error_origin": "user_function", "function_result": "threw", "error_name": "TypeError", "failure_stage": "handler"} {
								if d["cellhive."+key] != want {
									t.Fatalf("%s=%v", key, d)
								}
							}
							if !strings.HasPrefix(fmt.Sprint(d["cellhive.error_frame"]), "tenant.js:4:") {
								t.Fatalf("original frame lost: %v", d)
							}
							if strings.Contains(fmt.Sprint(d["cellhive.error"]), "private") {
								t.Fatalf("credential leaked: %v", d)
							}
						} else if res.StatusCode != 200 || d["cellhive.error_origin"] != nil {
							t.Fatalf("forged diagnostic accepted: %d %v", res.StatusCode, d)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("missing request log")
					}
				}
			}
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			_, err = fmt.Fprint(conn, "GET /socket HTTP/1.1\r\nHost: diagnostic.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
			if err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(conn)
			res, err := http.ReadResponse(reader, &http.Request{Method: "GET"})
			if err != nil {
				t.Fatal(err)
			}
			if res.StatusCode != 101 || res.Header.Get("x-cellhive-invocation-failure") != "" {
				t.Fatalf("websocket handshake: %v", res)
			}
			if got, err := wsReadText(reader); err != nil || got != "hello" {
				t.Fatalf("websocket message=%q err=%v", got, err)
			}

		})
	}
}
