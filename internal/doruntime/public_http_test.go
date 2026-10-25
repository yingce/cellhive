package doruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"

	"cellhive/internal/userruntime"
)

// Real user-runtime -> shared DO binding -> forwarding do-runtime -> owning
// do-runtime -> native facet. The control/lease service is a local test stub;
// cell-agent's HTTP proxy is separately exercised by TestDOProxyPreservesHTTPResponse.
func TestPublicDOHTTPRoundTrip(t *testing.T) {
	owner, stopOwner := startRuntimeWithBundle(t, &ownerStub{live: map[string]bool{}}, httpRegressionTenant)
	defer stopOwner()
	forward, stopForward := startRuntimeWithBundle(t, &ownerStub{forwardTo: owner}, httpRegressionTenant)
	defer stopForward()
	target, _ := url.Parse(forward)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Director = func(r *http.Request) {
		r.URL.Scheme = target.Scheme
		r.URL.Host = target.Host
		r.Header.Set("x-cellhive-internal-token", "tok")
	}
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes":
			json.NewEncoder(w).Encode(map[string]any{})
		case "/v1/control/host":
			json.NewEncoder(w).Encode(map[string]any{"routes": []any{map[string]any{"ns": "demo", "worker": "web"}}, "workers": map[string]any{"demo/web": map[string]any{"version": 1, "bundle_sha": "web", "storage_id": "web-1"}}})
		case "/v1/control/worker":
			json.NewEncoder(w).Encode(map[string]any{"version": 1, "bundle_sha": "web", "storage_id": "web-1", "bindings": []any{map[string]any{"type": "do", "name": "ROOM", "id": "Review"}}})
		case "/v1/internal/bundle":
			io.WriteString(w, `export default {async fetch(req,env){return env.ROOM.get("one").fetch(req)}};`)
		case "/v1/do/invoke":
			proxy.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer agent.Close()
	port := freePort(t)
	cfg, err := userruntime.Render(t.TempDir(), userruntime.Config{CellURL: agent.URL, CellToken: "tok", ScopeSecret: "test-scope-secret", InternalPort: freePort(t), PublicPort: port, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js"})
	if err != nil {
		t.Fatal(err)
	}
	bin, err := FindWorkerd()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- userruntime.Run(ctx, bin, cfg) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("user-runtime exit timeout")
		}
	}()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := noProxyClient()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	for deadline := time.Now().Add(25 * time.Second); ; {
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
	for _, path := range []string{"/echo", "/binary", "/headers", "/empty", "/redirect", "/failed", "/delete"} {
		t.Run(path, func(t *testing.T) {
			method := "GET"
			var payload []byte
			if path == "/echo" {
				method = "POST"
				payload = []byte{0, 255, 128, 65}
			}
			req, _ := http.NewRequest(method, base+path, bytes.NewReader(payload))
			req.Host = "public-do.test"
			res, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}
			status := 200
			switch path {
			case "/empty":
				status = 204
			case "/redirect":
				status = 302
			case "/failed":
				status = 418
			}
			if res.StatusCode != status {
				t.Fatalf("status=%d body=%s", res.StatusCode, body)
			}
			switch path {
			case "/echo", "/binary", "/failed":
				if !bytes.Equal(body, []byte{0, 255, 128, 65}) {
					t.Fatalf("changed bytes: %x", body)
				}
			case "/headers":
				if res.Header.Get("X-Review") != "kept" || res.Header.Get("Location") != "/next" || len(res.Header.Values("Set-Cookie")) != 2 {
					t.Fatalf("headers lost: %v", res.Header)
				}
			case "/redirect":
				if res.Header.Get("Location") != "/must-not-follow" {
					t.Fatal("redirect lost")
				}
			case "/delete":
				if string(body) != "0" {
					t.Fatalf("keys remain: %s", body)
				}
			}
			if res.Header.Get("X-Cellhive-Do-App") != "" || res.Header.Get("X-Cellhive-Internal-Token") != "" {
				t.Fatal("platform response marker leaked")
			}
		})
	}
}
