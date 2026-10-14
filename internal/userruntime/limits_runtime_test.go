package userruntime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestUserRuntimeEnforcesSubrequestLimit(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skip(err)
	}
	proj := map[string]any{"apps": []any{map[string]any{
		"namespace": "acme", "routes": []any{map[string]any{"host": "limits.test", "worker": "web"}},
		"workers": []any{map[string]any{"worker": "web", "active": 1, "version": map[string]any{
			"number": 1, "bundle_sha": "sha-limits", "limits": map[string]any{"subrequests": 1},
		}}},
	}}}
	bundle := `export default { async fetch() {
  let attempted=0;
  for (let i = 0; i < 2; i++) { try { await fetch("http://127.0.0.1:1/"); } catch (e) { if (e && e.code === "subrequest_limit_exceeded") return new Response(e.code+":"+attempted); } attempted++; }
  return new Response("limit-not-enforced");
} };`
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/routes", "/v1/control/host", "/v1/control/worker":
			serveProjection(proj, w, r)
		case "/v1/internal/bundle":
			io.WriteString(w, bundle)
		default:
			http.NotFound(w, r)
		}
	}))
	defer agent.Close()
	port := freePort(t)
	path, err := Render(t.TempDir(), Config{CellURL: agent.URL, CellToken: "tok", InternalPort: freePort(t), PublicPort: port, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js", DisableTenantLogs: true})
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
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodGet, base+"/", nil)
		req.Host = "limits.test"
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != "subrequest_limit_exceeded:1" {
			t.Fatalf("status=%d body=%q", resp.StatusCode, body)
		}
	}
}
