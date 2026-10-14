package userruntimepool

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/userruntime"
	"cellhive/internal/workerdpool"
)

func TestManagerProxyRealPinnedWorkerAndProcessReplacement(t *testing.T) {
	bin, err := userruntime.FindWorkerd()
	if err != nil {
		t.Skip(err)
	}
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/host":
			json.NewEncoder(w).Encode(control.HostView{Routes: []control.HostRoute{{Namespace: "n", Worker: "api", Path: "/api"}}, Workers: map[string]control.HostWorkerPtr{"n/api": {Version: 1, BundleSHA: "sha"}}})
		case "/v1/control/worker":
			json.NewEncoder(w).Encode(map[string]any{"version": 1, "bundle_sha": "sha"})
		case "/v1/internal/bundle-url":
			w.WriteHeader(501)
		case "/v1/internal/bundle":
			io.WriteString(w, `export default {fetch(r){return new Response('ok:'+new URL(r.url).pathname)}}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer agent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, err := NewManager(ctx, bin, t.TempDir(), userruntime.Config{CellURL: agent.URL, CellToken: "internal", DispatchToken: "dispatch", PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js", DisableTenantLogs: true}, workerdpool.Config{MaxSlots: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.Close(closeCtx); err != nil {
			t.Error(err)
		}
	}()
	router := NewRouter(agent.URL, "internal", "dispatch")
	defer router.Close()
	proxy := workerdpool.NewProxy(m.Supervisor, router.Public, m.Acquire)
	defer proxy.Close()
	server := httptest.NewServer(proxy)
	defer server.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	get := func() {
		r, err := client.Get(server.URL + "/api/users")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != 200 || string(body) != "ok:/users" {
			t.Fatalf("response=%d %q", r.StatusCode, body)
		}
	}
	get()
	old := m.Supervisor.Pool.Slots()[0]
	if err = m.Supervisor.KillGeneration(old.ID, old.Generation); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(m.Supervisor.Pool.Slots()) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("old process retained")
		}
		time.Sleep(time.Millisecond)
	}
	get()
	next := m.Supervisor.Pool.Slots()[0]
	if next.Generation <= old.Generation {
		t.Fatal("stale generation reused")
	}
}
