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

func TestHotspotCreatesDedicatedChildAndColdPeerStaysShared(t *testing.T) {
	bin, err := userruntime.FindWorkerd()
	if err != nil {
		t.Skip(err)
	}
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/control/host":
			worker := "hot"
			if r.URL.Query().Get("host") == "cold.test" {
				worker = "cold"
			}
			json.NewEncoder(w).Encode(control.HostView{Routes: []control.HostRoute{{Namespace: "n", Worker: worker, Path: "/"}}, Workers: map[string]control.HostWorkerPtr{"n/" + worker: {Version: 1, BundleSHA: worker}}})
		case "/v1/control/worker":
			json.NewEncoder(w).Encode(map[string]any{"version": 1, "bundle_sha": r.URL.Query().Get("worker")})
		case "/v1/internal/bundle-url":
			w.WriteHeader(501)
		case "/v1/internal/bundle":
			io.WriteString(w, `export default {fetch(){return new Response('ok')}}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer agent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, err := NewManager(ctx, bin, t.TempDir(), userruntime.Config{CellURL: agent.URL, CellToken: "internal", DispatchToken: "dispatch", PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js", DisableTenantLogs: true}, workerdpool.Config{MaxSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := m.Close(c); err != nil {
			t.Error(err)
		}
	}()
	router := NewRouter(agent.URL, "internal", "dispatch")
	defer router.Close()
	proxy := workerdpool.NewProxy(m.Supervisor, router.Public, m.Acquire)
	defer proxy.Close()
	server := httptest.NewServer(proxy)
	defer server.Close()
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	get := func(host string) {
		req, _ := http.NewRequest("GET", server.URL, nil)
		req.Host = host
		r, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != 200 || string(b) != "ok" {
			t.Fatalf("request %s=%d %q", host, r.StatusCode, b)
		}
	}
	var dedicated workerdpool.Slot
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		get("hot.test")
		time.Sleep(10 * time.Millisecond)
		for _, s := range m.Supervisor.Pool.Slots() {
			if s.Mode == workerdpool.Dedicated && s.Key.Worker == "hot" && s.Ready {
				dedicated = s
				break
			}
		}
		if dedicated.ID != "" {
			break
		}
	}
	if dedicated.ID == "" {
		t.Fatal("sustained traffic did not promote hot function")
	}
	get("cold.test")
	lease, ok := m.Supervisor.Pool.Select(workerdpool.FunctionKey{Namespace: "n", Worker: "hot", Version: 1, BundleSHA: "hot"}, time.Now())
	if !ok || lease.ID != dedicated.ID {
		t.Fatal("hot requests did not switch to dedicated generation")
	}
	m.Supervisor.Pool.Release(lease.ID, lease.Generation, time.Now())
	lease, ok = m.Supervisor.Pool.Select(workerdpool.FunctionKey{Namespace: "n", Worker: "cold", Version: 1, BundleSHA: "cold"}, time.Now())
	if !ok || lease.Mode != workerdpool.Shared {
		t.Fatal("cold peer used hot dedicated slot")
	}
	m.Supervisor.Pool.Release(lease.ID, lease.Generation, time.Now())
	// Cold samples eventually demote the hot process; shared remains available.
	deadline = time.Now().Add(40 * time.Second)
	for {
		found := false
		for _, s := range m.Supervisor.Pool.Slots() {
			if s.ID == dedicated.ID {
				found = true
			}
		}
		if !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle dedicated slot not demoted")
		}
		time.Sleep(50 * time.Millisecond)
	}
	get("cold.test")
	get("hot.test")
}
