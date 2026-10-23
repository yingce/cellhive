package userruntimepool

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"cellhive/internal/control"
)

func TestPublicRoutingPinsVersionAndStripsMount(t *testing.T) {
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-cellhive-internal-token") != "internal" {
			t.Error("missing control authentication")
		}
		if r.URL.Query().Get("host") != "app.test" {
			t.Errorf("host=%s", r.URL.Query().Get("host"))
		}
		json.NewEncoder(w).Encode(control.HostView{Routes: []control.HostRoute{{Namespace: "n", Worker: "web", Path: "/"}, {Namespace: "n", Worker: "api", Path: "/api"}}, Workers: map[string]control.HostWorkerPtr{"n/web": {Version: 1, BundleSHA: "web"}, "n/api": {Version: 3, BundleSHA: "api"}}})
	}))
	defer agent.Close()
	router := NewRouter(agent.URL, "internal", "dispatch")
	r := httptest.NewRequest("POST", "http://app.test/api/users?q=1", strings.NewReader("body"))
	r.Host = "APP.TEST:8081"
	r.Header.Set("x-cellhive-pool-worker", "attacker")
	route, err := router.Public(r)
	if err != nil {
		t.Fatal(err)
	}
	if route.Key.Worker != "api" || route.Key.Version != 3 || route.Key.BundleSHA != "api" {
		t.Fatalf("key=%+v", route.Key)
	}
	clone := r.Clone(r.Context())
	if err = route.PrepareRequest(clone); err != nil {
		t.Fatal(err)
	}
	if clone.URL.Path != "/users" || clone.URL.RawQuery != "q=1" {
		t.Fatalf("url=%s", clone.URL)
	}
	if clone.Header.Get("x-cellhive-pool-worker") != "api" || clone.Header.Get("x-cellhive-internal-token") != "internal" {
		t.Fatal("fixed identity not installed")
	}
	if r.URL.Path != "/api/users" {
		t.Fatal("resolver changed caller URL")
	}
}

func TestInternalDispatchAuthenticatesAndPreservesBody(t *testing.T) {
	router := NewRouter("http://unused", "internal", "dispatch")
	body := `{"namespace":"n","worker":"consumer","version":7,"bundle_sha":"sha","messages":[{"body":"hello"}]}`
	r := httptest.NewRequest("POST", "http://internal/v1/queues/dispatch", strings.NewReader(body))
	if _, err := router.Internal(r); err == nil {
		t.Fatal("unauthenticated dispatch routed")
	}
	r = httptest.NewRequest("POST", "http://internal/v1/queues/dispatch", strings.NewReader(body))
	r.Header.Set("x-cellhive-internal-token", "dispatch")
	route, err := router.Internal(r)
	if err != nil {
		t.Fatal(err)
	}
	if !route.Internal || route.Key.Version != 7 || route.Key.Worker != "consumer" {
		t.Fatalf("route=%+v", route)
	}
	got, _ := io.ReadAll(r.Body)
	if string(got) != body {
		t.Fatalf("dispatch body changed=%s", got)
	}
}

func TestMountPrefixMatchesSegments(t *testing.T) {
	if matches("/api", "/apix") || !matches("/api", "/api/users") || !matches("/api", "/api") {
		t.Fatal("incorrect mount match")
	}
}

func TestHostCacheIsBoundedAndDoesNotCacheErrorsAs404(t *testing.T) {
	r := NewRouter("http://unused", "internal", "dispatch")
	for i := 0; i < maxCachedHosts+50; i++ {
		r.mu.Lock()
		r.storeLocked("h"+strconv.Itoa(i)+".test", hostEntry{expires: time.Now().Add(time.Minute)})
		r.mu.Unlock()
	}
	r.mu.Lock()
	n := len(r.hosts)
	r.mu.Unlock()
	if n > maxCachedHosts {
		t.Fatalf("host cache grew to %d, want <= %d", n, maxCachedHosts)
	}

	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer agent.Close()
	errRouter := NewRouter(agent.URL, "internal", "dispatch")
	r1 := httptest.NewRequest("GET", "http://down.test/", nil)
	r1.Host = "down.test"
	if _, err := errRouter.Public(r1); err == nil {
		t.Fatal("first lookup unexpectedly succeeded")
	}
	r2 := httptest.NewRequest("GET", "http://down.test/", nil)
	r2.Host = "down.test"
	_, err := errRouter.Public(r2)
	if err == nil || strings.Contains(err.Error(), "host not registered") {
		t.Fatalf("cached failure was turned into a 404: %v", err)
	}
}
