package userruntimepool

import (
	"cellhive/internal/control"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
)

func TestIncarnationPinSurvivesConnectionNominations(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-cellhive-pool-storage-id"); got != "new" {
			t.Errorf("wire storage_id=%q", got)
		}
		if got := r.Header.Get("x-cellhive-internal-token"); got != "internal" {
			t.Errorf("wire auth=%q", got)
		}
		w.WriteHeader(200)
	}))
	defer backend.Close()
	target, _ := url.Parse(backend.URL)
	proxy := httputil.NewSingleHostReverseProxy(target)
	req := httptest.NewRequest("GET", "http://app.test/", nil)
	req.Header.Add("Connection", "keep-alive, X-Cellhive-Pool-Storage-Id")
	req.Header.Add("Connection", "x-cellhive-internal-token")
	router := NewRouter("http://unused", "internal", "dispatch")
	view := control.HostView{Routes: []control.HostRoute{{Namespace: "n", Worker: "api"}}, Workers: map[string]control.HostWorkerPtr{"n/api": {Version: 1, BundleSHA: "same", StorageID: "new"}}}
	route, err := router.routeView(req, view)
	if err != nil {
		t.Fatal(err)
	}
	if err := route.PrepareRequest(req); err != nil {
		t.Fatal(err)
	}
	proxy.ServeHTTP(httptest.NewRecorder(), req)
}

func TestInternalRoutePreservesWorkerIncarnation(t *testing.T) {
	router := NewRouter("http://unused", "internal", "dispatch")
	req := httptest.NewRequest("POST", "http://internal/v1/queues/dispatch", strings.NewReader(`{"namespace":"n","worker":"api","version":1,"bundle_sha":"same","storage_id":"new"}`))
	req.Header.Set("x-cellhive-internal-token", "dispatch")
	route, err := router.Internal(req)
	if err != nil {
		t.Fatal(err)
	}
	if route.Key.StorageID != "new" {
		t.Fatalf("lost incarnation: %+v", route.Key)
	}
}

func TestPublicRoutePinsWorkerIncarnation(t *testing.T) {
	router := NewRouter("http://unused", "internal", "dispatch")
	req := httptest.NewRequest("GET", "http://app.test/", nil)
	var view control.HostView
	if err := json.Unmarshal([]byte(`{"routes":[{"ns":"n","worker":"api"}],"workers":{"n/api":{"version":1,"bundle_sha":"same","storage_id":"first"}}}`), &view); err != nil {
		t.Fatal(err)
	}
	first, err := router.routeView(req, view)
	if err != nil {
		t.Fatal(err)
	}
	out := req.Clone(req.Context())
	out.Header.Set("x-cellhive-pool-storage-id", "spoofed")
	if err := first.PrepareRequest(out); err != nil {
		t.Fatal(err)
	}
	if got := out.Header.Get("x-cellhive-pool-storage-id"); got != "first" {
		t.Errorf("incarnation header=%q, want first", got)
	}
	if err := json.Unmarshal([]byte(`{"routes":[{"ns":"n","worker":"api"}],"workers":{"n/api":{"version":1,"bundle_sha":"same","storage_id":"second"}}}`), &view); err != nil {
		t.Fatal(err)
	}
	second, err := router.routeView(req, view)
	if err != nil {
		t.Fatal(err)
	}
	if first.Key == second.Key {
		t.Error("recreated worker reused the same pool key")
	}
}
