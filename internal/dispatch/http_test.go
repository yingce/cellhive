package dispatch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"cellhive/internal/timer"
)

func TestHTTPDispatcherPostsTimer(t *testing.T) {
	var gotPath, gotToken string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotToken = r.Header.Get("x-cellhive-internal-token")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := NewHTTP(srv.URL, "tok")
	if d == nil {
		t.Fatalf("NewHTTP returned nil for a non-empty URL")
	}
	tm := timer.New(1234, timer.KindCron, "app/__cron__/web", "slot")
	tm.Worker, tm.BundleSHA, tm.Version = "web", "shaWeb", 4
	if err := d.Dispatch(context.Background(), tm); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if gotPath != "/v1/timers/dispatch" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotToken != "tok" {
		t.Fatalf("token header = %q", gotToken)
	}
	if gotBody["token"] != tm.Token || gotBody["kind"] != "cron" || gotBody["scope"] != tm.Scope {
		t.Fatalf("body = %+v", gotBody)
	}
	if gotBody["worker"] != "web" || gotBody["bundle_sha"] != "shaWeb" || gotBody["scheduled_time_ms"] != float64(1234) {
		t.Fatalf("target fields = %+v", gotBody)
	}
	if gotBody["version"] != float64(4) {
		t.Fatalf("version = %v, want 4 (ADR-127)", gotBody["version"])
	}
}

func TestHTTPDispatcherNilForEmptyURL(t *testing.T) {
	if NewHTTP("", "tok") != nil {
		t.Fatalf("empty URL must yield nil")
	}
}

func TestHTTPDispatcherErrorsOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	d := NewHTTP(srv.URL, "")
	tm := timer.New(1, timer.KindCron, "s", "o")
	if err := d.Dispatch(context.Background(), tm); err == nil {
		t.Fatalf("want error on non-2xx")
	}
}

// TestHTTPDispatcherSendsNamespace: the cron/scheduled dispatch body must carry
// the namespace derived from the timer scope, otherwise user-runtime's
// /v1/timers/dispatch rejects it with 400 and cron never fires (ADR-154).
func TestHTTPDispatcherSendsNamespace(t *testing.T) {
	got := make(chan map[string]any, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		got <- body
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	d := NewHTTP(srv.URL, "tok")
	if err := d.Dispatch(context.Background(), timer.Timer{
		Kind: "cron", Scope: "acme/__timer__/do", Worker: "web", BundleSHA: "sha", Version: 2,
		DueAtMs: 123, Occurrence: "oc",
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	b := <-got
	if b["namespace"] != "acme" {
		t.Fatalf("namespace = %v, want acme (from the scope)", b["namespace"])
	}
	if b["worker"] != "web" || b["bundle_sha"] != "sha" {
		t.Fatalf("target fields missing: %+v", b)
	}
}
