package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/workerdpool"
)

func TestServeAdaptiveLogEventsUsesFirstNonEmptySlot(t *testing.T) {
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"session":"empty","events":[]}`))
	}))
	defer empty.Close()
	events := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("authorization") != "Bearer log-token" {
			t.Fatalf("authorization = %q", r.Header.Get("authorization"))
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"session":"slot-2","events":[{"seq":1,"event":{"outcome":"ok","logs":[{"message":["hello"],"level":"log"}]}}]}`))
	}))
	defer events.Close()
	req := httptest.NewRequest(http.MethodGet, "http://runtime:8088/v1/internal/logs/events?ns=vwork-1&worker=web&since=0", nil)
	req.Header.Set("authorization", "Bearer log-token")
	rec := httptest.NewRecorder()
	serveAdaptiveLogEvents(rec, req, []*workerdpool.Handle{{PublicURL: empty.URL}, {PublicURL: events.URL}}, &http.Client{Timeout: time.Second})
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"session":"slot-2"`) || !strings.Contains(rec.Body.String(), `"hello"`) {
		t.Fatalf("response = %d %s", rec.Code, rec.Body.String())
	}
}

func TestServeAdaptiveLogEventsEmptySlots(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"session":"","events":[]}`))
	}))
	defer srv.Close()
	req := httptest.NewRequest(http.MethodGet, "http://runtime:8088/v1/internal/logs/events?ns=vwork-1&worker=web&since=0", nil)
	rec := httptest.NewRecorder()
	serveAdaptiveLogEvents(rec, req, []*workerdpool.Handle{{InternalURL: srv.URL}}, &http.Client{Timeout: time.Second})
	if rec.Body.String() != `{"session":"","events":[]}` {
		t.Fatalf("response = %s", rec.Body.String())
	}
}
