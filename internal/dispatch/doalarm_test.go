package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/timer"
)

type countingDispatcher struct{ n int }

func (c *countingDispatcher) Dispatch(context.Context, timer.Timer) error { c.n++; return nil }

func TestDoAlarmDispatcherRoutesAlarms(t *testing.T) {
	var gotPath, gotToken string
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotToken = r.Header.Get("x-cellhive-internal-token")
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	next := &countingDispatcher{}
	d := &DoAlarmDispatcher{
		Next:  next,
		Token: "tok",
		Resolve: func(context.Context, cell.Scope) (cell.Owner, string, error) {
			return cell.Owner{Node: "do-1", Address: srv.URL}, "", nil
		},
		Bundle: func(context.Context, string, string) (string, int, bool) { return "sha1", 3, true },
	}
	tm := timer.New(1234, timer.KindDOAlarm, "demo/__timer__/do", `{"w":"w","c":"C","s":0,"sc":"C","sid":"ds_1","id":"a1"}`)
	if err := d.Dispatch(context.Background(), tm); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if gotPath != "/v1/do/invoke" || gotToken != "tok" {
		t.Fatalf("path=%q token=%q", gotPath, gotToken)
	}
	if got["kind"] != "alarm" || got["worker"] != "w" || got["class"] != "C" || got["id"] != "a1" ||
		got["shard"] != float64(0) || got["bundle_sha"] != "sha1" || got["namespace"] != "demo" ||
		got["storage_id"] != "ds_1" || got["storage_class"] != "C" {
		t.Fatalf("body = %+v", got)
	}
	if next.n != 0 {
		t.Fatalf("next should not be called for alarms")
	}

	// Other kinds pass through.
	if err := d.Dispatch(context.Background(), timer.New(1, timer.KindCron, "demo/__cron__/w", "s")); err != nil {
		t.Fatalf("pass-through: %v", err)
	}
	if next.n != 1 {
		t.Fatalf("next.n = %d, want 1", next.n)
	}
}

func TestDoAlarmDispatcherAddsScheme(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	d := &DoAlarmDispatcher{
		Next:  &countingDispatcher{},
		Token: "tok",
		Resolve: func(context.Context, cell.Scope) (cell.Owner, string, error) {
			return cell.Owner{Node: "do-1", Address: strings.TrimPrefix(srv.URL, "http://")}, "", nil
		},
		Bundle: func(context.Context, string, string) (string, int, bool) { return "sha1", 1, true },
	}
	if err := d.Dispatch(context.Background(), timer.New(1, timer.KindDOAlarm, "demo/__timer__/do", `{"w":"w","c":"C","s":0,"sc":"C","sid":"ds_1","id":"a1"}`)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if gotPath != "/v1/do/invoke" {
		t.Fatalf("path = %q, want /v1/do/invoke", gotPath)
	}
}

func TestDoAlarmDispatcherErrors(t *testing.T) {
	d := &DoAlarmDispatcher{
		Next:  &countingDispatcher{},
		Token: "t",
		Resolve: func(context.Context, cell.Scope) (cell.Owner, string, error) {
			return cell.Owner{}, "", errors.New("boom")
		},
		Bundle: func(context.Context, string, string) (string, int, bool) { return "s", 1, true },
	}
	if err := d.Dispatch(context.Background(), timer.New(1, timer.KindDOAlarm, "demo/__timer__/do", `{"w":"w","c":"C","s":0,"sc":"C","sid":"ds_1","id":"a1"}`)); err == nil {
		t.Fatal("expected resolve error")
	}
}
