package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"cellhive/internal/cellstore"
	"cellhive/internal/invocation"
	"cellhive/internal/logbuf"
	"cellhive/internal/telemetry"
	collectorlog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestInvocationPutExportsTiming(t *testing.T) {
	for _, status := range []string{"success", "failed"} {
		t.Run(status, func(t *testing.T) {
			var mu sync.Mutex
			var records []*logsv1.LogRecord
			sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/logs" {
					body, _ := io.ReadAll(r.Body)
					var req collectorlog.ExportLogsServiceRequest
					if err := proto.Unmarshal(body, &req); err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					defer mu.Unlock()
					for _, resource := range req.ResourceLogs {
						for _, scope := range resource.ScopeLogs {
							records = append(records, scope.LogRecords...)
						}
					}
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer sink.Close()
			tel, err := telemetry.New(t.Context(), telemetry.Config{Endpoint: sink.URL, Logs: "all"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
			s := newControlServer(t)
			cs, err := cellstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cs.Close() })
			s.Invocations = invocation.New(cs)
			record := invocation.Record{ID: "attempt-1", RequestID: "caller-request", LogicalID: "job-1", Namespace: "acme", Worker: "cron", Kind: "cron", Attempt: 1, StartedMs: 100, FinishedMs: 137, Status: status}
			encoded, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			response := do(t, s.Handler(), http.MethodPost, "/v1/internal/invocations?ns=acme", "tok", encoded)
			if response.Code != http.StatusOK {
				t.Fatalf("put = %d %s", response.Code, response.Body.String())
			}
			if err := tel.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(records) != 1 {
				t.Fatalf("got %d logs, want one completion", len(records))
			}
			rec := records[0]
			attrs := map[string]*commonv1.AnyValue{}
			for _, attr := range rec.Attributes {
				attrs[attr.Key] = attr.Value
			}
			for key, want := range map[string]int64{"cellhive.started_ms": 100, "cellhive.finished_ms": 137, "cellhive.duration_ms": 37} {
				if value, ok := attrs[key]; !ok || value.GetIntValue() != want {
					t.Errorf("%s = %v, want integer %d", key, value, want)
				}
			}
			if rec.TimeUnixNano != 137000000 {
				t.Errorf("timeUnixNano = %d", rec.TimeUnixNano)
			}
			if attrs["cellhive.request_id"].GetStringValue() != "caller-request" || attrs["cellhive.outcome"].GetStringValue() != status {
				t.Errorf("correlation/outcome changed: %v", attrs)
			}
		})
	}
}

func TestLogIngestExportsInvocationWithoutConsole(t *testing.T) {
	var exports []*collectorlog.ExportLogsServiceRequest
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/logs" {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read OTLP: %v", err)
			return
		}
		var req collectorlog.ExportLogsServiceRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			t.Errorf("decode OTLP: %v", err)
			return
		}
		exports = append(exports, &req)
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()
	tel, err := telemetry.New(t.Context(), telemetry.Config{Endpoint: sink.URL, Logs: "all"})
	if err != nil {
		t.Fatal(err)
	}
	s := newControlServer(t)
	s.Logs = logbuf.New(8, 2)
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	s.Invocations = invocation.New(cs)
	const event = `{"events":[{"outcome":"exception","exceptions":[{"name":"Error","message":"boom","timestamp":42}],"logs":[],"eventTimestamp":41,"event_type":"scheduled","scriptVersion":{"id":"v2"},"requestId":"req-42"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/logs?ns=acme&worker=cron", bytes.NewBufferString(event))
	resp := httptest.NewRecorder()
	s.handleLogIngest(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("ingest = %d %s", resp.Code, resp.Body.String())
	}
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	record := invocation.Record{ID: "attempt-1", RequestID: "attempt-1", Traceparent: traceparent, LogicalID: "message-1", Namespace: "acme", Worker: "cron", Kind: "queue", Attempt: 1, StartedMs: 100, FinishedMs: 110, Status: "success", Logs: []invocation.Log{}}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	response := do(t, s.Handler(), http.MethodPost, "/v1/internal/invocations?ns=acme", "tok", encoded)
	if response.Code != http.StatusOK {
		t.Fatalf("invocation put = %d %s", response.Code, response.Body.String())
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	var found, foundAsync bool
	for _, batch := range exports {
		for _, resource := range batch.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				for _, rec := range scope.LogRecords {
					if rec.Body.GetStringValue() != "invocation" {
						continue
					}
					attrs := map[string]string{}
					for _, attr := range rec.Attributes {
						attrs[attr.Key] = attr.Value.GetStringValue()
					}
					if attrs["cellhive.event_type"] != "scheduled" || attrs["cellhive.script_version"] != `{"id":"v2"}` || attrs["cellhive.request_id"] != "req-42" || attrs["cellhive.outcome"] != "exception" || attrs["cellhive.exceptions"] != `[{"name":"Error","message":"boom","timestamp":42}]` {
						if attrs["cellhive.event_type"] == "queue" && attrs["cellhive.request_id"] == "attempt-1" && attrs["cellhive.trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736" && fmt.Sprintf("%x", rec.TraceId) == "4bf92f3577b34da6a3ce929d0e0e4736" {
							foundAsync = true
							continue
						}
						t.Fatalf("invocation attributes = %#v", attrs)
					}
					found = true
				}
			}
		}
	}
	if !found || !foundAsync {
		t.Fatalf("invocation exports missing: Tail=%v async=%v", found, foundAsync)
	}
}
