package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"cellhive/internal/telemetry"
	collectorlog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestRequestLogExportsFunctionRequest(t *testing.T) {
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
	payload, _ := json.Marshal(requestLogPayload{
		Namespace: "acme", Worker: "meme-api", RequestID: "req-1", Method: "POST",
		Path: "/functions/v1/meme-api/v1/share-links/x/open", Route: "functions",
		Status: 504, StartedMs: 1000, FinishedMs: 61000, Outcome: "error",
	})
	response := do(t, s.Handler(), http.MethodPost, "/v1/internal/request-log", "tok", payload)
	if response.Code != http.StatusOK {
		t.Fatalf("request-log = %d %s", response.Code, response.Body.String())
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(records) != 1 {
		t.Fatalf("got %d logs, want one request log", len(records))
	}
	attrs := map[string]*commonv1.AnyValue{}
	for _, attr := range records[0].Attributes {
		attrs[attr.Key] = attr.Value
	}
	for key, want := range map[string]string{
		"cellhive.event_type": "fetch", "cellhive.request_id": "req-1",
		"cellhive.method": "POST", "cellhive.path": "/functions/v1/meme-api/v1/share-links/x/open",
		"cellhive.route": "functions", "cellhive.outcome": "error",
	} {
		if got := attrs[key]; got == nil || got.GetStringValue() != want {
			t.Errorf("%s = %v, want %q", key, got, want)
		}
	}
	if got := attrs["cellhive.invocation"]; got == nil || !got.GetBoolValue() {
		t.Errorf("cellhive.invocation = %v, want true", got)
	}
	if got := attrs["cellhive.status"]; got == nil || got.GetIntValue() != 504 {
		t.Errorf("cellhive.status = %v, want 504", got)
	}
	if got := attrs["cellhive.duration_ms"]; got == nil || got.GetIntValue() != 60000 {
		t.Errorf("cellhive.duration_ms = %v, want 60000", got)
	}
}
