package telemetry

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	collectorlog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

type logSink struct {
	srv    *httptest.Server
	mu     sync.Mutex
	bodies []string
	traces map[string][2][]byte // body -> {traceID, spanID}
}

func newLogSink(t *testing.T) *logSink {
	t.Helper()
	s := &logSink{traces: map[string][2][]byte{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/logs" {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req collectorlog.ExportLogsServiceRequest
		if proto.Unmarshal(body, &req) == nil {
			s.mu.Lock()
			for _, rl := range req.ResourceLogs {
				for _, sl := range rl.ScopeLogs {
					for _, lr := range sl.LogRecords {
						s.bodies = append(s.bodies, lr.Body.GetStringValue())
						s.traces[lr.Body.GetStringValue()] = [2][]byte{lr.TraceId, lr.SpanId}
					}
				}
			}
			s.mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *logSink) lines() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.bodies, "\n")
}

func shutdownTel(t *testing.T, tel *Telemetry) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// TestLogExportModes covers ADR-172: off exports nothing, all exports every
// line, and tail exports only subscribed workers until the TTL lapses.
func TestLogExportModes(t *testing.T) {
	sink := newLogSink(t)
	ctx := context.Background()

	off, err := New(ctx, Config{Endpoint: sink.srv.URL, Logs: "off"})
	if err != nil {
		t.Fatalf("off: %v", err)
	}
	ExportLog("ns-off", "w", "log", "off-line", 0, "", "")
	shutdownTel(t, off)
	if strings.Contains(sink.lines(), "off-line") {
		t.Fatalf("off mode exported: %q", sink.lines())
	}

	all, err := New(ctx, Config{Endpoint: sink.srv.URL, Logs: "all"})
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	ExportLog("ns-all", "w", "error", "all-line", 0, "", "")
	shutdownTel(t, all)
	if !strings.Contains(sink.lines(), "all-line") {
		t.Fatalf("all mode missing line: %q", sink.lines())
	}

	tail, err := New(ctx, Config{Endpoint: sink.srv.URL, Logs: "tail"})
	if err != nil {
		t.Fatalf("tail: %v", err)
	}
	ExportLog("ns-tail", "w", "log", "pre-sub", 0, "", "")
	Subscribe("ns-tail", "w", 2*time.Second)
	ExportLog("ns-tail", "w", "log", "post-sub", 0, "", "")
	shutdownTel(t, tail)
	got := sink.lines()
	if !strings.Contains(got, "post-sub") || strings.Contains(got, "pre-sub") {
		t.Fatalf("tail gating wrong: %q", got)
	}

	exp, err := New(ctx, Config{Endpoint: sink.srv.URL, Logs: "tail"})
	if err != nil {
		t.Fatalf("tail2: %v", err)
	}
	Subscribe("ns-exp", "w", 20*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	ExportLog("ns-exp", "w", "log", "expired", 0, "", "")
	shutdownTel(t, exp)
	if strings.Contains(sink.lines(), "expired") {
		t.Fatalf("expired subscription still exported: %q", sink.lines())
	}
}

func TestLogSubscribeTTL(t *testing.T) {
	if LogSubscribed("ns-none", "w") {
		t.Fatal("unsubscribed worker reported subscribed")
	}
	Subscribe("ns-ttl", "w", 30*time.Millisecond)
	if !LogSubscribed("ns-ttl", "w") {
		t.Fatal("fresh subscription not visible")
	}
	time.Sleep(50 * time.Millisecond)
	if LogSubscribed("ns-ttl", "w") {
		t.Fatal("expired subscription still visible")
	}
}

// TestLogExportTraceCorrelation covers ADR-174 follow-up: when a log line
// carries a W3C traceparent, the exported OTLP record carries the same
// trace_id/span_id (so backends can jump trace <-> logs). A malformed or
// absent traceparent yields a record with no trace context.
func TestLogExportTraceCorrelation(t *testing.T) {
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	tid, sid := TraceIDs(tp)
	if tid != "4bf92f3577b34da6a3ce929d0e0e4736" || sid != "00f067aa0ba902b7" {
		t.Fatalf("TraceIDs = %q/%q", tid, sid)
	}
	if a, b := TraceIDs("garbage"); a != "" || b != "" {
		t.Fatalf("malformed traceparent accepted: %q/%q", a, b)
	}
	if a, b := TraceIDs("00-00000000000000000000000000000000-0000000000000000-01"); a != "" || b != "" {
		t.Fatalf("all-zero ids accepted: %q/%q", a, b)
	}
	sink := newLogSink(t)
	tel, err := New(context.Background(), Config{Endpoint: sink.srv.URL, Logs: "all"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ExportLog("ns", "w", "log", "traced-line", 0, tid, sid)
	ExportLog("ns", "w", "log", "untraced-line", 0, "", "")
	shutdownTel(t, tel)
	got, ok := sink.traces["traced-line"]
	if !ok {
		t.Fatalf("traced-line not exported (got %q)", sink.lines())
	}
	if fmt.Sprintf("%x", got[0]) != tid || fmt.Sprintf("%x", got[1]) != sid {
		t.Fatalf("record trace/span = %x/%x, want %s/%s", got[0], got[1], tid, sid)
	}
	if u := sink.traces["untraced-line"]; len(u[0]) != 0 || len(u[1]) != 0 {
		t.Fatalf("untraced record has trace context: %x/%x", u[0], u[1])
	}
}

func BenchmarkTraceIDs(b *testing.B) {
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		TraceIDs(tp)
	}
}

// BenchmarkExportLogOff is the per-line cost when OTLP log export is disabled
// (the default): a mutex-guarded mode check followed by an early return.
func BenchmarkExportLogOff(b *testing.B) {
	if _, err := New(context.Background(), Config{}); err != nil {
		b.Fatalf("new: %v", err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ExportLog("ns", "w", "log", "hello", 0, "", "")
	}
}
