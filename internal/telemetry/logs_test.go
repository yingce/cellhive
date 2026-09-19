package telemetry

import (
	"context"
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
}

func newLogSink(t *testing.T) *logSink {
	t.Helper()
	s := &logSink{}
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
	ExportLog("ns-off", "w", "log", "off-line", 0)
	shutdownTel(t, off)
	if strings.Contains(sink.lines(), "off-line") {
		t.Fatalf("off mode exported: %q", sink.lines())
	}

	all, err := New(ctx, Config{Endpoint: sink.srv.URL, Logs: "all"})
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	ExportLog("ns-all", "w", "error", "all-line", 0)
	shutdownTel(t, all)
	if !strings.Contains(sink.lines(), "all-line") {
		t.Fatalf("all mode missing line: %q", sink.lines())
	}

	tail, err := New(ctx, Config{Endpoint: sink.srv.URL, Logs: "tail"})
	if err != nil {
		t.Fatalf("tail: %v", err)
	}
	ExportLog("ns-tail", "w", "log", "pre-sub", 0)
	Subscribe("ns-tail", "w", 2*time.Second)
	ExportLog("ns-tail", "w", "log", "post-sub", 0)
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
	ExportLog("ns-exp", "w", "log", "expired", 0)
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
