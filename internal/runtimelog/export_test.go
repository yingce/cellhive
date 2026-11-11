package runtimelog

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	collectlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collecttrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}
func stopSpool(t *testing.T, s *Spool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestExportOutageRestartAndTrace(t *testing.T) {
	var available atomic.Bool
	var attempts atomic.Int32
	var mu sync.Mutex
	var messages []string
	var traces int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		if !available.Load() {
			w.WriteHeader(503)
			return
		}
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/v1/logs":
			var req collectlogs.ExportLogsServiceRequest
			if err := proto.Unmarshal(b, &req); err != nil {
				t.Error(err)
			}
			for _, rs := range req.ResourceLogs {
				for _, sl := range rs.ScopeLogs {
					for _, l := range sl.LogRecords {
						messages = append(messages, l.Body.GetStringValue())
						if l.Attributes[0].Value.GetIntValue() != 200 {
							t.Error("lost numeric attribute")
						}
					}
				}
			}
		case "/v1/traces":
			var req collecttrace.ExportTraceServiceRequest
			if err := proto.Unmarshal(b, &req); err != nil {
				t.Error(err)
			}
			p := req.ResourceSpans[0].ScopeSpans[0].Spans[0]
			if len(p.TraceId) != 16 || len(p.ParentSpanId) != 8 || p.Name != "fetch" {
				t.Error("lost trace identity")
			}
			traces++
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	cfg := SpoolConfig{Dir: dir, Stream: "test", Endpoint: server.URL}
	s, err := OpenSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintf(s.Writer(), "%s%s %s\n", framePrefix, s.Token(), `{"kind":"log","message":"one","attributes":{"status":200}}`)
	_, _ = fmt.Fprintf(s.Writer(), "%s%s %s\n", framePrefix, s.Token(), `{"kind":"span","span":{"trace_id":"11111111111111111111111111111111","span_id":"2222222222222222","parent_span_id":"3333333333333333","name":"fetch","start_unix_nano":1,"end_unix_nano":2}}`)
	eventually(t, func() bool { return attempts.Load() > 0 })
	if _, err = os.Stat(s.cursorPath()); !os.IsNotExist(err) {
		t.Fatal("failed delivery advanced cursor")
	}
	stopSpool(t, s) // Backlog survives process restart.
	available.Store(true)
	s, err = OpenSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(messages) == 1 && traces == 1 })
	eventually(t, func() bool { _, err := os.Stat(s.cursorPath()); return err == nil })
	stopSpool(t, s)
	s, err = OpenSpool(cfg)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	stopSpool(t, s)
	mu.Lock()
	defer mu.Unlock()
	if len(messages) != 1 || traces != 1 {
		t.Fatalf("acknowledged records replayed: %v / %d", messages, traces)
	}
}
func TestReadBatchPartialAndRotation(t *testing.T) {
	dir := t.TempDir()
	s := &Spool{cfg: SpoolConfig{Dir: dir, Stream: "test"}}
	first := filepath.Join(dir, "test-001.ndjson")
	if err := os.WriteFile(first, []byte("{}\n{\"kind\":"), 0600); err != nil {
		t.Fatal(err)
	}
	c, records, err := s.readBatch(exportCursor{})
	if err != nil || len(records) != 1 || c.Offset != 3 {
		t.Fatalf("partial consumed: %+v %d %v", c, len(records), err)
	}
	f, _ := os.OpenFile(first, os.O_APPEND|os.O_WRONLY, 0600)
	_, _ = f.WriteString("\"runtime\"}\n")
	f.Close()
	c, records, err = s.readBatch(c)
	if err != nil || len(records) != 1 {
		t.Fatal("partial not resumed", err)
	}
	os.WriteFile(filepath.Join(dir, "test-002.ndjson"), []byte("{}\n"), 0600)
	c, records, err = s.readBatch(c)
	if err != nil || len(records) != 1 || c.File != "test-002.ndjson" {
		t.Fatal("rotation", c, err)
	}
}
func TestExportResponseStallDoesNotBlockWriterOrClose(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(entered)
		<-release
	}))
	defer srv.Close()
	defer close(release)
	s, err := OpenSpool(SpoolConfig{Dir: t.TempDir(), Stream: "test", Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(s.Writer(), "%s%s {\"kind\":\"log\"}\n", framePrefix, s.Token())
	select {
	case <-entered:
	case <-time.After(4 * time.Second):
		t.Fatal("not exporting")
	}
	start := time.Now()
	for i := 0; i < 1000; i++ {
		_, _ = s.Writer().Write([]byte("diagnostic\n"))
	}
	if time.Since(start) > time.Second {
		t.Fatal("writer blocked by collector")
	}
	stopSpool(t, s)
}
func TestOTLPPartialSuccessIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := proto.Marshal(&collectlogs.ExportLogsServiceResponse{PartialSuccess: &collectlogs.ExportLogsPartialSuccess{RejectedLogRecords: 1}})
		w.Write(b)
	}))
	defer srv.Close()
	s := &Spool{cfg: SpoolConfig{Endpoint: srv.URL}}
	if err := s.sendBatch(context.Background(), srv.Client(), [][]byte{[]byte(`{"kind":"log","message":"test"}`)}); err != nil {
		t.Fatal(err)
	}
	if s.Stats().Dropped != 1 {
		t.Fatal(s.Stats())
	}
}
func TestCursorPersistence(t *testing.T) {
	s := &Spool{cfg: SpoolConfig{Dir: t.TempDir(), Stream: "test"}}
	want := exportCursor{"test-001.ndjson", 42}
	if err := s.saveCursor(want); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(s.cursorPath())
	var got exportCursor
	if err := json.Unmarshal(b, &got); err != nil || got != want {
		t.Fatal(got, err)
	}
}

// Opt-in real Vector acceptance; endpoint must be an isolated local collector.
func TestVectorExportIntegration(t *testing.T) {
	endpoint := os.Getenv("CELLHIVE_TEST_VECTOR_ENDPOINT")
	if endpoint == "" {
		t.Skip("isolated real Vector endpoint not configured")
	}
	var forwarded chan *collecttrace.ExportTraceServiceRequest
	if addr := os.Getenv("CELLHIVE_TEST_VECTOR_TRACE_LISTEN"); addr != "" {
		forwarded = make(chan *collecttrace.ExportTraceServiceRequest, 4)
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		server := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var reader io.Reader = r.Body
			if r.Header.Get("Content-Encoding") == "gzip" {
				g, err := gzip.NewReader(r.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				defer g.Close()
				reader = g
			}
			b, err := io.ReadAll(io.LimitReader(reader, 1<<20))
			if err != nil {
				t.Error(err)
				return
			}
			var req collecttrace.ExportTraceServiceRequest
			if err := proto.Unmarshal(b, &req); err != nil {
				t.Errorf("Vector downstream protobuf: %v", err)
				w.WriteHeader(400)
				return
			}
			select {
			case forwarded <- &req:
			default:
			}
		})}
		go server.Serve(listener)
		defer server.Close()
	}
	dir := os.Getenv("CELLHIVE_TEST_VECTOR_SPOOL")
	if dir == "" {
		dir = t.TempDir()
	}
	s, err := OpenSpool(SpoolConfig{Dir: dir, Stream: "integration", Endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	defer stopSpool(t, s)
	for i := 0; i < 100; i++ {
		rec := map[string]any{"kind": "log", "timestamp": "2026-11-11T00:00:00Z", "observed_timestamp": "2026-11-11T00:00:00Z", "severity_text": "info", "message": "go-vector-canary", "trace_id": "11111111111111111111111111111111", "span_id": "2222222222222222", "attributes": map[string]any{"cellhive.namespace": "vwork-999999", "cellhive.worker": "go-export-test", "cellhive.request_id": fmt.Sprintf("go-vector-%03d", i), "cellhive.invocation": true, "cellhive.status": 200}}
		b, _ := json.Marshal(rec)
		fmt.Fprintf(s.Writer(), "%s%s %s\n", framePrefix, s.Token(), b)
	}
	fmt.Fprintf(s.Writer(), "%s%s %s\n", framePrefix, s.Token(), `{"kind":"span","span":{"trace_id":"11111111111111111111111111111111","span_id":"2222222222222222","parent_span_id":"3333333333333333","name":"go-vector-canary","start_unix_nano":1794355200000000000,"end_unix_nano":1794355200001000000}}`)
	eventually(t, func() bool {
		if s.Stats().Written != 101 {
			return false
		}
		b, err := os.ReadFile(s.cursorPath())
		if err != nil {
			return false
		}
		var c exportCursor
		if json.Unmarshal(b, &c) != nil {
			return false
		}
		info, err := os.Stat(filepath.Join(dir, c.File))
		return err == nil && info.Size() == c.Offset
	})
	if forwarded != nil {
		select {
		case req := <-forwarded:
			if len(req.ResourceSpans) == 0 || len(req.ResourceSpans[0].ScopeSpans) == 0 || len(req.ResourceSpans[0].ScopeSpans[0].Spans) == 0 {
				t.Fatal("no forwarded spans")
			}
			span := req.ResourceSpans[0].ScopeSpans[0].Spans[0]
			if fmt.Sprintf("%x", span.TraceId) != "11111111111111111111111111111111" || fmt.Sprintf("%x", span.ParentSpanId) != "3333333333333333" || span.Name != "go-vector-canary" {
				t.Fatalf("corrupted forwarded span: %v", span)
			}
		case <-time.After(8 * time.Second):
			t.Fatal("Vector did not forward trace")
		}
	}
	if s.Stats().Dropped != 0 {
		t.Fatal(s.Stats())
	}
}
