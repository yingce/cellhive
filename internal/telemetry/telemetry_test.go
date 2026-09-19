package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// otlpSink is a minimal OTLP/HTTP receiver that decodes the protobuf request.
type otlpSink struct {
	srv   *httptest.Server
	mu    sync.Mutex
	spans []string
	trace [][16]byte
}

func newOTLPSink(t *testing.T) *otlpSink {
	t.Helper()
	s := &otlpSink{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req collectortrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			t.Errorf("decode OTLP request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		for _, rs := range req.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, sp := range ss.Spans {
					s.spans = append(s.spans, sp.Name)
					var tid [16]byte
					copy(tid[:], sp.TraceId)
					s.trace = append(s.trace, tid)
				}
			}
		}
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *otlpSink) names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.spans...)
}

func TestDisabledIsNoop(t *testing.T) {
	tel, err := New(context.Background(), Config{})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if tel.Enabled() || Enabled() {
		t.Fatal("empty endpoint must be disabled")
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestExportsSpanAndRemoteSpan(t *testing.T) {
	sink := newOTLPSink(t)
	tel, err := New(context.Background(), Config{
		Endpoint: sink.srv.URL,
		Service:  "cellhive-test",
		NodeID:   "node-1",
		Ratio:    1.0,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if !tel.Enabled() || !Enabled() {
		t.Fatal("exporter should be enabled")
	}

	// A locally-started span.
	ctx, span := Tracer().Start(context.Background(), "local.op",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(ScopeAttrs("acme", "kv", "KV", "acme/__kv__/main")...))
	span.End()

	// A JS-reported span carrying its own ids, in a new trace.
	Ingest(ctx, []RemoteSpan{{
		TraceID: "0123456789abcdef0123456789abcdef", SpanID: "0123456789abcdef",
		Name: "do.invoke", Kind: "server", StartUnixNano: time.Now().Add(-2 * time.Millisecond).UnixNano(),
		EndUnixNano: time.Now().UnixNano(), Attributes: map[string]string{"cellhive.kind": "do"},
		StatusCode: 1,
	}})

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tel.Shutdown(shutCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	names := sink.names()
	found := map[string]bool{}
	for _, n := range names {
		found[n] = true
	}
	if !found["local.op"] || !found["do.invoke"] {
		t.Fatalf("exported spans = %v, want local.op and do.invoke", names)
	}
}

func TestSamplerHonorsRemoteFlag(t *testing.T) {
	sink := newOTLPSink(t)
	tel, err := New(context.Background(), Config{Endpoint: sink.srv.URL, Ratio: 0})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	// Ratio 0: a local root without a remote parent is not sampled.
	_, span := Tracer().Start(context.Background(), "root")
	span.End()
	// A remote parent with the sampled flag must still be sampled (ParentBased).
	Ingest(context.Background(), []RemoteSpan{{
		TraceID: "fedcba9876543210fedcba9876543210", SpanID: "fedcba9876543210",
		Name: "remote.sampled", StartUnixNano: time.Now().UnixNano(), EndUnixNano: time.Now().UnixNano(),
	}})
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tel.Shutdown(shutCtx)
	names := sink.names()
	for _, n := range names {
		if n == "root" {
			t.Fatalf("ratio-0 root should not be sampled; got %v", names)
		}
	}
	found := false
	for _, n := range names {
		if n == "remote.sampled" {
			found = true
		}
	}
	if !found {
		t.Fatalf("remote sampled span missing: %v", names)
	}
}

func TestEndpointPathVariants(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		want string
	}{
		{"base", "/api/default", "/api/default/v1/traces"},
		{"full", "/api/default/v1/traces", "/api/default/v1/traces"},
		{"root", "", "/v1/traces"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan string, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case got <- r.URL.Path:
				default:
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()
			tel, err := New(context.Background(), Config{Endpoint: srv.URL + tc.path, Ratio: 1})
			if err != nil {
				t.Fatalf("new: %v", err)
			}
			_, span := Tracer().Start(context.Background(), "x")
			span.End()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tel.Shutdown(ctx)
			select {
			case p := <-got:
				if p != tc.want {
					t.Fatalf("export path = %q, want %q", p, tc.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("no export request (want path %q)", tc.want)
			}
		})
	}
}
