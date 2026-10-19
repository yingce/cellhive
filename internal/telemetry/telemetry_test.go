package telemetry

import (
	"context"
	"fmt"
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
type exportedSpan struct {
	name   string
	trace  [16]byte
	span   [8]byte
	parent [8]byte
	links  [][3]string
}

type otlpSink struct {
	srv   *httptest.Server
	mu    sync.Mutex
	spans []exportedSpan
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
					var got exportedSpan
					got.name = sp.Name
					copy(got.trace[:], sp.TraceId)
					copy(got.span[:], sp.SpanId)
					copy(got.parent[:], sp.ParentSpanId)
					for _, link := range sp.Links {
						messageID := ""
						for _, kv := range link.Attributes {
							if kv.Key == "messaging.message.id" {
								messageID = kv.Value.GetStringValue()
							}
						}
						got.links = append(got.links, [3]string{fmt.Sprintf("%x", link.TraceId), fmt.Sprintf("%x", link.SpanId), messageID})
					}
					s.spans = append(s.spans, got)
				}
			}
		}
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func TestQueueConsumerSpanLinksMixedProducerTraces(t *testing.T) {
	sink := newOTLPSink(t)
	tel, err := New(context.Background(), Config{Endpoint: sink.srv.URL, Ratio: 0})
	if err != nil {
		t.Fatal(err)
	}
	ExportQueueConsumerSpan("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01", "acme", "consumer", "events", "success", 100, 200, []QueueLink{
		{MessageID: "m1", RequestID: "r1", Traceparent: "00-11111111111111111111111111111111-2222222222222222-01"},
		{MessageID: "m2", RequestID: "r2", Traceparent: "00-33333333333333333333333333333333-4444444444444444-01"},
		{MessageID: "m3", Traceparent: "invalid"},
	})
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := sink.exported()
	if len(spans) != 1 || spans[0].name != "queue.consume" || fmt.Sprintf("%x", spans[0].trace) != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || fmt.Sprintf("%x", spans[0].span) != "bbbbbbbbbbbbbbbb" || fmt.Sprintf("%x", spans[0].parent) != "0000000000000000" {
		t.Fatalf("consumer identity/parent: %+v", spans)
	}
	if got := spans[0].links; len(got) != 2 || got[0] != [3]string{"11111111111111111111111111111111", "2222222222222222", "m1"} || got[1] != [3]string{"33333333333333333333333333333333", "4444444444444444", "m2"} {
		t.Fatalf("producer links: %+v", got)
	}
}

func (s *otlpSink) exported() []exportedSpan {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]exportedSpan(nil), s.spans...)
}

func (s *otlpSink) names() []string {
	spans := s.exported()
	names := make([]string, len(spans))
	for i := range spans {
		names[i] = spans[i].name
	}
	return names
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

	// A JS-reported span must retain its supplied identity and parent. Otherwise
	// logs and child spans carrying its W3C span ID cannot correlate to it.
	Ingest(ctx, []RemoteSpan{{
		TraceID: "0123456789abcdef0123456789abcdef", SpanID: "0123456789abcdef",
		ParentSpanID: "fedcba9876543210", Name: "do.invoke", Kind: "server",
		StartUnixNano: time.Now().Add(-2 * time.Millisecond).UnixNano(),
		EndUnixNano:   time.Now().UnixNano(), Attributes: map[string]string{"cellhive.kind": "do"},
		StatusCode: 1,
	}})
	Ingest(ctx, []RemoteSpan{{
		TraceID: "11111111111111111111111111111111", SpanID: "2222222222222222",
		Name: "loader.root", Kind: "server", StartUnixNano: time.Now().UnixNano(),
		EndUnixNano: time.Now().UnixNano(),
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
	if !found["local.op"] || !found["do.invoke"] || !found["loader.root"] {
		t.Fatalf("exported spans = %v, want local.op, do.invoke, and loader.root", names)
	}
	for _, sp := range sink.exported() {
		if sp.name != "do.invoke" {
			continue
		}
		if got := fmt.Sprintf("%x", sp.trace); got != "0123456789abcdef0123456789abcdef" {
			t.Fatalf("remote trace id = %s", got)
		}
		if got := fmt.Sprintf("%x", sp.span); got != "0123456789abcdef" {
			t.Fatalf("remote span id = %s, want supplied JS id", got)
		}
		if got := fmt.Sprintf("%x", sp.parent); got != "fedcba9876543210" {
			t.Fatalf("remote parent span id = %s", got)
		}
	}
	for _, sp := range sink.exported() {
		if sp.name != "loader.root" {
			continue
		}
		if got := fmt.Sprintf("%x", sp.trace); got != "11111111111111111111111111111111" {
			t.Fatalf("remote root trace id = %s", got)
		}
		if got := fmt.Sprintf("%x", sp.span); got != "2222222222222222" {
			t.Fatalf("remote root span id = %s, want supplied JS id", got)
		}
		if got := fmt.Sprintf("%x", sp.parent); got != "0000000000000000" {
			t.Fatalf("remote root parent span id = %s, want empty", got)
		}
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
		{"arms", "/adapt-example/api/otlp/traces", "/adapt-example/api/otlp/traces"},
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
