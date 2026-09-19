// Package telemetry wires CellHive to the standard OpenTelemetry OTLP/HTTP
// export path (ADR-167). Traces are exported to any OTLP-compatible backend
// (Collector/Tempo/Jaeger/cloud) by changing CELLHIVE_OTLP_ENDPOINT; CellHive
// itself stores nothing. When the endpoint is empty the package is a no-op and
// the request path pays nothing.
package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Config configures the exporter. Endpoint empty disables telemetry entirely.
type Config struct {
	// Endpoint is an OTLP/HTTP base URL (e.g. http://collector:4318). A path
	// ending in /v1/traces is honored; otherwise the default is used.
	Endpoint string
	// Headers are sent on every export request (authorization etc.).
	Headers map[string]string
	// Service/Version/NodeID populate the OTLP Resource.
	Service string
	Version string
	NodeID  string
	// Ratio is the head sampling ratio for new (root) traces. The upstream
	// W3C sampled flag always wins via ParentBased.
	Ratio float64
	// Logs selects OTLP log export: "off" (default), "tail" (only workers with
	// an active tail subscription) or "all". Requires Endpoint.
	Logs string
}

// enabled mirrors the active provider for cheap checks on hot paths.
var enabled atomic.Bool

// Telemetry owns the tracer and logger providers.
type Telemetry struct {
	enabled bool
	tp      *sdktrace.TracerProvider
	lp      *sdklog.LoggerProvider
}

// errInvalidEndpoint is shared by the trace and log exporters.
func errInvalidEndpoint(endpoint string) error {
	return fmt.Errorf("telemetry: OTLP endpoint needs a host: %q", endpoint)
}

// Enabled reports whether any exporter is active.
func Enabled() bool { return enabled.Load() }

// New builds the exporter and installs the global tracer provider and W3C
// trace-context propagator. With an empty endpoint it returns a disabled
// instance and leaves the OTel no-op globals in place.
func New(ctx context.Context, cfg Config) (*Telemetry, error) {
	// The package-level mirrors reflect the most recently created provider (a
	// single New per process in production; tests create several).
	enabled.Store(false)
	logs.mu.Lock()
	logs.mode = LogsOff
	logs.logger = nil
	logs.mu.Unlock()
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return &Telemetry{}, nil
	}
	exp, err := newExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}
	ratio := cfg.Ratio
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	res := resource.NewSchemaless(
		attribute.String("service.name", cfg.Service),
		attribute.String("service.version", cfg.Version),
		// service.instance.id is the OTel-recommended per-instance identity; the
		// cellhive.node_id alias is kept for existing dashboards.
		attribute.String("service.instance.id", cfg.NodeID),
		attribute.String("cellhive.node_id", cfg.NodeID),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	enabled.Store(true)
	tel := &Telemetry{enabled: true, tp: tp}

	if mode := normalizeLogsMode(cfg.Logs); mode != LogsOff {
		lp, lerr := newLogProvider(ctx, cfg, res)
		if lerr != nil {
			_ = tp.Shutdown(ctx)
			return nil, lerr
		}
		tel.lp = lp
		logs.mu.Lock()
		logs.mode = mode
		logs.logger = lp.Logger("cellhive")
		logs.mu.Unlock()
	}
	return tel, nil
}

func newExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("telemetry: bad OTLP endpoint: %w", err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("telemetry: OTLP endpoint needs a host: %q", cfg.Endpoint)
	}
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(u.Host)}
	if u.Scheme != "https" {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	// Accept both a base URL (path appended) and a full /v1/traces URL.
	if p := strings.TrimSuffix(u.Path, "/"); p != "" {
		if !strings.HasSuffix(p, "/v1/traces") {
			p += "/v1/traces"
		}
		opts = append(opts, otlptracehttp.WithURLPath(p))
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(cfg.Headers))
	}
	return otlptracehttp.New(ctx, opts...)
}

// Enabled reports whether traces are exported.
func (t *Telemetry) Enabled() bool { return t != nil && t.enabled }

// Shutdown flushes pending spans and logs.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if t.lp != nil {
		_ = t.lp.Shutdown(ctx)
	}
	if t.tp == nil {
		return nil
	}
	return t.tp.Shutdown(ctx)
}

// SpanFromContext returns the active span (no-op span when none/disabled).
func SpanFromContext(ctx context.Context) trace.Span { return trace.SpanFromContext(ctx) }

// Tracer returns the shared CellHive tracer (no-op when disabled).
func Tracer() trace.Tracer { return otel.Tracer("cellhive") }

// Extract reads a W3C traceparent from HTTP headers into ctx.
func Extract(ctx context.Context, h http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(h))
}

// Inject writes the current span's traceparent into outgoing HTTP headers.
func Inject(ctx context.Context, h http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
}

// Start begins a span, extracting remote context from the request headers.
func Start(ctx context.Context, r *http.Request, name string, kind trace.SpanKind, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx = Extract(ctx, r.Header)
	return Tracer().Start(ctx, name, trace.WithSpanKind(kind), trace.WithAttributes(attrs...))
}

// End finishes a span, recording the error and status when set.
func End(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// ScopeAttrs are the standard CellHive span attributes.
func ScopeAttrs(ns, kind, name, scope string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 4)
	if ns != "" {
		attrs = append(attrs, attribute.String("cellhive.namespace", ns))
	}
	if kind != "" {
		attrs = append(attrs, attribute.String("cellhive.kind", kind))
	}
	if name != "" {
		attrs = append(attrs, attribute.String("cellhive.name", name))
	}
	if scope != "" {
		attrs = append(attrs, attribute.String("cellhive.scope", scope))
	}
	return attrs
}

// RemoteSpan is one span reported by a JS platform component (workerd cannot
// run the OTel SDK). IDs are hex W3C ids; status 0=unset 1=ok 2=error.
type RemoteSpan struct {
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	ParentSpanID  string            `json:"parent_span_id"`
	Name          string            `json:"name"`
	Kind          string            `json:"kind"` // server|client|internal (default internal)
	StartUnixNano int64             `json:"start_unix_nano"`
	EndUnixNano   int64             `json:"end_unix_nano"`
	Attributes    map[string]string `json:"attributes"`
	StatusCode    int               `json:"status_code"`
	StatusMessage string            `json:"status_message"`
}

// Ingest re-creates JS-reported spans in the (sampled) trace and exports them
// with the same OTLP pipeline.
func Ingest(ctx context.Context, spans []RemoteSpan) {
	tr := Tracer()
	for _, s := range spans {
		tid, err := trace.TraceIDFromHex(s.TraceID)
		if err != nil {
			continue
		}
		sid, err := trace.SpanIDFromHex(s.SpanID)
		if err != nil {
			continue
		}
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, Remote: true,
		})
		start := time.Unix(0, s.StartUnixNano)
		if s.StartUnixNano == 0 {
			start = time.Now()
		}
		end := time.Unix(0, s.EndUnixNano)
		attrs := make([]attribute.KeyValue, 0, len(s.Attributes))
		for k, v := range s.Attributes {
			attrs = append(attrs, attribute.String(k, v))
		}
		_, span := tr.Start(trace.ContextWithSpanContext(ctx, sc), s.Name,
			trace.WithSpanKind(spanKind(s.Kind)),
			trace.WithTimestamp(start),
			trace.WithAttributes(attrs...))
		switch s.StatusCode {
		case 1:
			span.SetStatus(codes.Ok, "")
		case 2:
			span.SetStatus(codes.Error, s.StatusMessage)
		}
		if s.EndUnixNano == 0 {
			span.End()
		} else {
			span.End(trace.WithTimestamp(end))
		}
	}
}

func spanKind(k string) trace.SpanKind {
	switch strings.ToLower(k) {
	case "server":
		return trace.SpanKindServer
	case "client":
		return trace.SpanKindClient
	case "producer":
		return trace.SpanKindProducer
	case "consumer":
		return trace.SpanKindConsumer
	default:
		return trace.SpanKindInternal
	}
}
