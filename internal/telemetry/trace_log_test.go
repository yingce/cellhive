package telemetry

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type traceLogWriter chan []byte

func (w traceLogWriter) Write(p []byte) (int, error) {
	w <- append([]byte(nil), p...)
	return len(p), nil
}
func TestDiagnosticSamplerExpiryAndNoExportPromotion(t *testing.T) {
	s := diagnosticSampler{fallback: sdktrace.NeverSample(), until: time.Now().Add(time.Minute)}
	if r := s.ShouldSample(sdktrace.SamplingParameters{}); r.Decision != sdktrace.RecordOnly {
		t.Fatal(r)
	}
	s.until = time.Now().Add(-time.Second)
	if r := s.ShouldSample(sdktrace.SamplingParameters{}); r.Decision != sdktrace.Drop {
		t.Fatal(r)
	}
	s.fallback = sdktrace.AlwaysSample()
	if r := s.ShouldSample(sdktrace.SamplingParameters{}); r.Decision != sdktrace.RecordAndSample {
		t.Fatal(r)
	}
}
func TestDiagnosticLogUnsampledIDsAndRedaction(t *testing.T) {
	w := make(traceLogWriter, 1)
	p := newTraceLogProcessor(w, time.Now().Add(time.Minute))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(diagnosticSampler{fallback: sdktrace.NeverSample(), until: time.Now().Add(time.Minute)}), sdktrace.WithSpanProcessor(p))
	defer tp.Shutdown(context.Background())
	_, span := tp.Tracer("test").Start(context.Background(), "cell.open_uncached", trace.WithAttributes(attribute.String("cell.scope", "vwork-115/__kv__/CACHE"), attribute.String("http.url", "SECRET_URL"), attribute.String("authorization", "SECRET_TOKEN")))
	if span.SpanContext().IsSampled() {
		t.Fatal("diagnostics promoted upstream sampling")
	}
	span.End()
	select {
	case raw := <-w:
		var r traceLogRecord
		if err := json.Unmarshal(raw, &r); err != nil {
			t.Fatal(err)
		}
		if r.TraceID != span.SpanContext().TraceID().String() || r.Name != "cell.open_uncached" || r.Attributes["cell.scope"] != "vwork-115/__kv__/CACHE" {
			t.Fatal(string(raw))
		}
		if strings.Contains(string(raw), "SECRET") {
			t.Fatal("secret leaked")
		}
	case <-time.After(time.Second):
		t.Fatal("unsampled span not logged")
	}
}
func TestDiagnosticDeadlineBounded(t *testing.T) {
	for _, v := range []string{"", "bad", time.Now().Add(-time.Hour).Format(time.RFC3339), time.Now().Add(25 * time.Hour).Format(time.RFC3339)} {
		t.Setenv("CELLHIVE_TRACE_LOG_UNTIL", v)
		if !traceLogDeadline().IsZero() {
			t.Fatal(v)
		}
	}
	t.Setenv("CELLHIVE_TRACE_LOG_UNTIL", time.Now().Add(time.Hour).Format(time.RFC3339))
	if traceLogDeadline().IsZero() {
		t.Fatal("valid deadline rejected")
	}
}
