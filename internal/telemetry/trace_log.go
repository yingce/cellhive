package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Diagnostic recording expires automatically and does not change the upstream
// sampled bit: normally dropped spans become RecordOnly, for the local log only.
type diagnosticSampler struct {
	fallback sdktrace.Sampler
	until    time.Time
}

func (s diagnosticSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	r := s.fallback.ShouldSample(p)
	if time.Now().Before(s.until) && r.Decision == sdktrace.Drop {
		r.Decision = sdktrace.RecordOnly
	}
	return r
}
func (s diagnosticSampler) Description() string {
	return "ExpiringTraceLog(" + s.fallback.Description() + ")"
}

// Unlike BatchSpanProcessor, which skips unsampled spans, this processor queues
// RecordOnly diagnostic spans too. Never block request execution on stdout.
type traceLogProcessor struct {
	stop  sync.Once
	queue chan traceLogRecord
	done  chan struct{}
	until time.Time
}
type traceLogRecord struct {
	Event        string         `json:"event"`
	Node         string         `json:"node"`
	TraceID      string         `json:"trace_id"`
	SpanID       string         `json:"span_id"`
	ParentSpanID string         `json:"parent_span_id"`
	Name         string         `json:"name"`
	Start        string         `json:"started_at"`
	DurationMS   float64        `json:"duration_ms"`
	Status       string         `json:"status"`
	Attributes   map[string]any `json:"attributes,omitempty"`
}

func newTraceLogProcessor(w io.Writer, until time.Time) *traceLogProcessor {
	p := &traceLogProcessor{queue: make(chan traceLogRecord, 4096), done: make(chan struct{}), until: until}
	go func() {
		enc := json.NewEncoder(w)
		for {
			select {
			case r := <-p.queue:
				_ = enc.Encode(r)
			case <-p.done:
				return
			}
		}
	}()
	return p
}
func (p *traceLogProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (p *traceLogProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	if !time.Now().Before(p.until) {
		return
	}
	r := traceLogRecord{Event: "trace_span", TraceID: s.SpanContext().TraceID().String(), SpanID: s.SpanContext().SpanID().String(), ParentSpanID: s.Parent().SpanID().String(), Name: s.Name(), Start: s.StartTime().UTC().Format(time.RFC3339Nano), DurationMS: float64(s.EndTime().Sub(s.StartTime())) / float64(time.Millisecond), Status: s.Status().Code.String()}
	for _, a := range s.Resource().Attributes() {
		if string(a.Key) == "cellhive.node_id" {
			r.Node = a.Value.AsString()
		}
	}
	for _, a := range s.Attributes() {
		switch string(a.Key) {
		case "cellhive.namespace", "cellhive.worker", "cellhive.scope", "cell.scope", "cellhive.kind", "cellhive.name", "cell.txid", "http.status_code", "object.key", "object.prefix":
			if r.Attributes == nil {
				r.Attributes = map[string]any{}
			}
			v := a.Value.AsInterface()
			if x, ok := v.(string); ok && len(x) > 512 {
				v = x[:512]
			}
			r.Attributes[string(a.Key)] = v
		}
	}
	select {
	case p.queue <- r:
	default:
	}
}

// Shutdown is best effort, like the bounded diagnostic queue itself.
func (p *traceLogProcessor) Shutdown(context.Context) error {
	p.stop.Do(func() { close(p.done) })
	return nil
}
func (p *traceLogProcessor) ForceFlush(context.Context) error { return nil }
func traceLogDeadline() time.Time {
	until, err := time.Parse(time.RFC3339, os.Getenv("CELLHIVE_TRACE_LOG_UNTIL"))
	if err != nil || !until.After(time.Now()) || until.After(time.Now().Add(24*time.Hour)) {
		return time.Time{}
	}
	return until
}
