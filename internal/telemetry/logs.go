// OTLP log export (ADR-172). CellHive keeps the bounded in-memory ring for
// `cellhive tail`; this file adds an optional standard OTLP/HTTP logs pipeline
// so the same lines can land in any OTLP backend (Collector / OpenObserve /
// ...). Export is OFF by default (log volume), and can be limited to workers
// that are actively `tail`ed via a TTL subscription.
package telemetry

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
)

// LogMetadata carries invocation context without coupling telemetry to the
// server's Tail DTO. JSON fields retain their original workerd values.
type LogMetadata struct {
	EventType     string
	MessageArgs   []json.RawMessage
	ScriptVersion json.RawMessage
	RequestID     string
	StartedMs     int64
	FinishedMs    int64
	Outcome       string
	Exceptions    json.RawMessage
	Invocation    bool
	ProducerLinks json.RawMessage
}

// Log export modes.
const (
	LogsOff  = "off"  // no export (default)
	LogsTail = "tail" // export only workers with an active tail subscription
	LogsAll  = "all"  // export every ingested line
)

type logState struct {
	mu       sync.Mutex
	mode     string
	logger   otellog.Logger
	provider *sdklog.LoggerProvider
	subs     map[string]time.Time // "ns/worker" -> expiry
}

var logs = &logState{mode: LogsOff}

// normalizeLogsMode maps the env value onto a valid mode (unknown -> off).
func normalizeLogsMode(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case LogsTail:
		return LogsTail
	case LogsAll:
		return LogsAll
	default:
		return LogsOff
	}
}

// newLogProvider builds the OTLP/HTTP logs pipeline with the same endpoint,
// headers and resource as traces ("/v1/logs" is appended to the path).
func newLogProvider(ctx context.Context, cfg Config, res *resource.Resource) (*sdklog.LoggerProvider, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Host == "" {
		return nil, errInvalidEndpoint(cfg.Endpoint)
	}
	_ = err
	opts := []otlploghttp.Option{otlploghttp.WithEndpoint(u.Host)}
	if u.Scheme != "https" {
		opts = append(opts, otlploghttp.WithInsecure())
	}
	if p := strings.TrimSuffix(u.Path, "/"); p != "" {
		if !strings.HasSuffix(p, "/v1/logs") {
			p += "/v1/logs"
		}
		opts = append(opts, otlploghttp.WithURLPath(p))
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlploghttp.WithHeaders(cfg.Headers))
	}
	exp, err := otlploghttp.New(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp, sdklog.WithExportInterval(2*time.Second))),
		sdklog.WithResource(res),
	), nil
}

// LogsMode reports the active log export mode.
func LogsMode() string {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	return logs.mode
}

// ActiveLogSubscriptions returns the currently tailed workers for on-demand
// runtime ring polling. Expired entries are removed under the same lock.
func ActiveLogSubscriptions() [][2]string {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	now := time.Now()
	out := make([][2]string, 0, len(logs.subs))
	for key, expiry := range logs.subs {
		if now.After(expiry) {
			delete(logs.subs, key)
			continue
		}
		if ns, worker, ok := strings.Cut(key, "/"); ok {
			out = append(out, [2]string{ns, worker})
		}
	}
	return out
}

// Subscribe enables log export for one worker for ttl (renewed by each tail
// poll). It records even when export is off, so a later mode change works.
func Subscribe(ns, worker string, ttl time.Duration) time.Time {
	if ttl <= 0 {
		ttl = time.Minute
	}
	exp := time.Now().Add(ttl)
	logs.mu.Lock()
	if logs.subs == nil {
		logs.subs = map[string]time.Time{}
	}
	logs.subs[ns+"/"+worker] = exp
	logs.mu.Unlock()
	return exp
}

// LogSubscribed reports whether a worker currently has an unexpired
// subscription (lazily dropping expired ones).
func LogSubscribed(ns, worker string) bool {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	return logs.subscribedLocked(ns, worker, time.Now())
}

func (s *logState) subscribedLocked(ns, worker string, now time.Time) bool {
	key := ns + "/" + worker
	exp, ok := s.subs[key]
	if !ok {
		return false
	}
	if now.After(exp) {
		delete(s.subs, key)
		return false
	}
	return true
}

// TraceIDs extracts the 32-hex trace id and 16-hex span id from a W3C
// traceparent header. It returns empty strings for a missing or malformed
// value (or an all-zero id), so callers can attach them unconditionally.
func TraceIDs(traceparent string) (traceID, spanID string) {
	// Fixed W3C layout: 00-<32 hex trace>-<16 hex span>-<2 hex flags>. Parse by
	// position (no split/allocation) and return the original hex substrings.
	tp := strings.TrimSpace(traceparent)
	if len(tp) != 55 || tp[:3] != "00-" || tp[35] != '-' || tp[52] != '-' {
		return "", ""
	}
	for _, ch := range tp[53:55] {
		if !strings.ContainsRune("0123456789abcdefABCDEF", ch) {
			return "", ""
		}
	}
	tid, err := trace.TraceIDFromHex(tp[3:35])
	if err != nil || !tid.IsValid() {
		return "", ""
	}
	sid, err := trace.SpanIDFromHex(tp[36:52])
	if err != nil || !sid.IsValid() {
		return "", ""
	}
	return tp[3:35], tp[36:52]
}

// ExportLog ships one captured line to the OTLP backend when the mode allows
// it. It is best-effort and non-blocking: the SDK batches in the background.
// traceID/spanID (hex, may be empty) correlate the record with its trace.
func ExportLog(ns, worker, level, message string, atMs int64, traceID, spanID string) {
	ExportLogWithMetadata(ns, worker, level, message, atMs, traceID, spanID, LogMetadata{})
}

// ExportLogWithMetadata exports a console line or an invocation record with
// searchable attributes. The original message arguments remain JSON encoded.
func ExportLogWithMetadata(ns, worker, level, message string, atMs int64, traceID, spanID string, meta LogMetadata) {
	s := logs
	s.mu.Lock()
	mode, logger := s.mode, s.logger
	ok := logger != nil
	switch mode {
	case LogsAll:
	case LogsTail:
		ok = ok && s.subscribedLocked(ns, worker, time.Now())
	default:
		ok = false
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	if atMs == 0 {
		atMs = time.Now().UnixMilli()
	}
	ctx := context.Background()
	if sc, ok := spanContext(traceID, spanID); ok {
		ctx = trace.ContextWithSpanContext(ctx, sc)
	}
	rec := buildLogRecord(ns, worker, level, message, atMs)
	if _, valid := spanContext(traceID, spanID); valid {
		// Some OTLP backends ingest the native LogRecord trace fields but do not
		// index them as query columns. Keep the same IDs searchable as attributes.
		rec.AddAttributes(attribute.String("cellhive.trace_id", traceID), attribute.String("cellhive.span_id", spanID))
	}
	if meta.EventType != "" {
		rec.AddAttributes(attribute.String("cellhive.event_type", meta.EventType))
	}
	if len(meta.MessageArgs) > 0 {
		if raw, err := json.Marshal(meta.MessageArgs); err == nil {
			rec.AddAttributes(attribute.String("cellhive.message_args", string(raw)))
		}
	}
	if len(meta.ScriptVersion) > 0 {
		rec.AddAttributes(attribute.String("cellhive.script_version", string(meta.ScriptVersion)))
	}
	if meta.RequestID != "" {
		rec.AddAttributes(attribute.String("cellhive.request_id", meta.RequestID))
	}
	if meta.StartedMs > 0 {
		rec.AddAttributes(attribute.Int64("cellhive.started_ms", meta.StartedMs))
	}
	if meta.FinishedMs > 0 {
		rec.AddAttributes(attribute.Int64("cellhive.finished_ms", meta.FinishedMs))
	}
	// Missing or reversed timestamps do not describe a valid duration.
	if meta.StartedMs > 0 && meta.FinishedMs >= meta.StartedMs {
		rec.AddAttributes(attribute.Int64("cellhive.duration_ms", meta.FinishedMs-meta.StartedMs))
	}
	if meta.Outcome != "" {
		rec.AddAttributes(attribute.String("cellhive.outcome", meta.Outcome))
	}
	if len(meta.Exceptions) > 0 {
		rec.AddAttributes(attribute.String("cellhive.exceptions", string(meta.Exceptions)))
	}
	if meta.Invocation {
		rec.AddAttributes(attribute.Bool("cellhive.invocation", true))
	}
	if len(meta.ProducerLinks) > 0 {
		rec.AddAttributes(attribute.String("cellhive.producer_links", string(meta.ProducerLinks)))
	}
	logger.Emit(ctx, rec)
}

// buildLogRecord assembles one OTLP log record: body = message, severity from
// level, cellhive.namespace/worker as attributes, and W3C trace/span ids when
// present (so backends can jump trace <-> logs).
func buildLogRecord(ns, worker, level, message string, atMs int64) otellog.Record {
	if atMs == 0 {
		atMs = time.Now().UnixMilli()
	}
	var rec otellog.Record
	rec.SetTimestamp(time.UnixMilli(atMs))
	rec.SetObservedTimestamp(time.Now())
	rec.SetSeverity(logSeverity(level))
	rec.SetSeverityText(level)
	rec.SetBody(attribute.StringValue(message))
	rec.AddAttributes(
		attribute.String("cellhive.namespace", ns),
		attribute.String("cellhive.worker", worker),
	)
	return rec
}

// spanContext builds a valid, sampled span context from hex ids (ok=false when
// either id is missing/invalid). The SDK takes a log record's trace/span ids
// from the context passed to Emit, not from the record itself.
func spanContext(traceID, spanID string) (trace.SpanContext, bool) {
	tid, terr := trace.TraceIDFromHex(traceID)
	sid, serr := trace.SpanIDFromHex(spanID)
	if terr != nil || serr != nil || !tid.IsValid() || !sid.IsValid() {
		return trace.SpanContext{}, false
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled,
	}), true
}

func logSeverity(level string) otellog.Severity {
	switch strings.ToLower(level) {
	case "debug":
		return otellog.SeverityDebug
	case "warn", "warning":
		return otellog.SeverityWarn
	case "error":
		return otellog.SeverityError
	default:
		return otellog.SeverityInfo
	}
}
