// OTLP log export (ADR-172). CellHive keeps the bounded in-memory ring for
// `cellhive tail`; this file adds an optional standard OTLP/HTTP logs pipeline
// so the same lines can land in any OTLP backend (Collector / OpenObserve /
// ...). Export is OFF by default (log volume), and can be limited to workers
// that are actively `tail`ed via a TTL subscription.
package telemetry

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

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

// ExportLog ships one captured line to the OTLP backend when the mode allows
// it. It is best-effort and non-blocking: the SDK batches in the background.
func ExportLog(ns, worker, level, message string, atMs int64) {
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
	logger.Emit(context.Background(), rec)
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
