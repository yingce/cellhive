package runtimelog

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	collectlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collecttrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

func attributes(m map[string]any) []*common.KeyValue {
	out := make([]*common.KeyValue, 0, len(m))
	for k, v := range m {
		out = append(out, &common.KeyValue{Key: k, Value: value(v)})
	}
	return out
}
func value(v any) *common.AnyValue {
	switch x := v.(type) {
	case string:
		return &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: x}}
	case bool:
		return &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: x}}
	case float64:
		if x == float64(int64(x)) {
			return &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64(x)}}
		}
		return &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: x}}
	default:
		b, _ := json.Marshal(v)
		return &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: string(b)}}
	}
}
func id(s string, n int) []byte {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != n {
		return nil
	}
	return b
}
func timestamp(s string) uint64 {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || t.UnixNano() < 0 {
		return 0
	}
	return uint64(t.UnixNano())
}
func encodeRecords(records [][]byte, service string) (*collectlogs.ExportLogsServiceRequest, *collecttrace.ExportTraceServiceRequest, uint64) {
	lr := &collectlogs.ExportLogsServiceRequest{}
	tr := &collecttrace.ExportTraceServiceRequest{}
	sl := &logs.ScopeLogs{Scope: &common.InstrumentationScope{Name: "cellhive.runtime"}}
	st := &trace.ScopeSpans{Scope: &common.InstrumentationScope{Name: "cellhive.runtime"}}
	var invalid uint64
	for _, raw := range records {
		var r struct {
			Kind       string         `json:"kind"`
			Timestamp  string         `json:"timestamp"`
			Observed   string         `json:"observed_timestamp"`
			Severity   string         `json:"severity_text"`
			Message    string         `json:"message"`
			TraceID    string         `json:"trace_id"`
			SpanID     string         `json:"span_id"`
			Attributes map[string]any `json:"attributes"`
			Span       struct {
				TraceID    string         `json:"trace_id"`
				SpanID     string         `json:"span_id"`
				ParentID   string         `json:"parent_span_id"`
				Name       string         `json:"name"`
				Kind       string         `json:"kind"`
				Start      uint64         `json:"start_unix_nano"`
				End        uint64         `json:"end_unix_nano"`
				Attributes map[string]any `json:"attributes"`
				Code       int32          `json:"status_code"`
				Message    string         `json:"status_message"`
			} `json:"span"`
		}
		if json.Unmarshal(raw, &r) != nil {
			invalid++
			continue
		}
		switch r.Kind {
		case "log":
			severity := logs.SeverityNumber_SEVERITY_NUMBER_INFO
			switch strings.ToLower(r.Severity) {
			case "debug":
				severity = logs.SeverityNumber_SEVERITY_NUMBER_DEBUG
			case "warn", "warning":
				severity = logs.SeverityNumber_SEVERITY_NUMBER_WARN
			case "error":
				severity = logs.SeverityNumber_SEVERITY_NUMBER_ERROR
			}
			sl.LogRecords = append(sl.LogRecords, &logs.LogRecord{TimeUnixNano: timestamp(r.Timestamp), ObservedTimeUnixNano: timestamp(r.Observed), SeverityText: r.Severity, SeverityNumber: severity, Body: value(r.Message), Attributes: attributes(r.Attributes), TraceId: id(r.TraceID, 16), SpanId: id(r.SpanID, 8)})
		case "span":
			p := r.Span
			tid, sid := id(p.TraceID, 16), id(p.SpanID, 8)
			if tid == nil || sid == nil || p.End < p.Start {
				invalid++
				continue
			}
			kind := trace.Span_SPAN_KIND_INTERNAL
			switch p.Kind {
			case "server":
				kind = trace.Span_SPAN_KIND_SERVER
			case "client":
				kind = trace.Span_SPAN_KIND_CLIENT
			case "producer":
				kind = trace.Span_SPAN_KIND_PRODUCER
			case "consumer":
				kind = trace.Span_SPAN_KIND_CONSUMER
			}
			st.Spans = append(st.Spans, &trace.Span{TraceId: tid, SpanId: sid, ParentSpanId: id(p.ParentID, 8), Name: p.Name, Kind: kind, StartTimeUnixNano: p.Start, EndTimeUnixNano: p.End, Attributes: attributes(p.Attributes), Status: &trace.Status{Code: trace.Status_StatusCode(p.Code), Message: p.Message}, Flags: 1})
		case "runtime": // Local process diagnostics are not tenant logs.
		default:
			invalid++
		}
	}
	res := &resource.Resource{Attributes: attributes(map[string]any{"service.name": service})}
	if len(sl.LogRecords) > 0 {
		lr.ResourceLogs = []*logs.ResourceLogs{{Resource: res, ScopeLogs: []*logs.ScopeLogs{sl}}}
	}
	if len(st.Spans) > 0 {
		tr.ResourceSpans = []*trace.ResourceSpans{{Resource: res, ScopeSpans: []*trace.ScopeSpans{st}}}
	}
	return lr, tr, invalid
}
