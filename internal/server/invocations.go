package server

import (
	"cellhive/internal/invocation"
	"cellhive/internal/telemetry"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
)

func (s *Server) handleInvocationPut(w http.ResponseWriter, r *http.Request) {
	if s.Invocations == nil {
		writeErr(w, 503, "no_invocations", "invocation store not configured")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 128*1024+1))
	if err != nil || len(raw) > 128*1024 {
		writeErr(w, 400, "invalid_invocation", "record too large")
		return
	}
	var record invocation.Record
	if err := json.Unmarshal(raw, &record); err != nil {
		writeErr(w, 400, "invalid_invocation", err.Error())
		return
	}
	if record.Namespace != r.URL.Query().Get("ns") {
		writeErr(w, 403, "scope_mismatch", "invocation namespace mismatch")
		return
	}
	if err := invocation.Validate(record); err != nil {
		s.Log.Warn("invocation persistence rejected", "ns", record.Namespace, "worker", record.Worker, "id", record.ID, "kind", record.Kind, "status", record.Status, "err", err)
		writeErr(w, 400, "invalid_invocation", err.Error())
		return
	}
	scope := invocation.Scope(record.Namespace)
	if s.forwardOrClaim(w, r, scope, raw) {
		return
	}
	if s.ensureOrForward(w, r, scope, raw) {
		return
	}
	if err := s.capturedWrite(r.Context(), scope, func() error {
		if record.Status == "running" {
			var err error
			record, err = s.Invocations.Start(r.Context(), record)
			return err
		}
		return s.Invocations.Put(r.Context(), record)
	}); err != nil {
		s.Log.Warn("invocation write failed", "ns", record.Namespace, "worker", record.Worker, "id", record.ID, "kind", record.Kind, "status", record.Status, "err", err)
		s.captureErr(w, err, "invocation_write_failed")
		return
	}
	if record.Status != "running" {
		traceID, spanID := telemetry.TraceIDs(record.Traceparent)
		var links json.RawMessage
		if len(record.ProducerLinks) > 0 {
			links, _ = json.Marshal(record.ProducerLinks)
		}
		telemetry.ExportLogWithMetadata(record.Namespace, record.Worker, "info", "invocation", record.FinishedMs, traceID, spanID,
			telemetry.LogMetadata{EventType: record.Kind, RequestID: record.RequestID, Outcome: record.Status, Invocation: true, ProducerLinks: links, StartedMs: record.StartedMs, FinishedMs: record.FinishedMs})
		if record.Kind == "queue" {
			queueLinks := make([]telemetry.QueueLink, 0, len(record.ProducerLinks))
			for _, link := range record.ProducerLinks {
				queueLinks = append(queueLinks, telemetry.QueueLink{MessageID: link.MessageID, RequestID: link.RequestID, Traceparent: link.Traceparent})
			}
			telemetry.ExportQueueConsumerSpan(record.Traceparent, record.Namespace, record.Worker, record.Resource, record.Status, record.StartedMs, record.FinishedMs, queueLinks)
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true, "invocation_id": record.ID, "invocation": record})
}
func (s *Server) handleInvocationList(w http.ResponseWriter, r *http.Request) {
	if s.Invocations == nil {
		writeErr(w, 503, "no_invocations", "invocation store not configured")
		return
	}
	q := r.URL.Query()
	ns := q.Get("namespace")
	if ns == "" {
		writeErr(w, 400, "bad_request", "namespace is required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	if s.forwardRead(w, r, invocation.Scope(ns), nil) {
		return
	}
	f := invocation.Filter{Namespace: ns, Worker: q.Get("worker"), Kind: q.Get("kind"), ID: q.Get("invocation_id"), Cursor: q.Get("cursor"), Limit: 100}
	f.MessageID = q.Get("message_id")
	f.MessageLogicalID = q.Get("message_logical_id")
	for k, dst := range map[string]*int64{"from_ms": &f.FromMs, "to_ms": &f.ToMs} {
		if q.Get(k) != "" {
			n, err := strconv.ParseInt(q.Get(k), 10, 64)
			if err != nil || n < 0 {
				writeErr(w, 400, "bad_request", "invalid time range")
				return
			}
			*dst = n
		}
	}
	if q.Get("limit") != "" {
		n, err := strconv.Atoi(q.Get("limit"))
		if err != nil || n < 1 || n > 100 {
			writeErr(w, 400, "bad_request", "limit must be 1..100")
			return
		}
		f.Limit = n
	}
	items, err := s.Invocations.List(r.Context(), f)
	if err != nil {
		writeErr(w, 500, "invocation_query_failed", err.Error())
		return
	}
	next := ""
	if len(items) == f.Limit {
		next = items[len(items)-1].ID
	}
	writeJSON(w, 200, map[string]any{"invocations": items, "next_cursor": next})
}
