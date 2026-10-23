package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/logbuf"
	"cellhive/internal/telemetry"
)

// logFanoutEvery throttles per-worker peer fan-out: subscribers renew every
// poll, but the fleet only needs to hear about it a couple of times per TTL.
const logFanoutEvery = 25 * time.Second

// handleLogIngest appends bounded log entries from a loaded worker (log role).
func (s *Server) handleLogIngest(w http.ResponseWriter, r *http.Request) {
	if s.Logs == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_logs", "log buffer not configured")
		return
	}
	ns, worker := r.URL.Query().Get("ns"), r.URL.Query().Get("worker")
	if ns == "" || worker == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "ns and worker are required")
		return
	}
	type logLine struct {
		Level       string            `json:"level"`
		Message     string            `json:"message"`
		MessageArgs []json.RawMessage `json:"message_args"`
		RequestID   string            `json:"request_id"`
		Traceparent string            `json:"traceparent"`
		AtMs        int64             `json:"at_ms"`
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		writeErr(w, http.StatusRequestEntityTooLarge, "logs_too_large", "log payload exceeds 1 MiB")
		return
	}
	var entries []logLine
	var batch struct {
		Events []logbuf.TailEvent `json:"events"`
	}
	if len(bytes.TrimSpace(body)) > 0 && bytes.TrimSpace(body)[0] == '[' {
		err = json.Unmarshal(body, &entries)
	} else {
		err = json.Unmarshal(body, &batch)
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	// A request-level traceparent is the fallback for entries that do not carry
	// their own (older log-tail versions).
	reqTP := r.Header.Get("traceparent")
	for _, event := range batch.Events {
		s.recordTailEvent(ns, worker, event, reqTP)
	}
	for _, e := range entries {
		tid, sid := telemetry.TraceIDs(e.Traceparent)
		if tid == "" {
			tid, sid = telemetry.TraceIDs(reqTP)
		}
		added := s.Logs.Add(logbuf.Entry{
			Namespace: ns, Worker: worker, Level: e.Level, Message: e.Message,
			MessageArgs: e.MessageArgs,
			RequestID:   e.RequestID, TraceID: tid, SpanID: sid, AtMs: e.AtMs,
		})
		// Optional OTLP export (ADR-172): off by default, and "tail" mode only
		// exports workers with an active subscription. Best-effort/non-blocking.
		telemetry.ExportLogWithMetadata(ns, worker, added.Level, added.Message, added.AtMs, added.TraceID, added.SpanID, telemetry.LogMetadata{MessageArgs: e.MessageArgs, RequestID: e.RequestID})
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "count": len(entries) + len(batch.Events)})
}

func (s *Server) recordTailEvent(ns, worker string, event logbuf.TailEvent, reqTP string) {
	s.recordTailEventWithExport(ns, worker, event, reqTP, true)
}

func (s *Server) recordTailEventWithExport(ns, worker string, event logbuf.TailEvent, reqTP string, export bool) {
	event.Namespace, event.Worker = ns, worker
	event.ScriptName = worker
	if event.EventTimestamp == 0 {
		event.EventTimestamp = time.Now().UnixMilli()
	}
	if event.Logs == nil {
		event.Logs = []logbuf.TailLog{}
	}
	if event.Exceptions == nil {
		event.Exceptions = []logbuf.TailException{}
	}
	s.Logs.AddEvent(event)
	tid, sid := telemetry.TraceIDs(event.Traceparent)
	if tid == "" {
		tid, sid = telemetry.TraceIDs(reqTP)
	}
	meta := telemetry.LogMetadata{EventType: event.EventType, ScriptVersion: event.ScriptVersion, RequestID: event.RequestID, Outcome: event.Outcome}
	if len(event.Exceptions) > 0 {
		if raw, err := json.Marshal(event.Exceptions); err == nil {
			meta.Exceptions = raw
		}
	}
	invocation := meta
	invocation.Invocation = true
	level := "info"
	if event.Outcome != "ok" {
		level = "error"
	}
	if export {
		telemetry.ExportLogWithMetadata(ns, worker, level, "invocation", event.EventTimestamp, tid, sid, invocation)
	}
	for _, line := range event.Logs {
		parts := make([]string, 0, len(line.Message))
		for _, arg := range line.Message {
			var value any
			if json.Unmarshal(arg, &value) == nil {
				if str, ok := value.(string); ok {
					parts = append(parts, str)
				} else {
					parts = append(parts, string(arg))
				}
			}
		}
		at := line.Timestamp
		if at == 0 {
			at = event.EventTimestamp
		}
		added := s.Logs.Add(logbuf.Entry{Namespace: ns, Worker: worker, Level: line.Level, Message: strings.Join(parts, " "), MessageArgs: line.Message, RequestID: event.RequestID, EventType: event.EventType, Outcome: event.Outcome, ScriptVersion: event.ScriptVersion, Exceptions: event.Exceptions, TraceID: tid, SpanID: sid, AtMs: at})
		lineMeta := meta
		lineMeta.MessageArgs = line.Message
		if export {
			telemetry.ExportLogWithMetadata(ns, worker, added.Level, added.Message, added.AtMs, added.TraceID, added.SpanID, lineMeta)
		}
	}
	for _, exception := range event.Exceptions {
		at := exception.Timestamp
		if at == 0 {
			at = event.EventTimestamp
		}
		message := fmt.Sprintf("%s: %s", exception.Name, exception.Message)
		added := s.Logs.Add(logbuf.Entry{Namespace: ns, Worker: worker, Level: "error", Message: message, RequestID: event.RequestID, EventType: event.EventType, Outcome: event.Outcome, ScriptVersion: event.ScriptVersion, Exceptions: event.Exceptions, TraceID: tid, SpanID: sid, AtMs: at})
		if export {
			telemetry.ExportLogWithMetadata(ns, worker, added.Level, added.Message, added.AtMs, added.TraceID, added.SpanID, meta)
		}
	}
}

// handleLogSubscribe enables OTLP log export for a worker for a TTL window
// (ADR-172); `cellhive tail` renews it on every poll so export stops shortly
// after the tail exits. Admin-authenticated.
func (s *Server) handleLogSubscribe(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Namespace string `json:"namespace"`
		Worker    string `json:"worker"`
		TTLMs     int64  `json:"ttl_ms"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	if req.Namespace == "" || req.Worker == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and worker are required")
		return
	}
	if !s.authorizeNS(w, r, req.Namespace) {
		return
	}
	ttl := time.Duration(req.TTLMs) * time.Millisecond
	exp := telemetry.Subscribe(req.Namespace, req.Worker, ttl)
	s.fanOutLogSubscribe(r.Context(), req.Namespace, req.Worker, ttl)
	writeJSON(w, http.StatusOK, map[string]any{
		"mode": telemetry.LogsMode(), "expires_ms": exp.UnixMilli(),
		"subscribed": telemetry.LogSubscribed(req.Namespace, req.Worker),
	})
}

// handleLogSubscribeInternal applies a peer's log-subscription fan-out
// (ADR-173) so every cell-agent exports logs for a tailed worker, whichever
// node serves it. Internal-token authenticated.
func (s *Server) handleLogSubscribeInternal(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Namespace string `json:"namespace"`
		Worker    string `json:"worker"`
		TTLMs     int64  `json:"ttl_ms"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	if req.Namespace == "" || req.Worker == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and worker are required")
		return
	}
	exp := telemetry.Subscribe(req.Namespace, req.Worker, time.Duration(req.TTLMs)*time.Millisecond)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "expires_ms": exp.UnixMilli()})
}

// fanOutLogSubscribe tells the other live cell-agents to export this worker's
// logs too, so a tail on one node covers workers served anywhere. Best-effort
// (fire-and-forget with a short timeout) and throttled per worker; the TTL on
// each node bounds staleness if a node misses an update.
func (s *Server) fanOutLogSubscribe(ctx context.Context, ns, worker string, ttl time.Duration) {
	if s.Lease == nil || s.Cfg.TokenInternal == "" {
		return
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	key := ns + "/" + worker
	now := time.Now()
	s.logFanMu.Lock()
	if s.logFan == nil {
		s.logFan = map[string]time.Time{}
	}
	if last, ok := s.logFan[key]; ok && now.Sub(last) < logFanoutEvery {
		s.logFanMu.Unlock()
		return
	}
	s.logFan[key] = now
	s.logFanMu.Unlock()

	leases := s.Lease.SampleCached(ctx, s.followerSampleTTL())
	body, err := json.Marshal(map[string]any{"namespace": ns, "worker": worker, "ttl_ms": ttl.Milliseconds()})
	if err != nil {
		return
	}
	for _, l := range leases {
		if l.Node == s.Cfg.NodeID || !l.Live(now) || l.Advertise == "" {
			continue
		}
		addr := l.Advertise
		if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
			addr = "http://" + addr
		}
		go func(addr string) {
			cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			req, rerr := http.NewRequestWithContext(cctx, http.MethodPost,
				strings.TrimRight(addr, "/")+"/v1/internal/logs/subscribe", bytes.NewReader(body))
			if rerr != nil {
				return
			}
			req.Header.Set("content-type", "application/json")
			req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenInternal)
			client := s.ForwardClient
			if client == nil {
				client = &http.Client{Timeout: 3 * time.Second}
			}
			if resp, derr := client.Do(req); derr == nil {
				_ = resp.Body.Close()
			}
		}(addr)
	}
}

// handleLogQuery returns buffered entries with seq > since (admin).
func (s *Server) handleLogQuery(w http.ResponseWriter, r *http.Request) {
	if s.Logs == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_logs", "log buffer not configured")
		return
	}
	q := r.URL.Query()
	ns, worker := q.Get("namespace"), q.Get("worker")
	if ns == "" || worker == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and worker are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	s.PullRuntimeLogs(r.Context(), ns, worker)
	var since uint64
	if v := q.Get("since"); v != "" {
		since, _ = strconv.ParseUint(v, 10, 64)
	}
	limit := 0
	if v := q.Get("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
	}
	entries := s.Logs.Since(ns, worker, since, limit)
	next := since
	if len(entries) > 0 {
		next = entries[len(entries)-1].Seq
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "next": next})
}
