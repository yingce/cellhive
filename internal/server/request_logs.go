package server

import (
	"cellhive/internal/telemetry"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// requestLogPayload is the small, best-effort HTTP request event emitted by the
// user-runtime after a worker invocation. It bypasses the tail pull so a
// function_request exists even when no tenant tail subscription is attached.
type requestLogPayload struct {
	Namespace   string `json:"namespace"`
	Worker      string `json:"worker"`
	RequestID   string `json:"request_id"`
	Method      string `json:"method"`
	Path        string `json:"path"`
	Route       string `json:"route"`
	Status      int    `json:"status"`
	StartedMs   int64  `json:"started_ms"`
	FinishedMs  int64  `json:"finished_ms"`
	Outcome     string `json:"outcome"`
	Traceparent string `json:"traceparent"`
}

func (s *Server) handleRequestLog(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 64*1024+1))
	if err != nil || len(raw) > 64*1024 {
		writeErr(w, http.StatusBadRequest, "invalid_request_log", "record too large")
		return
	}
	var p requestLogPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request_log", err.Error())
		return
	}
	if p.Namespace == "" || p.Worker == "" || p.Path == "" || p.Status < 100 || p.Status > 599 {
		writeErr(w, http.StatusBadRequest, "invalid_request_log", "namespace, worker, path and status are required")
		return
	}
	if len(p.RequestID) > 256 || len(p.Path) > 4096 || len(p.Method) > 32 || len(p.Route) > 256 {
		writeErr(w, http.StatusBadRequest, "invalid_request_log", "field too large")
		return
	}
	if p.FinishedMs <= 0 {
		p.FinishedMs = time.Now().UnixMilli()
	}
	if p.StartedMs <= 0 || p.StartedMs > p.FinishedMs {
		p.StartedMs = p.FinishedMs
	}
	if p.Outcome == "" {
		if p.Status >= 500 {
			p.Outcome = "error"
		} else {
			p.Outcome = "ok"
		}
	}
	traceID, spanID := telemetry.TraceIDs(p.Traceparent)
	level := "info"
	if p.Outcome == "error" || p.Status >= 500 {
		level = "error"
	}
	telemetry.ExportLogWithMetadata(p.Namespace, p.Worker, level, "invocation", p.FinishedMs, traceID, spanID, telemetry.LogMetadata{
		EventType: "fetch", RequestID: p.RequestID, Method: p.Method, Path: p.Path, Route: p.Route,
		Status: p.Status, StartedMs: p.StartedMs, FinishedMs: p.FinishedMs, Outcome: p.Outcome, Invocation: true,
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
