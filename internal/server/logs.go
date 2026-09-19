package server

import (
	"bytes"
	"context"
	"encoding/json"
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
	var entries []struct {
		Level     string `json:"level"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
		AtMs      int64  `json:"at_ms"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&entries); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	for _, e := range entries {
		added := s.Logs.Add(logbuf.Entry{Namespace: ns, Worker: worker, Level: e.Level, Message: e.Message, RequestID: e.RequestID, AtMs: e.AtMs})
		// Optional OTLP export (ADR-172): off by default, and "tail" mode only
		// exports workers with an active subscription. Best-effort/non-blocking.
		telemetry.ExportLog(ns, worker, added.Level, added.Message, added.AtMs)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "count": len(entries)})
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

	leases := s.Lease.SampleCached(ctx, followerSampleTTL)
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
