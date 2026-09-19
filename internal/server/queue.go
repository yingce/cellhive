package server

import (
	"encoding/base64"
	"io"
	"net/http"
	"strconv"

	queuepkg "cellhive/internal/queue"
)

func (s *Server) requireQueue(w http.ResponseWriter) bool {
	if s.Queue == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_queue", "queue store not configured")
		return false
	}
	return true
}

// queueGate claims or forwards a queue-cell write (ADR-118).
func (s *Server) queueGate(w http.ResponseWriter, r *http.Request, ns, queue string) bool {
	if ns == "" || queue == "" {
		return false
	}
	return s.forwardOrClaim(w, r, queuepkg.Scope(ns, queue), nil)
}

func (s *Server) handleQueueSend(w http.ResponseWriter, r *http.Request) {
	if !s.requireQueue(w) {
		return
	}
	q := r.URL.Query()
	if s.queueGate(w, r, s.scopeNS(r), q.Get("queue")) {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	delay := 0
	if v := q.Get("delay_seconds"); v != "" {
		delay, _ = strconv.Atoi(v)
	}
	var id string
	ns, queue := s.scopeNS(r), q.Get("queue")
	if err := s.capturedWrite(r.Context(), queuepkg.Scope(ns, queue), func() error {
		var e error
		id, e = s.Queue.Send(r.Context(), ns, queue, body, r.Header.Get("content-type"), delay, q.Get("idempotency_key"))
		return e
	}); err != nil {
		s.captureErr(w, err, "queue_send_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id})
}

func (s *Server) handleQueueClaim(w http.ResponseWriter, r *http.Request) {
	if !s.requireQueue(w) {
		return
	}
	q := r.URL.Query()
	if s.queueGate(w, r, s.scopeNS(r), q.Get("queue")) {
		return
	}
	limit := 0
	if v := q.Get("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
	}
	lease := int64(0)
	if v := q.Get("lease_ms"); v != "" {
		lease, _ = strconv.ParseInt(v, 10, 64)
	}
	msgs, err := s.Queue.Claim(r.Context(), s.scopeNS(r), q.Get("queue"), limit, lease)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "queue_claim_failed", err.Error())
		return
	}
	out := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, map[string]any{
			"id": m.ID, "body": base64.StdEncoding.EncodeToString(m.Body),
			"content_type": m.ContentType, "attempts": m.Attempts,
			"visible_at_ms": m.VisibleAtMs, "lease_until_ms": m.LeaseUntilMs,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": out})
}

func (s *Server) handleQueueAck(w http.ResponseWriter, r *http.Request) {
	if !s.requireQueue(w) {
		return
	}
	q := r.URL.Query()
	if s.queueGate(w, r, s.scopeNS(r), q.Get("queue")) {
		return
	}
	if err := s.capturedWrite(r.Context(), queuepkg.Scope(s.scopeNS(r), q.Get("queue")), func() error {
		return s.Queue.Ack(r.Context(), s.scopeNS(r), q.Get("queue"), q.Get("id"))
	}); err != nil {
		s.captureErr(w, err, "queue_ack_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleQueueRetry(w http.ResponseWriter, r *http.Request) {
	if !s.requireQueue(w) {
		return
	}
	q := r.URL.Query()
	if s.queueGate(w, r, s.scopeNS(r), q.Get("queue")) {
		return
	}
	delay := int64(0)
	if v := q.Get("delay_ms"); v != "" {
		delay, _ = strconv.ParseInt(v, 10, 64)
	}
	if err := s.capturedWrite(r.Context(), queuepkg.Scope(s.scopeNS(r), q.Get("queue")), func() error {
		return s.Queue.Retry(r.Context(), s.scopeNS(r), q.Get("queue"), q.Get("id"), delay)
	}); err != nil {
		s.captureErr(w, err, "queue_retry_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
