package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	queuepkg "cellhive/internal/queue"
)

// queueWrite publishes a local scheduling hint only after durable capture.
func (s *Server) queueWrite(ctx context.Context, ns, name string, write func() error) error {
	if err := s.capturedWrite(ctx, queuepkg.Scope(ns, name), write); err != nil {
		return err
	}
	if s.QueueNudge != nil {
		s.QueueNudge(ns, name)
	}
	return nil
}

const maxQueueBodyBytes = 128 * 1024

// Preserve the original URL for forwarding/authentication. Only local storage
// and ownership use the physical ID resolved by binding authorization.
func queueStorageName(r *http.Request) string {
	if id, ok := r.Context().Value(ctxQueueStorage).(string); ok && id != "" {
		return id
	}
	return r.URL.Query().Get("queue")
}

func (s *Server) handleQueueTransfer(w http.ResponseWriter, r *http.Request) {
	if !s.requireQueue(w) {
		return
	}
	ns, name := r.URL.Query().Get("ns"), r.URL.Query().Get("queue")
	if err := s.PrepareResourceAccess(r.Context(), queuepkg.Scope(ns, name)); err != nil {
		writeErr(w, 410, "resource_deleted", err.Error())
		return
	}
	if s.queueGate(w, r, ns, name) {
		return
	}
	var message queuepkg.Message
	if err := json.NewDecoder(io.LimitReader(r.Body, 2*1024*1024)).Decode(&message); err != nil {
		writeErr(w, 400, "queue_invalid_transfer", err.Error())
		return
	}
	var id string
	if err := s.recordQueueDestination(r.Context(), ns, name, message); err != nil {
		s.captureErr(w, err, "queue_transfer_failed")
		return
	}
	end, err := s.BeginResourceRequest(r.Context(), queuepkg.Scope(ns, name))
	if err != nil {
		s.captureErr(w, err, "queue_transfer_failed")
		return
	}
	defer end()
	if err := s.queueWrite(r.Context(), ns, name, func() error {
		var err error
		id, err = s.Queue.TransferValidated(r.Context(), ns, name, message, func(ctx context.Context) error { return s.validateQueueTransfer(ctx, ns, message) })
		return err
	}); err != nil {
		s.captureErr(w, err, "queue_transfer_failed")
		return
	}
	writeJSON(w, 200, map[string]any{"id": id})
}

func queueContentType(r *http.Request) string {
	if v := r.Header.Get("x-cellhive-queue-content-type"); v != "" {
		return v
	}
	if r.Header.Get("content-type") == "text/plain" {
		return "text"
	}
	return "json"
}

// Correlation is best effort: an oversized caller header must not prevent an
// otherwise valid business message from being enqueued.
func queueProducerContext(r *http.Request) (requestID, traceparent string) {
	requestID, traceparent = r.Header.Get("x-request-id"), r.Header.Get("traceparent")
	if len(requestID) > 160 {
		requestID = ""
	}
	if len(traceparent) > 256 {
		traceparent = ""
	}
	return
}

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
	if s.queueGate(w, r, s.scopeNS(r), queueStorageName(r)) {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxQueueBodyBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	if len(body) > maxQueueBodyBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "queue_body_too_large", "queue message exceeds 128 KiB")
		return
	}
	delay := 0
	if v := q.Get("delay_seconds"); v != "" {
		delay, _ = strconv.Atoi(v)
	}
	if delay < 0 || delay > 86400 {
		writeErr(w, http.StatusBadRequest, "queue_invalid_delay", "delaySeconds must be between 0 and 86400")
		return
	}
	contentType := queueContentType(r)
	if contentType != "json" && contentType != "text" && contentType != "bytes" && contentType != "v8" {
		writeErr(w, http.StatusBadRequest, "queue_invalid_content_type", "contentType must be json, text, bytes, or v8")
		return
	}
	var id string
	ns, queue := s.scopeNS(r), queueStorageName(r)
	producerID, producerTrace := queueProducerContext(r)
	producer := queuepkg.SendMessage{Body: body, ContentType: contentType, DelaySeconds: delay, IdempotencyKey: q.Get("idempotency_key"), ProducerRequestID: producerID, ProducerTraceparent: producerTrace}
	if err := s.queueWrite(r.Context(), ns, queue, func() error {
		var e error
		var ids []string
		ids, e = s.Queue.SendBatch(r.Context(), ns, queue, []queuepkg.SendMessage{producer})
		if e == nil {
			id = ids[0]
		}
		return e
	}); err != nil {
		s.captureErr(w, err, "queue_send_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id})
}

type queueBatchRequest struct {
	Messages []struct {
		Body           string          `json:"body"`
		ContentType    string          `json:"contentType"`
		DelaySeconds   int             `json:"delaySeconds"`
		IdempotencyKey string          `json:"idempotencyKey"`
		Metadata       json.RawMessage `json:"metadata"`
	} `json:"messages"`
}

func (s *Server) handleQueueSendBatch(w http.ResponseWriter, r *http.Request) {
	if !s.requireQueue(w) {
		return
	}
	ns, name := s.scopeNS(r), queueStorageName(r)
	if s.queueGate(w, r, ns, name) {
		return
	}
	var req queueBatchRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 2*1024*1024))
	if err := dec.Decode(&req); err != nil || len(req.Messages) == 0 || len(req.Messages) > 100 {
		writeErr(w, http.StatusBadRequest, "queue_invalid_batch", "messages must contain 1..100 entries")
		return
	}
	msgs := make([]queuepkg.SendMessage, len(req.Messages))
	producerID, producerTrace := queueProducerContext(r)
	for i, m := range req.Messages {
		body, err := base64.StdEncoding.DecodeString(m.Body)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "queue_invalid_body", "body must be base64")
			return
		}
		msgs[i] = queuepkg.SendMessage{Body: body, ContentType: m.ContentType, DelaySeconds: m.DelaySeconds, IdempotencyKey: m.IdempotencyKey, Metadata: m.Metadata, ProducerRequestID: producerID, ProducerTraceparent: producerTrace}
	}
	var ids []string
	if err := s.queueWrite(r.Context(), ns, name, func() error { var e error; ids, e = s.Queue.SendBatch(r.Context(), ns, name, msgs); return e }); err != nil {
		s.captureErr(w, err, "queue_send_batch_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ids": ids})
}

func (s *Server) handleQueueClaim(w http.ResponseWriter, r *http.Request) {
	if !s.requireQueue(w) {
		return
	}
	q := r.URL.Query()
	if s.queueGate(w, r, s.scopeNS(r), queueStorageName(r)) {
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
	ns, name := s.scopeNS(r), queueStorageName(r)
	var msgs []queuepkg.Message
	if err := s.queueWrite(r.Context(), ns, name, func() error {
		var err error
		msgs, err = s.Queue.Claim(r.Context(), ns, name, limit, lease)
		return err
	}); err != nil {
		s.captureErr(w, err, "queue_claim_failed")
		return
	}
	out := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, map[string]any{
			"id": m.ID, "body": base64.StdEncoding.EncodeToString(m.Body),
			"content_type": m.ContentType, "attempts": m.Attempts,
			"timestamp": m.TimestampMs, "metadata": m.Metadata,
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
	if s.queueGate(w, r, s.scopeNS(r), queueStorageName(r)) {
		return
	}
	if err := s.queueWrite(r.Context(), s.scopeNS(r), queueStorageName(r), func() error {
		return s.Queue.Ack(r.Context(), s.scopeNS(r), queueStorageName(r), q.Get("id"))
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
	if s.queueGate(w, r, s.scopeNS(r), queueStorageName(r)) {
		return
	}
	delay := int64(0)
	if v := q.Get("delay_ms"); v != "" {
		delay, _ = strconv.ParseInt(v, 10, 64)
	}
	if err := s.queueWrite(r.Context(), s.scopeNS(r), queueStorageName(r), func() error {
		return s.Queue.Retry(r.Context(), s.scopeNS(r), queueStorageName(r), q.Get("id"), delay)
	}); err != nil {
		s.captureErr(w, err, "queue_retry_failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
