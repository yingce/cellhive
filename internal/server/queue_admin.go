package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strconv"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/ownerclient"
	queuepkg "cellhive/internal/queue"
)

// R4 (ADR-156): queue observability and dead-letter replay for operators.

// queueConsumerTarget finds the consumer config of (ns, queue) in the projection.
func (s *Server) queueConsumerTarget(ctx context.Context, ns, queue string) (control.Consumer, bool) {
	if s.Control == nil {
		return control.Consumer{}, false
	}
	proj, err := s.Control.Projection(ctx)
	if err != nil {
		return control.Consumer{}, false
	}
	for _, t := range proj.QueueTargets() {
		if t.Namespace == ns && t.Queue == queue {
			return t.Consumer, true
		}
	}
	return control.Consumer{}, false
}

// handleControlQueueStatus reports a queue's depth plus its dead-letter queue's
// depth (when a consumer declares one).
func (s *Server) handleControlQueueStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requireQueue(w) {
		return
	}
	q := r.URL.Query()
	ns, name := q.Get("namespace"), q.Get("queue")
	if ns == "" || name == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and queue are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	if err := s.PrepareResourceAccess(r.Context(), queuepkg.Scope(ns, name)); err != nil {
		writeErr(w, 410, "resource_deleted", err.Error())
		return
	}
	if s.forwardRead(w, r, queuepkg.Scope(ns, name), nil) {
		return
	}
	if !s.requireControl(w) {
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	out, err := s.queueStatusPayload(r.Context(), ns, name)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "queue_status_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// handleControlQueueDLQPreview reads DLQ messages without Claim: attempts,
// leases and visibility remain unchanged. The queue query names the main queue;
// its configured independent DLQ is resolved from the projection.
func (s *Server) deadLetterConsumer(r *http.Request, ns, name string) (control.Consumer, bool) {
	if r.URL.Query().Get("direct") == "1" {
		if s.Control == nil {
			return control.Consumer{}, false
		}
		_, ok, err := s.Control.Binding(r.Context(), ns, "queue", name)
		return control.Consumer{DeadLetterQueue: name}, ok && err == nil
	}
	return s.queueConsumerTarget(r.Context(), ns, name)
}

func (s *Server) handleControlQueueDLQPreview(w http.ResponseWriter, r *http.Request) {
	if !s.requireQueue(w) {
		return
	}
	q := r.URL.Query()
	ns, name := q.Get("namespace"), q.Get("queue")
	if ns == "" || name == "" {
		writeErr(w, 400, "bad_request", "namespace and queue are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	consumer, ok := s.deadLetterConsumer(r, ns, name)
	if !ok || consumer.DeadLetterQueue == "" {
		writeErr(w, 400, "no_dead_letter_queue", "queue has no configured dead letter queue")
		return
	}
	if err := s.PrepareResourceAccess(r.Context(), queuepkg.Scope(ns, consumer.DeadLetterQueue)); err != nil {
		writeErr(w, 410, "resource_deleted", err.Error())
		return
	}
	if s.forwardRead(w, r, queuepkg.Scope(ns, consumer.DeadLetterQueue), nil) {
		return
	}
	end, err := s.BeginResourceRequest(r.Context(), queuepkg.Scope(ns, consumer.DeadLetterQueue))
	if err != nil {
		writeErr(w, 410, "resource_deleted", err.Error())
		return
	}
	defer end()
	limit := 100
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 && n <= 1000 {
		limit = n
	}
	after := ""
	if cursor := q.Get("cursor"); cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		var v struct{ Namespace, Queue, After string }
		if err != nil || json.Unmarshal(raw, &v) != nil || v.Namespace != ns || v.Queue != consumer.DeadLetterQueue {
			writeErr(w, 400, "invalid_cursor", "cursor must belong to this namespace and DLQ")
			return
		}
		after = v.After
	}
	items, next, err := s.Queue.PeekPage(r.Context(), ns, consumer.DeadLetterQueue, after, limit)
	if err != nil {
		writeErr(w, 500, "dlq_preview_failed", err.Error())
		return
	}
	if next != "" {
		raw, _ := json.Marshal(struct{ Namespace, Queue, After string }{ns, consumer.DeadLetterQueue, next})
		next = base64.RawURLEncoding.EncodeToString(raw)
	}
	writeJSON(w, 200, map[string]any{"namespace": ns, "queue": name, "dead_letter_queue": consumer.DeadLetterQueue, "messages": items, "next_cursor": next, "body_encoding": "base64"})
}

// handleControlQueueDLQAck is idempotent: Ack deletes the selected message if
// present and returns success when it is already absent.
func (s *Server) handleControlQueueDLQAck(w http.ResponseWriter, r *http.Request) {
	if !s.requireQueue(w) {
		return
	}
	q := r.URL.Query()
	ns, name, id := q.Get("namespace"), q.Get("queue"), q.Get("message_id")
	if ns == "" || name == "" {
		writeErr(w, 400, "bad_request", "namespace and queue are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	consumer, ok := s.deadLetterConsumer(r, ns, name)
	if !ok || consumer.DeadLetterQueue == "" {
		writeErr(w, 400, "no_dead_letter_queue", "queue has no configured dead letter queue")
		return
	}
	if err := s.PrepareResourceAccess(r.Context(), queuepkg.Scope(ns, consumer.DeadLetterQueue)); err != nil {
		writeErr(w, 410, "resource_deleted", err.Error())
		return
	}
	scope := queuepkg.Scope(ns, consumer.DeadLetterQueue)
	if s.forwardOrClaim(w, r, scope, nil) {
		return
	}
	end, err := s.BeginResourceRequest(r.Context(), scope)
	if err != nil {
		writeErr(w, 410, "resource_deleted", err.Error())
		return
	}
	defer end()
	ids := []string{id}
	if id == "" {
		var body struct {
			IDs []string `json:"message_ids"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 32*1024)).Decode(&body); err != nil {
			writeErr(w, 400, "bad_request", "message_id or message_ids is required")
			return
		}
		ids = body.IDs
	}
	if len(ids) < 1 || len(ids) > 100 {
		writeErr(w, 400, "bad_request", "message_ids requires 1..100 entries")
		return
	}
	for _, id := range ids {
		if id == "" || len(id) > 256 {
			writeErr(w, 400, "bad_request", "invalid message ID")
			return
		}
	}
	if err := s.capturedWrite(r.Context(), scope, func() error { return s.Queue.AckMany(r.Context(), ns, consumer.DeadLetterQueue, ids) }); err != nil {
		writeErr(w, 500, "dlq_ack_failed", err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "message_ids": ids})
}

// queueStatusPayload builds the queue status fields shared by the legacy
// /v1/control/queue/status endpoint and the per-domain /v1/queue/stats
// (ADR-156/157). The ADR-157 lag fields are additive.
func (s *Server) queueStatusPayload(ctx context.Context, ns, name string) (map[string]any, error) {
	st, err := s.readQueueStatus(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"namespace": ns, "queue": name,
		"depth": st.Depth, "visible": st.Visible, "leased": st.Leased,
		"oldest_visible_ms": st.OldestVisibleMs, "max_attempts": st.MaxAttempts,
	}
	if consumer, ok := s.queueConsumerTarget(ctx, ns, name); ok && consumer.DeadLetterQueue != "" {
		out["dead_letter_queue"] = consumer.DeadLetterQueue
		if s.resourceReplica() != nil {
			blocked, err := s.resourceReplica().ResourceBlocked(ctx, queuepkg.Scope(ns, consumer.DeadLetterQueue))
			if err != nil {
				return nil, err
			}
			if blocked {
				out["dead_letter_deleted"] = true
				return out, nil
			}
		}
		if dlq, derr := s.readQueueStatus(ctx, ns, consumer.DeadLetterQueue); derr == nil {
			out["dead_letter_depth"] = dlq.Depth
			out["dead_letter_visible"] = dlq.Visible
			out["dead_letter_oldest_visible_ms"] = dlq.OldestVisibleMs
		}
	}
	return out, nil
}

func (s *Server) readQueueStatus(ctx context.Context, ns, name string) (queuepkg.Status, error) {
	if err := s.PrepareResourceAccess(ctx, queuepkg.Scope(ns, name)); err != nil {
		return queuepkg.Status{}, err
	}
	end, err := s.BeginResourceRequest(ctx, queuepkg.Scope(ns, name))
	if err != nil {
		return queuepkg.Status{}, err
	}
	defer end()
	return s.Queue.Status(ctx, ns, name)
}

// handleControlQueueReplayDLQ moves up to limit messages from a queue's dead
// letter queue back onto the main queue (ADR-156). It runs on the DLQ cell's
// owner (so the claim/ack are authoritative) and re-enqueues through the main
// queue's owner.
func (s *Server) handleControlQueueReplayDLQ(w http.ResponseWriter, r *http.Request) {
	if !s.requireQueue(w) {
		return
	}
	q := r.URL.Query()
	ns, name := q.Get("namespace"), q.Get("queue")
	if ns == "" || name == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace and queue are required")
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}
	limit := 100
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	consumer, ok := s.queueConsumerTarget(r.Context(), ns, name)
	if !ok || consumer.DeadLetterQueue == "" {
		writeErr(w, http.StatusBadRequest, "no_dead_letter_queue",
			"queue "+ns+"/"+name+" has no consumer with a dead_letter_queue")
		return
	}
	dlq := consumer.DeadLetterQueue
	for _, target := range []string{name, dlq} {
		if err := s.PrepareResourceAccess(r.Context(), queuepkg.Scope(ns, target)); err != nil {
			writeErr(w, 410, "resource_deleted", err.Error())
			return
		}
	}
	// Run on the DLQ cell's owner so claim/ack are authoritative.
	if s.forwardOrClaim(w, r, queuepkg.Scope(ns, dlq), nil) {
		return
	}
	end, err := s.BeginResourceRequest(r.Context(), queuepkg.Scope(ns, dlq))
	if err != nil {
		writeErr(w, 410, "resource_deleted", err.Error())
		return
	}
	defer end()
	if !s.requireControl(w) {
		return
	}
	if !s.authorizeNS(w, r, ns) {
		return
	}

	var msgs []queuepkg.Message
	if err := s.capturedWrite(r.Context(), queuepkg.Scope(ns, dlq), func() error {
		var e error
		msgs, e = s.Queue.Claim(r.Context(), ns, dlq, limit, 30_000)
		return e
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "queue_replay_claim_failed", err.Error())
		return
	}
	replayed := 0
	var failed []string
	for _, m := range msgs {
		if err := s.enqueueToQueueOwner(r.Context(), ns, name, m); err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", m.ID, err))
			continue
		}
		if err := s.capturedWrite(r.Context(), queuepkg.Scope(ns, dlq), func() error {
			return s.Queue.Ack(r.Context(), ns, dlq, m.ID)
		}); err != nil {
			failed = append(failed, fmt.Sprintf("%s: ack: %v", m.ID, err))
			continue
		}
		replayed++
	}
	resp := map[string]any{
		"ok": len(failed) == 0, "namespace": ns, "queue": name, "dead_letter_queue": dlq,
		"claimed": len(msgs), "replayed": replayed,
	}
	if len(failed) > 0 {
		resp["errors"] = failed
		writeJSON(w, http.StatusInternalServerError, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// EnqueueToQueueOwner sends to the destination queue's owner, claiming it if
// unowned. A remote owner receives an internal-authenticated transfer.
// The content type, metadata and business key survive DLQ delivery and replay.
func (s *Server) EnqueueToQueueOwner(ctx context.Context, ns, queue string, m queuepkg.Message) error {
	return s.enqueueToQueueOwner(ctx, ns, queue, m)
}

// enqueueToQueueOwner is shared by the operator replay and queue runner.

func (s *Server) enqueueToQueueOwner(ctx context.Context, ns, queue string, m queuepkg.Message) error {
	sc := queuepkg.Scope(ns, queue)
	if err := s.PrepareResourceAccess(ctx, sc); err != nil {
		return err
	}
	if m.SourceQueue != "" && m.SourceQueue != queue {
		if err := s.PrepareResourceAccess(ctx, queuepkg.Scope(ns, m.SourceQueue)); err != nil {
			return err
		}
	}
	if s.Owner == nil {
		return s.acceptLocalQueueTransfer(ctx, ns, queue, m)
	}
	now := time.Now()
	addr := ""
	owned := false
	if o, _, err := s.Owner.Resolve(ctx, sc); err == nil && !o.Expired(now) {
		if o.Node == s.Cfg.NodeID {
			owned = true
		} else if o.Address != "" {
			addr = o.Address
		}
	}
	if !owned && addr == "" {
		s.invalidateStaleLocal(ctx, sc)
		if _, err := s.Owner.Claim(ctx, sc, now); err != nil {
			// Lost the race to a peer: route to its current owner.
			if o, _, rerr := s.Owner.Resolve(ctx, sc); rerr == nil && o.Node != s.Cfg.NodeID && o.Address != "" {
				addr = o.Address
			} else {
				return fmt.Errorf("queue owner unavailable: %v", err)
			}
		} else {
			owned = true
			s.registerPendingTimers(context.Background(), sc)
		}
	}
	if owned {
		return s.acceptLocalQueueTransfer(ctx, ns, queue, m)
	}
	url := ownerclient.OwnerURL(ownerclient.Hint{Address: addr}) + "/v1/internal/queue/transfer?ns=" + neturl.QueryEscape(ns) + "&queue=" + neturl.QueryEscape(queue)
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenInternal)
	client := s.ForwardClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("owner send: %s: %s", resp.Status, string(bytes.TrimSpace(body)))
	}
	return nil
}
