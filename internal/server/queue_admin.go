package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strconv"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/ownerclient"
	queuepkg "cellhive/internal/queue"
	"cellhive/internal/scopedtoken"
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

// queueStatusPayload builds the queue status fields shared by the legacy
// /v1/control/queue/status endpoint and the per-domain /v1/queue/stats
// (ADR-156/157). The ADR-157 lag fields are additive.
func (s *Server) queueStatusPayload(ctx context.Context, ns, name string) (map[string]any, error) {
	st, err := s.Queue.Status(ctx, ns, name)
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
		if dlq, derr := s.Queue.Status(ctx, ns, consumer.DeadLetterQueue); derr == nil {
			out["dead_letter_depth"] = dlq.Depth
			out["dead_letter_visible"] = dlq.Visible
			out["dead_letter_oldest_visible_ms"] = dlq.OldestVisibleMs
		}
	}
	return out, nil
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
	// Run on the DLQ cell's owner so claim/ack are authoritative.
	if s.forwardOrClaim(w, r, queuepkg.Scope(ns, dlq), nil) {
		return
	}
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
// unowned. A remote owner receives an authenticated scoped send.
// The content type and idempotency key survive DLQ delivery and replay.
func (s *Server) EnqueueToQueueOwner(ctx context.Context, ns, queue string, m queuepkg.Message) error {
	return s.enqueueToQueueOwner(ctx, ns, queue, m)
}

// enqueueToQueueOwner is shared by the operator replay and queue runner.

func (s *Server) enqueueToQueueOwner(ctx context.Context, ns, queue string, m queuepkg.Message) error {
	sc := queuepkg.Scope(ns, queue)
	if s.Owner == nil {
		_, err := s.Queue.Send(ctx, ns, queue, m.Body, m.ContentType, 0, m.IdempotencyKey)
		return err
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
		return s.capturedWrite(ctx, sc, func() error {
			_, e := s.Queue.Send(ctx, ns, queue, m.Body, m.ContentType, 0, m.IdempotencyKey)
			return e
		})
	}
	tok, err := scopedtoken.Mint(s.scopeSecret(), scopedtoken.Claims{Namespace: ns, Kind: "queue", Name: queue})
	if err != nil {
		return err
	}
	url := ownerclient.OwnerURL(ownerclient.Hint{Address: addr}) + "/v1/queue/send?ns=" + ns + "&queue=" + queue
	if m.IdempotencyKey != "" {
		url += "&idempotency_key=" + neturl.QueryEscape(m.IdempotencyKey)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(m.Body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", m.ContentType)
	req.Header.Set("x-cellhive-scope-token", tok)
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
