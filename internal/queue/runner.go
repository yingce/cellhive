package queue

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Ref identifies a queue to poll and the worker that consumes it.
type Ref struct {
	Namespace string
	Name      string
	// Worker and BundleSHA identify the consuming worker's active version, so the
	// dispatch endpoint (user-runtime) can load it without resolving routing.
	Worker    string
	BundleSHA string
	// Version is the consuming worker's active version number. The runtime keys
	// its isolate by (worker, version, sha); a binding-only redeploy keeps the
	// sha and must still refresh the isolate/env (ADR-127).
	Version int
	// Consumer retry/dead-letter configuration (ADR-072).
	MaxRetries             int
	DeadLetterQueue        string
	MaxBatchSize           int
	MaxBatchTimeoutSeconds int
	// MaxConcurrency is how many batches may be in flight for this queue at once
	// (0/1 = sequential).
	MaxConcurrency int
}

// Dispatcher delivers a claimed batch to the worker that consumes the queue.
type Dispatcher interface {
	Dispatch(ctx context.Context, ref Ref, msgs []Message) error
}

// Runner polls registered queues, claims messages and dispatches batches. It is
// the consumer side of Queues (docs/bindings.md): claim with a lease, then Ack
// on success or Retry on failure, so at-least-once delivery is preserved.
type Runner struct {
	Store    *Store
	Queues   func(ctx context.Context) ([]Ref, error)
	Dispatch Dispatcher
	Batch    int
	LeaseMs  int64
	// RetryDelayMs is how long a failed batch stays invisible before retry.
	RetryDelayMs int64
	Interval     time.Duration
	Log          *slog.Logger
	// Filter, when set, decides whether this node should consume a queue (e.g.
	// OwnerGate: only queues whose cell this node owns, ADR-119). An error skips
	// the queue for this pass.
	Filter func(ctx context.Context, ref Ref) (bool, error)
	// Commit, when set, wraps every store mutation (claim/ack/retry/dead-letter)
	// so it is captured and proven durable before the runner proceeds (RPO=0).
	// nil means plain local writes (tests / capture disabled).
	Commit func(ctx context.Context, ns, name string, fn func() error) error
	// DeadLetterSend routes a dead letter through the destination queue's owner.
	// nil is only for local, non-replicated runners.
	DeadLetterSend func(ctx context.Context, ns, name string, m Message) error
}

// commit runs fn through the Commit hook when configured.
func (r *Runner) commit(ctx context.Context, ns, name string, fn func() error) error {
	if r.Commit == nil {
		return fn()
	}
	return r.Commit(ctx, ns, name, fn)
}

func (r *Runner) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}

func (r *Runner) retryDelay() int64 {
	if r.RetryDelayMs > 0 {
		return r.RetryDelayMs
	}
	return 30_000
}

// Start runs the consumer loop until ctx is cancelled.
func (r *Runner) Start(ctx context.Context) {
	go func() {
		interval := r.Interval
		if interval <= 0 {
			interval = time.Second
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := r.Pass(ctx); err != nil {
					r.log().Warn("queue runner pass failed", "err", err)
				}
			}
		}
	}()
}

// Pass runs one poll pass: for every queue, claim a batch and dispatch it.
// Acked messages are removed; a failed dispatch retries the whole batch.
func (r *Runner) Pass(ctx context.Context) (dispatched int, err error) {
	if r.Store == nil || r.Dispatch == nil || r.Queues == nil {
		return 0, nil
	}
	refs, err := r.Queues(ctx)
	if err != nil {
		return 0, err
	}
	for _, ref := range refs {
		if r.Filter != nil {
			ok, ferr := r.Filter(ctx, ref)
			if ferr != nil {
				r.log().Warn("queue filter failed", "ns", ref.Namespace, "queue", ref.Name, "err", ferr)
				continue
			}
			if !ok {
				continue
			}
		}
		dispatched += r.dispatchRef(ctx, ref)
	}
	return dispatched, nil
}

// dispatchRef dispatches one wave of up to Ref.MaxConcurrency batches (0/1 =
// sequential, the default), keeping them in flight concurrently. The next Pass
// takes the next wave, so a pass never blocks indefinitely on a busy queue.
// Batch order within a wave is not guaranteed, which matches the queue's
// at-least-once contract.
func (r *Runner) dispatchRef(ctx context.Context, ref Ref) int {
	conc := ref.MaxConcurrency
	if conc < 1 {
		conc = 1
	}
	batch := r.Batch
	if ref.MaxBatchSize > 0 {
		batch = ref.MaxBatchSize
	}
	batches := make([][]Message, 0, conc)
	for i := 0; i < conc; i++ {
		var msgs []Message
		cerr := r.commit(ctx, ref.Namespace, ref.Name, func() error {
			var e error
			msgs, e = r.Store.Claim(ctx, ref.Namespace, ref.Name, batch, r.LeaseMs)
			return e
		})
		if cerr != nil {
			r.log().Warn("queue claim failed", "ns", ref.Namespace, "queue", ref.Name, "err", cerr)
			break
		}
		if len(msgs) == 0 {
			break
		}
		batches = append(batches, msgs)
	}
	if len(batches) == 0 {
		return 0
	}
	total := 0
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, msgs := range batches {
		wg.Add(1)
		go func(msgs []Message) {
			defer wg.Done()
			n := r.dispatchBatch(ctx, ref, msgs)
			mu.Lock()
			total += n
			mu.Unlock()
		}(msgs)
	}
	wg.Wait()
	return total
}

// dispatchBatch delivers one claimed batch and acks (or retries/dead-letters)
// its messages, returning the number acked.
func (r *Runner) dispatchBatch(ctx context.Context, ref Ref, msgs []Message) int {
	var res DispatchResult
	var derr error
	if dd, ok := r.Dispatch.(DetailedDispatcher); ok {
		res, derr = dd.DispatchDetailed(ctx, ref, msgs)
	} else {
		derr = r.Dispatch.Dispatch(ctx, ref, msgs)
	}
	if derr != nil {
		for _, m := range msgs {
			// Attempts was incremented when the message was claimed above.
			if ref.MaxRetries > 0 && m.Attempts >= ref.MaxRetries {
				r.deadLetter(ctx, ref, m, derr)
				continue
			}
			if rerr := r.commit(ctx, ref.Namespace, ref.Name, func() error {
				return r.Store.Retry(ctx, ref.Namespace, ref.Name, m.ID, r.retryDelay())
			}); rerr != nil {
				r.log().Warn("queue retry failed", "ns", ref.Namespace, "queue", ref.Name, "id", m.ID, "err", rerr)
			}
		}
		r.log().Warn("queue dispatch failed; batch retried",
			"ns", ref.Namespace, "queue", ref.Name, "batch", len(msgs), "err", derr)
		return 0
	}
	// Per-message outcomes (ADR-155): retried ids first (honoring retry()
	// delaySeconds), everything else implicitly acked on a successful dispatch.
	retryDelay := map[string]int64{}
	for _, rs := range res.Retry {
		retryDelay[rs.ID] = int64(rs.DelaySeconds) * 1000
	}
	acked := 0
	for _, m := range msgs {
		if delayMs, retry := retryDelay[m.ID]; retry {
			if ref.MaxRetries > 0 && m.Attempts >= ref.MaxRetries {
				r.deadLetter(ctx, ref, m, fmt.Errorf("retry requested by consumer"))
				continue
			}
			if rerr := r.commit(ctx, ref.Namespace, ref.Name, func() error {
				return r.Store.Retry(ctx, ref.Namespace, ref.Name, m.ID, delayMs)
			}); rerr != nil {
				r.log().Warn("queue retry failed", "ns", ref.Namespace, "queue", ref.Name, "id", m.ID, "err", rerr)
			}
			continue
		}
		if aerr := r.commit(ctx, ref.Namespace, ref.Name, func() error {
			return r.Store.Ack(ctx, ref.Namespace, ref.Name, m.ID)
		}); aerr != nil {
			r.log().Warn("queue ack failed", "ns", ref.Namespace, "queue", ref.Name, "id", m.ID, "err", aerr)
			continue
		}
		acked++
	}
	return acked
}

// deadLetter moves a message that exhausted its retries to the configured
// dead-letter queue, or drops it (with a warning) when none is configured.
// The idempotency key travels with the replayed send so the DLQ message
// dedupes against any still-live twin (ADR-182 review fix).
func (r *Runner) deadLetter(ctx context.Context, ref Ref, m Message, cause error) {
	if ref.DeadLetterQueue != "" {
		send := func() error {
			return r.commit(ctx, ref.Namespace, ref.DeadLetterQueue, func() error {
				_, e := r.Store.Send(ctx, ref.Namespace, ref.DeadLetterQueue, m.Body, m.ContentType, 0, m.IdempotencyKey)
				return e
			})
		}
		var err error
		if r.DeadLetterSend != nil {
			err = r.DeadLetterSend(ctx, ref.Namespace, ref.DeadLetterQueue, m)
		} else {
			err = send()
		}
		if err != nil {
			r.log().Warn("queue dead-letter send failed", "ns", ref.Namespace, "queue", ref.Name,
				"dlq", ref.DeadLetterQueue, "id", m.ID, "err", err)
			return
		}
		if err := r.commit(ctx, ref.Namespace, ref.Name, func() error {
			return r.Store.Ack(ctx, ref.Namespace, ref.Name, m.ID)
		}); err != nil {
			r.log().Warn("queue ack after dead-letter failed", "ns", ref.Namespace, "queue", ref.Name, "id", m.ID, "err", err)
			return
		}
		r.log().Warn("queue message dead-lettered", "ns", ref.Namespace, "queue", ref.Name,
			"dlq", ref.DeadLetterQueue, "id", m.ID, "attempts", m.Attempts, "err", cause)
		return
	}
	if err := r.commit(ctx, ref.Namespace, ref.Name, func() error {
		return r.Store.Ack(ctx, ref.Namespace, ref.Name, m.ID)
	}); err != nil {
		r.log().Warn("queue drop-ack failed", "ns", ref.Namespace, "queue", ref.Name, "id", m.ID, "err", err)
		return
	}
	r.log().Warn("queue message dropped (retries exhausted, no dead-letter queue)",
		"ns", ref.Namespace, "queue", ref.Name, "id", m.ID, "attempts", m.Attempts, "err", cause)
}

// HTTPDispatcher POSTs claimed batches to user-runtime (/v1/queues/dispatch).
type HTTPDispatcher struct {
	URL    string
	Token  string
	Client *http.Client
}

// NewHTTP builds an HTTP dispatcher. An empty URL yields nil so callers can skip
// enabling the consumer loop.
func NewHTTP(url, token string) *HTTPDispatcher {
	url = strings.TrimSpace(url)
	if url == "" {
		return nil
	}
	return &HTTPDispatcher{URL: strings.TrimRight(url, "/"), Token: token, Client: &http.Client{Timeout: 30 * time.Second}}
}

// DispatchResult reports per-message consumer outcomes (CF Queue semantics):
// explicitly acked ids and ids to retry with an optional delay. Messages that
// are neither are implicitly acked on a successful dispatch (ADR-155).
type DispatchResult struct {
	Ack   []string    `json:"ack,omitempty"`
	Retry []RetrySpec `json:"retry,omitempty"`
}

// RetrySpec is one message to retry after DelaySeconds.
type RetrySpec struct {
	ID           string `json:"id"`
	DelaySeconds int    `json:"delay_seconds,omitempty"`
}

// DetailedDispatcher is implemented by dispatchers that can report per-message
// ack/retry. The runner falls back to whole-batch semantics otherwise.
type DetailedDispatcher interface {
	DispatchDetailed(ctx context.Context, ref Ref, msgs []Message) (DispatchResult, error)
}

type dispatchMessage struct {
	ID          string `json:"id"`
	Body        string `json:"body"` // base64
	ContentType string `json:"content_type"`
	Attempts    int    `json:"attempts"`
}

// Dispatch delivers a batch to the consumer endpoint (whole-batch semantics).
func (d *HTTPDispatcher) Dispatch(ctx context.Context, ref Ref, msgs []Message) error {
	_, err := d.DispatchDetailed(ctx, ref, msgs)
	return err
}

// DispatchDetailed delivers a batch and decodes the per-message ack/retry
// outcomes the consumer reports (ADR-155).
func (d *HTTPDispatcher) DispatchDetailed(ctx context.Context, ref Ref, msgs []Message) (DispatchResult, error) {
	out := make([]dispatchMessage, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, dispatchMessage{
			ID:          m.ID,
			Body:        base64.StdEncoding.EncodeToString(m.Body),
			ContentType: m.ContentType,
			Attempts:    m.Attempts,
		})
	}
	body, err := json.Marshal(map[string]any{
		"namespace":  ref.Namespace,
		"queue":      ref.Name,
		"worker":     ref.Worker,
		"bundle_sha": ref.BundleSHA,
		"version":    ref.Version,
		"messages":   out,
	})
	if err != nil {
		return DispatchResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL+"/v1/queues/dispatch", bytes.NewReader(body))
	if err != nil {
		return DispatchResult{}, err
	}
	req.Header.Set("content-type", "application/json")
	if d.Token != "" {
		req.Header.Set("x-cellhive-internal-token", d.Token)
	}
	resp, err := d.Client.Do(req)
	if err != nil {
		return DispatchResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return DispatchResult{}, fmt.Errorf("queue dispatch: %s", resp.Status)
	}
	var res DispatchResult
	// The response is an envelope ({ok, result, ack, retry}); a body we cannot
	// decode still counts as a successful whole-batch ack.
	data, rerr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if rerr == nil && len(data) > 0 {
		_ = json.Unmarshal(data, &res)
	}
	return res, nil
}
