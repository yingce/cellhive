package queue

import (
	"bytes"
	"cellhive/internal/bearertoken"
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
	Namespace   string
	Name        string // physical storage ID
	BindingName string // business name exposed to the worker; empty means Name
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
	RetryDelaySeconds      int
	DeadLetterQueue        string
	MaxBatchSize           int
	MaxBatchTimeoutSeconds int
	// MaxConcurrency is how many batches may be in flight for this queue at once
	// (0/1 = sequential).
	MaxConcurrency int
}

func (r Ref) BusinessName() string {
	if r.BindingName != "" {
		return r.BindingName
	}
	return r.Name
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
	// Resolve reads the current consumer by physical queue identity.
	Resolve   func(context.Context, string, string) (Ref, bool, error)
	wakeMu    sync.Mutex
	deadlines map[[2]string]time.Time
	Log       *slog.Logger
	// Filter, when set, decides whether this node should consume a queue (e.g.
	// OwnerGate: only queues whose cell this node owns, ADR-119). An error skips
	// the queue for this pass.
	Filter        func(ctx context.Context, ref Ref) (bool, error)
	AdmitDispatch func(context.Context, Ref) (func(), error)
	// AdmitRead protects BatchReady only; release precedes mutation admission.
	AdmitRead func(context.Context, Ref) (func(), error)
	// ValidateDispatch runs under admission and must not claim, reset or evict.
	ValidateDispatch func(context.Context, Ref) (bool, error)
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

func (r *Runner) retryDelayFor(ref Ref) int64 {
	if ref.RetryDelaySeconds > 0 {
		return int64(ref.RetryDelaySeconds) * 1000
	}
	return r.retryDelay()
}

func retriesFor(ref Ref) int {
	if ref.MaxRetries > 0 {
		return ref.MaxRetries
	}
	return 3
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
		var reconcileAt time.Time
		for {
			now := time.Now()
			if r.Resolve == nil {
				if _, err := r.Pass(ctx); err != nil {
					r.log().Warn("queue pass failed", "err", err)
				}
			} else {
				reconcileAt = r.scheduledPass(ctx, now, reconcileAt)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}

func (r *Runner) scheduledPass(ctx context.Context, now, reconcileAt time.Time) time.Time {
	if !now.Before(reconcileAt) {
		if err := r.reconcile(ctx); err != nil {
			r.log().Warn("queue reconciliation failed", "err", err)
			reconcileAt = now.Add(5 * time.Second)
		} else {
			reconcileAt = now.Add(time.Minute)
		}
	}
	r.runRefs(ctx, r.takeDue(now), true)
	return reconcileAt
}

// Nudge marks a queue dirty after its owner commits a message change.
func (r *Runner) Nudge(ns, name string) {
	r.schedule(ns, name, time.Time{})
}

func (r *Runner) schedule(ns, name string, at time.Time) {
	r.wakeMu.Lock()
	defer r.wakeMu.Unlock()
	if r.deadlines == nil {
		r.deadlines = make(map[[2]string]time.Time)
	}
	key := [2]string{ns, name}
	if old, ok := r.deadlines[key]; !ok || at.Before(old) {
		r.deadlines[key] = at
	}
}

func (r *Runner) reconcile(ctx context.Context) error {
	refs, err := r.Queues(ctx)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		r.Nudge(ref.Namespace, ref.Name)
	}
	return nil
}

func (r *Runner) takeDue(now time.Time) []Ref {
	r.wakeMu.Lock()
	defer r.wakeMu.Unlock()
	var refs []Ref
	for key, at := range r.deadlines {
		if !at.After(now) {
			refs = append(refs, Ref{Namespace: key[0], Name: key[1]})
			delete(r.deadlines, key)
		}
	}
	return refs
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
	return r.runRefs(ctx, refs, false), nil
}

func (r *Runner) runRefs(ctx context.Context, refs []Ref, scheduled bool) (dispatched int) {
	if len(refs) == 0 {
		return 0
	}
	// Bound cross-queue fan-out independently of each queue's batch limit.
	jobs := make(chan Ref)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < min(len(refs), 8); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ref := range jobs {
				n := r.runRef(ctx, ref, scheduled)
				mu.Lock()
				dispatched += n
				mu.Unlock()
			}
		}()
	}
	seen := map[[2]string]bool{}
submit:
	for _, ref := range refs {
		key := [2]string{ref.Namespace, ref.Name}
		if seen[key] {
			continue
		}
		seen[key] = true
		select {
		case <-ctx.Done():
			break submit
		case jobs <- ref:
		}
	}
	close(jobs)
	wg.Wait()
	return dispatched
}

func (r *Runner) runRef(ctx context.Context, ref Ref, scheduled bool) int {
	if ctx.Err() != nil {
		return 0
	}
	if r.Filter != nil {
		ok, err := r.Filter(ctx, ref)
		if err != nil {
			r.log().Warn("queue filter failed", "ns", ref.Namespace, "queue", ref.Name, "err", err)
			return 0
		}
		if !ok {
			return 0
		}
	}
	if scheduled {
		// Read the local deadline before consulting the control plane. An
		// empty queue needs no consumer lookup until the next mutation.
		now := r.Store.now()
		at, err := r.nextBatch(ctx, ref, 1, now)
		if err != nil {
			r.log().Warn("queue readiness failed", "queue", ref.Name, "err", err)
			return 0
		}
		if at.IsZero() {
			return 0
		}
		if at.After(now) {
			r.schedule(ref.Namespace, ref.Name, at)
			return 0
		}
		current, ok, err := r.Resolve(ctx, ref.Namespace, ref.Name)
		if err != nil {
			r.log().Warn("queue consumer lookup failed", "queue", ref.Name, "err", err)
			return 0
		}
		if !ok {
			return 0
		}
		ref = current
	}
	return r.dispatchRef(ctx, ref, scheduled)
}

// MaxConsumerConcurrency bounds queue batch fan-out and allocation.
const MaxConsumerConcurrency = 100

// dispatchRef dispatches one wave of up to Ref.MaxConcurrency batches (0/1 =
// sequential, the default), keeping them in flight concurrently. The next Pass
// takes the next wave, so a pass never blocks indefinitely on a busy queue.
// Batch order within a wave is not guaranteed, which matches the queue's
// at-least-once contract.
func (r *Runner) dispatchRef(ctx context.Context, ref Ref, scheduled bool) int {
	conc := min(max(ref.MaxConcurrency, 1), MaxConsumerConcurrency)
	batch := r.Batch
	if ref.MaxBatchSize > 0 {
		batch = ref.MaxBatchSize
	}
	if batch <= 0 || batch > 100 {
		batch = 10
	}
	batches := make([][]Message, 0, conc)
	for i := 0; i < conc; i++ {
		now := r.Store.now()
		at, rerr := r.nextBatch(ctx, ref, batch, now)
		if rerr != nil {
			r.log().Warn("queue batch readiness failed", "ns", ref.Namespace, "queue", ref.Name, "err", rerr)
			break
		}
		if at.IsZero() || at.After(now) {
			if scheduled && len(batches) == 0 && !at.IsZero() {
				r.schedule(ref.Namespace, ref.Name, at)
			}
			break
		}
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
	if scheduled {
		// Settlements, failed acknowledgments and remaining backlog can each
		// change the next wake. A concurrent producer's earlier nudge wins.
		at, err := r.nextBatch(ctx, ref, batch, r.Store.now())
		if err != nil {
			r.log().Warn("queue settlement readiness failed", "queue", ref.Name, "err", err)
		} else if !at.IsZero() {
			r.schedule(ref.Namespace, ref.Name, at)
		}
	}
	return total
}

func (r *Runner) nextBatch(ctx context.Context, ref Ref, batch int, now time.Time) (time.Time, error) {
	if r.AdmitRead != nil {
		release, err := r.AdmitRead(ctx, ref)
		if err != nil {
			return time.Time{}, err
		}
		defer release()
	}
	return r.Store.nextBatch(ctx, ref.Namespace, ref.Name, batch, ref.MaxBatchTimeoutSeconds, now)
}

// dispatchBatch delivers one claimed batch and acks (or retries/dead-letters)
// its messages, returning the number acked.
func (r *Runner) dispatchBatch(ctx context.Context, ref Ref, msgs []Message) int {
	// Preparation may reset a stale generation, so it must precede admission.
	if r.Filter != nil {
		allowed, err := r.Filter(ctx, ref)
		if err != nil || !allowed {
			return 0
		}
	}
	release := func() {}
	if r.AdmitDispatch != nil {
		var err error
		release, err = r.AdmitDispatch(ctx, ref)
		if err != nil {
			return 0
		}
	}
	held := true
	defer func() {
		if held {
			release()
		}
	}()
	if r.Store.ResourceGeneration != nil {
		current, err := r.Store.ResourceGeneration(ctx, ref.Namespace, ref.Name)
		if err != nil {
			return 0
		}
		for _, m := range msgs {
			if m.ClaimGeneration != current {
				return 0
			}
		}
	}
	if r.Resolve != nil {
		current, ok, err := r.Resolve(ctx, ref.Namespace, ref.Name)
		if err != nil || !ok || current != ref {
			// Leave the claim leased. The next pass uses the new configuration.
			return 0
		}
	}
	if r.ValidateDispatch != nil {
		allowed, err := r.ValidateDispatch(ctx, ref)
		if err != nil || !allowed {
			return 0
		}
	}
	var res DispatchResult
	var derr error
	if dd, ok := r.Dispatch.(DetailedDispatcher); ok {
		res, derr = dd.DispatchDetailed(ctx, ref, msgs)
	} else {
		derr = r.Dispatch.Dispatch(ctx, ref, msgs)
	}
	release()
	held = false
	if derr != nil {
		for _, m := range msgs {
			// Attempts was incremented when the message was claimed above.
			if m.Attempts > retriesFor(ref) {
				r.deadLetter(ctx, ref, m, derr)
				continue
			}
			if rerr := r.commit(ctx, ref.Namespace, ref.Name, func() error {
				return r.Store.Retry(ctx, ref.Namespace, ref.Name, m.ID, r.retryDelayFor(ref))
			}); rerr != nil {
				r.log().Warn("queue retry failed", "ns", ref.Namespace, "queue", ref.Name, "id", m.ID, "err", rerr)
			}
		}
		attrs := []slog.Attr{slog.String("ns", ref.Namespace), slog.String("queue", ref.Name), slog.Int("batch", len(msgs))}
		attrs = append(attrs, dispatchFailureAttrs(derr, ref.Worker)...)
		r.log().LogAttrs(ctx, slog.LevelWarn, "queue dispatch failed; batch retried", attrs...)
		return 0
	}
	// Per-message outcomes (ADR-155): retried ids first (honoring retry()
	// delaySeconds), everything else implicitly acked on a successful dispatch.
	retryDelay := map[string]int64{}
	for _, rs := range res.Retry {
		retryDelay[rs.ID] = int64(rs.DelaySeconds) * 1000
	}
	// A successful dispatch implicitly acks every non-retried message. Batch
	// them into one durable commit: one resource-gate read + capture proof per
	// batch instead of one per message.
	acks := make([]string, 0, len(msgs))
	for _, m := range msgs {
		if delayMs, retry := retryDelay[m.ID]; retry {
			if m.Attempts > retriesFor(ref) {
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
		acks = append(acks, m.ID)
	}
	if len(acks) == 0 {
		return 0
	}
	if aerr := r.commit(ctx, ref.Namespace, ref.Name, func() error {
		return r.Store.AckMany(ctx, ref.Namespace, ref.Name, acks)
	}); aerr != nil {
		// AckMany is atomic: a failure leaves the whole batch unacked and
		// eligible for redelivery (at-least-once).
		r.log().Warn("queue ack failed", "ns", ref.Namespace, "queue", ref.Name, "ids", len(acks), "err", aerr)
		return 0
	}
	return len(acks)
}

// deadLetter moves a message that exhausted its retries to the configured
// dead-letter queue, or drops it (with a warning) when none is configured.
// The original business key/metadata travel with the message. A source-ID
// transfer receipt prevents a retried move from inserting another DLQ message.
func (r *Runner) deadLetter(ctx context.Context, ref Ref, m Message, cause error) {
	if m.SourceQueue == "" {
		m.SourceQueue = ref.Name
		m.SourceID = m.ID
		m.SourceCreatedMs = m.TimestampMs
	}
	if m.Attempts > m.SourceAttempts {
		m.SourceAttempts = m.Attempts
	}
	if ref.DeadLetterQueue != "" {
		send := func() error {
			return r.commit(ctx, ref.Namespace, ref.DeadLetterQueue, func() error {
				_, e := r.Store.Transfer(ctx, ref.Namespace, ref.DeadLetterQueue, m)
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
		attrs := []slog.Attr{slog.String("ns", ref.Namespace), slog.String("queue", ref.Name), slog.String("dlq", ref.DeadLetterQueue), slog.String("id", m.ID), slog.Int("attempts", m.Attempts)}
		attrs = append(attrs, dispatchFailureAttrs(cause, ref.Worker)...)
		r.log().LogAttrs(ctx, slog.LevelWarn, "queue message dead-lettered", attrs...)
		return
	}
	if err := r.commit(ctx, ref.Namespace, ref.Name, func() error {
		return r.Store.Ack(ctx, ref.Namespace, ref.Name, m.ID)
	}); err != nil {
		r.log().Warn("queue drop-ack failed", "ns", ref.Namespace, "queue", ref.Name, "id", m.ID, "err", err)
		return
	}
	attrs := []slog.Attr{slog.String("ns", ref.Namespace), slog.String("queue", ref.Name), slog.String("id", m.ID), slog.Int("attempts", m.Attempts)}
	attrs = append(attrs, dispatchFailureAttrs(cause, ref.Worker)...)
	r.log().LogAttrs(ctx, slog.LevelWarn, "queue message dropped (retries exhausted, no dead-letter queue)", attrs...)
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
	ID                  string          `json:"id"`
	Body                string          `json:"body"` // base64
	ContentType         string          `json:"content_type"`
	Attempts            int             `json:"attempts"`
	Timestamp           int64           `json:"timestamp"`
	Metadata            json.RawMessage `json:"metadata,omitempty"`
	SourceQueue         string          `json:"source_queue"`
	SourceGeneration    string          `json:"source_generation"`
	SourceID            string          `json:"source_id"`
	ProducerRequestID   string          `json:"producer_request_id,omitempty"`
	ProducerTraceparent string          `json:"producer_traceparent,omitempty"`
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
			Timestamp:   m.TimestampMs,
			Metadata:    m.Metadata,
			SourceQueue: m.SourceQueue, SourceGeneration: m.SourceGeneration, SourceID: m.SourceID,
			ProducerRequestID: m.ProducerRequestID, ProducerTraceparent: m.ProducerTraceparent,
		})
	}
	body, err := json.Marshal(map[string]any{
		"namespace":  ref.Namespace,
		"queue":      ref.BusinessName(),
		"worker":     ref.Worker,
		"bundle_sha": ref.BundleSHA,
		"version":    ref.Version,
		"messages":   out,
	})
	if err != nil {
		return DispatchResult{}, err
	}
	// A runtime can briefly return 5xx while its worker isolate is being
	// loaded or its owner lease is being refreshed. Retry the same batch once
	// before handing it back to the queue retry/dead-letter machinery. Queue
	// delivery is at-least-once, so a duplicate is safe and preferable to
	// turning a transient runtime restart into an application retry.
	const dispatchAttempts = 2
	var resp *http.Response
	for attempt := 0; attempt < dispatchAttempts; attempt++ {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, d.URL+"/v1/queues/dispatch", bytes.NewReader(body))
		if reqErr != nil {
			return DispatchResult{}, reqErr
		}
		req.Header.Set("content-type", "application/json")
		if d.Token != "" {
			if err := bearertoken.SetInternal(req, d.Token, "dispatch"); err != nil {
				return DispatchResult{}, err
			}
		}
		resp, err = d.Client.Do(req)
		if err != nil {
			if attempt+1 < dispatchAttempts {
				select {
				case <-ctx.Done():
					return DispatchResult{}, ctx.Err()
				case <-time.After(25 * time.Millisecond):
				}
				continue
			}
			return DispatchResult{}, err
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			break
		}
		if !retryableDispatchStatus(resp.StatusCode) || attempt+1 == dispatchAttempts {
			data, _ := io.ReadAll(io.LimitReader(resp.Body, dispatchErrorBodyLimit))
			resp.Body.Close()
			return DispatchResult{}, dispatchResponseError(d.URL, ref, resp.StatusCode, data)
		}
		resp.Body.Close()
		select {
		case <-ctx.Done():
			return DispatchResult{}, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return DispatchResult{}, nil
	}
	const maxResponse = 1 << 20
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil {
		return DispatchResult{}, fmt.Errorf("queue dispatch response: %w", err)
	}
	if len(data) > maxResponse {
		return DispatchResult{}, fmt.Errorf("queue dispatch response exceeds %d bytes", maxResponse)
	}
	var envelope struct {
		OK bool `json:"ok"`
		DispatchResult
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return DispatchResult{}, fmt.Errorf("queue dispatch response: %w", err)
	}
	if !envelope.OK {
		return DispatchResult{}, fmt.Errorf("queue dispatch response missing success confirmation")
	}
	return envelope.DispatchResult, nil
}

// Only statuses that normally mean the dispatching runtime or an upstream
// gateway is temporarily unavailable are safe to retry automatically. A 500
// is commonly a tenant handler failure; retrying it can repeat a side effect
// without improving the outcome.
func retryableDispatchStatus(status int) bool {
	return status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}
