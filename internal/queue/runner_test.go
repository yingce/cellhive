package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cellstore"
	"cellhive/internal/queue"
)

type fakeDispatcher struct {
	calls int
	ref   queue.Ref
	last  []queue.Message
	err   error
}

func (f *fakeDispatcher) Dispatch(_ context.Context, ref queue.Ref, msgs []queue.Message) error {
	f.calls++
	f.ref, f.last = ref, msgs
	return f.err
}

func newStore(t *testing.T) *queue.Store {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	return queue.New(cs)
}

func staticRefs(ns, name string) func(context.Context) ([]queue.Ref, error) {
	return func(context.Context) ([]queue.Ref, error) {
		return []queue.Ref{{Namespace: ns, Name: name, Worker: "consumer", BundleSHA: "sha1"}}, nil
	}
}

func TestRunnerClaimsDispatchesAcks(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	for _, b := range []string{"m1", "m2"} {
		if _, err := st.Send(ctx, "demo", "q", []byte(b), "text/plain", 0, ""); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	fd := &fakeDispatcher{}
	r := &queue.Runner{Store: st, Dispatch: fd, Batch: 10, Queues: staticRefs("demo", "q")}
	n, err := r.Pass(ctx)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if n != 2 || fd.calls != 1 || len(fd.last) != 2 {
		t.Fatalf("pass=%d calls=%d batch=%d; want 2,1,2", n, fd.calls, len(fd.last))
	}
	if fd.ref.Namespace != "demo" || fd.ref.Name != "q" || fd.ref.Worker != "consumer" || fd.ref.BundleSHA != "sha1" {
		t.Fatalf("dispatched ref = %+v", fd.ref)
	}
	if d, _ := st.Depth(ctx, "demo", "q"); d != 0 {
		t.Fatalf("depth after ack = %d, want 0", d)
	}
}

func TestRunnerRetriesOnDispatchError(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	if _, err := st.Send(ctx, "demo", "q", []byte("m1"), "text/plain", 0, ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	fd := &fakeDispatcher{err: errors.New("consumer down")}
	r := &queue.Runner{Store: st, Dispatch: fd, Batch: 10, RetryDelayMs: 60_000, Queues: staticRefs("demo", "q")}
	n, err := r.Pass(ctx)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if n != 0 || fd.calls != 1 {
		t.Fatalf("pass=%d calls=%d; want 0,1", n, fd.calls)
	}
	if d, _ := st.Depth(ctx, "demo", "q"); d != 1 {
		t.Fatalf("failed message should remain (retry), depth=%d", d)
	}
	// Invisible during the retry delay: an immediate second pass claims nothing.
	if n2, _ := r.Pass(ctx); n2 != 0 {
		t.Fatalf("second pass dispatched %d during retry delay", n2)
	}
}

func TestHTTPDispatcherPayload(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/queues/dispatch" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.Header.Get("x-cellhive-internal-token") != "tok" {
			t.Errorf("missing token header")
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := queue.NewHTTP(srv.URL, "tok")
	msgs := []queue.Message{{ID: "id1", Body: []byte("hi"), ContentType: "text/plain", Attempts: 1}}
	if err := d.Dispatch(context.Background(), queue.Ref{Namespace: "demo", Name: "q", Worker: "consumer", BundleSHA: "sha1", Version: 7}, msgs); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if got["namespace"] != "demo" || got["queue"] != "q" || got["worker"] != "consumer" || got["bundle_sha"] != "sha1" {
		t.Fatalf("payload = %+v", got)
	}
	if got["version"] != float64(7) {
		t.Fatalf("version = %v, want 7 (ADR-127)", got["version"])
	}
	arr, _ := got["messages"].([]any)
	if len(arr) != 1 {
		t.Fatalf("messages = %+v", got["messages"])
	}
	m, _ := arr[0].(map[string]any)
	if m["id"] != "id1" || m["body"] != "aGk=" || m["content_type"] != "text/plain" {
		t.Fatalf("message = %+v", m)
	}
}

func TestHTTPDispatcherErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	if err := queue.NewHTTP(srv.URL, "").Dispatch(context.Background(), queue.Ref{Namespace: "demo", Name: "q"}, nil); err == nil {
		t.Fatal("expected error on 500")
	}
}

func TestNewHTTPEmptyIsNil(t *testing.T) {
	if queue.NewHTTP("", "tok") != nil {
		t.Fatal("empty URL should yield nil dispatcher")
	}
}

func TestRunnerDeadLettersExhaustedMessage(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	if _, err := st.Send(ctx, "demo", "jobs", []byte("m1"), "text/plain", 0, ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	fd := &fakeDispatcher{err: errors.New("consumer down")}
	r := &queue.Runner{
		Store: st, Dispatch: fd,
		Queues: func(context.Context) ([]queue.Ref, error) {
			return []queue.Ref{{Namespace: "demo", Name: "jobs", Worker: "c", BundleSHA: "s", MaxRetries: 1, DeadLetterQueue: "jobs-dlq"}}, nil
		},
	}
	if n, err := r.Pass(ctx); err != nil || n != 0 {
		t.Fatalf("pass = %d, %v", n, err)
	}
	if d, _ := st.Depth(ctx, "demo", "jobs"); d != 0 {
		t.Fatalf("original queue depth = %d, want 0 (moved)", d)
	}
	if d, _ := st.Depth(ctx, "demo", "jobs-dlq"); d != 1 {
		t.Fatalf("dlq depth = %d, want 1", d)
	}
}

// TestRunnerDeadLetterPreservesIdempotencyKey covers the replay-dedup fix:
// a message dead-lettered with an idempotency key keeps that key, so a DLQ
// replay dedupes against any still-live twin instead of duplicating it.
func TestRunnerDeadLetterPreservesIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	if _, err := st.Send(ctx, "demo", "jobs", []byte("m1"), "text/plain", 0, "biz-key-1"); err != nil {
		t.Fatalf("send: %v", err)
	}
	fd := &fakeDispatcher{err: errors.New("consumer down")}
	r := &queue.Runner{
		Store: st, Dispatch: fd,
		Queues: func(context.Context) ([]queue.Ref, error) {
			return []queue.Ref{{Namespace: "demo", Name: "jobs", Worker: "c", BundleSHA: "s", MaxRetries: 1, DeadLetterQueue: "jobs-dlq"}}, nil
		},
	}
	if n, err := r.Pass(ctx); err != nil || n != 0 {
		t.Fatalf("pass = %d, %v", n, err)
	}
	claimed, err := st.Claim(ctx, "demo", "jobs-dlq", 10, 30_000)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("dlq claim = %d, %v", len(claimed), err)
	}
	if claimed[0].IdempotencyKey != "biz-key-1" {
		t.Fatalf("dlq idempotency key = %q, want biz-key-1", claimed[0].IdempotencyKey)
	}
	// A replay Send with the same key into the original queue dedupes when a
	// live twin exists (the unique-index path), proving the key survived the
	// dead-letter round trip.
	id, err := st.Send(ctx, "demo", "jobs", []byte("m1"), "text/plain", 0, "biz-key-1")
	if err != nil {
		t.Fatalf("replay send: %v", err)
	}
	if d, _ := st.Depth(ctx, "demo", "jobs"); d != 1 {
		t.Fatalf("depth after replay = %d, want 1 (deduped)", d)
	}
	_ = id
}

func TestRunnerDropsWhenNoDeadLetterQueue(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	if _, err := st.Send(ctx, "demo", "jobs", []byte("m1"), "text/plain", 0, ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	fd := &fakeDispatcher{err: errors.New("boom")}
	r := &queue.Runner{
		Store: st, Dispatch: fd,
		Queues: func(context.Context) ([]queue.Ref, error) {
			return []queue.Ref{{Namespace: "demo", Name: "jobs", MaxRetries: 1}}, nil
		},
	}
	if _, err := r.Pass(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if d, _ := st.Depth(ctx, "demo", "jobs"); d != 0 {
		t.Fatalf("depth = %d, want 0 (dropped)", d)
	}
}

func TestRunnerRetriesBelowMaxRetries(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	if _, err := st.Send(ctx, "demo", "jobs", []byte("m1"), "text/plain", 0, ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	fd := &fakeDispatcher{err: errors.New("boom")}
	r := &queue.Runner{
		Store: st, Dispatch: fd, RetryDelayMs: 60_000,
		Queues: func(context.Context) ([]queue.Ref, error) {
			return []queue.Ref{{Namespace: "demo", Name: "jobs", MaxRetries: 3}}, nil
		},
	}
	// attempts becomes 1 (< 3) -> retry, not dead-letter/drop.
	if _, err := r.Pass(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	if d, _ := st.Depth(ctx, "demo", "jobs"); d != 1 {
		t.Fatalf("depth = %d, want 1 (retained for retry)", d)
	}
}

func TestRunnerHonorsMaxBatchSize(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	for _, b := range []string{"m1", "m2", "m3"} {
		if _, err := st.Send(ctx, "demo", "jobs", []byte(b), "text/plain", 0, ""); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	fd := &fakeDispatcher{}
	r := &queue.Runner{
		Store: st, Dispatch: fd, Batch: 10,
		Queues: func(context.Context) ([]queue.Ref, error) {
			return []queue.Ref{{Namespace: "demo", Name: "jobs", MaxBatchSize: 2}}, nil
		},
	}
	if n, err := r.Pass(ctx); err != nil || n != 2 {
		t.Fatalf("pass = %d, %v; want 2 (MaxBatchSize)", n, err)
	}
	if len(fd.last) != 2 {
		t.Fatalf("dispatch batch = %d, want 2", len(fd.last))
	}
	if d, _ := st.Depth(ctx, "demo", "jobs"); d != 1 {
		t.Fatalf("depth = %d, want 1 remaining", d)
	}
}

// gateDispatcher blocks every Dispatch until `want` calls are in flight, then
// releases them all. It records the observed maximum in-flight concurrency.
type gateDispatcher struct {
	want  int
	once  sync.Once
	gate  chan struct{}
	mu    sync.Mutex
	cur   int
	max   int
	calls int
}

func newGate(want int) *gateDispatcher {
	return &gateDispatcher{want: want, gate: make(chan struct{})}
}

func (d *gateDispatcher) Dispatch(_ context.Context, _ queue.Ref, _ []queue.Message) error {
	d.mu.Lock()
	d.cur++
	d.calls++
	if d.cur > d.max {
		d.max = d.cur
	}
	hit := d.cur >= d.want
	d.mu.Unlock()
	if hit {
		d.once.Do(func() { close(d.gate) })
	}
	<-d.gate
	d.mu.Lock()
	d.cur--
	d.mu.Unlock()
	return nil
}

// TestRunnerMaxConcurrency: with MaxConcurrency=3 and single-message batches,
// three dispatches are in flight at once; without it (default) a pass still
// claims exactly one batch, so the default behavior is unchanged.
func TestRunnerMaxConcurrency(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	for i := 0; i < 3; i++ {
		if _, err := st.Send(ctx, "demo", "q", []byte(fmt.Sprintf("m%d", i)), "text/plain", 0, ""); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	d := newGate(3)
	refs := func(context.Context) ([]queue.Ref, error) {
		return []queue.Ref{{
			Namespace: "demo", Name: "q", Worker: "consumer", BundleSHA: "sha1",
			MaxBatchSize: 1, MaxConcurrency: 3,
		}}, nil
	}
	r := &queue.Runner{Store: st, Dispatch: d, Batch: 10, Queues: refs}
	done := make(chan struct{})
	var n int
	var err error
	go func() { n, err = r.Pass(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("pass never reached MaxConcurrency=3 in flight")
	}
	if err != nil || n != 3 {
		t.Fatalf("pass = %d, %v; want 3, nil", n, err)
	}
	if d.max != 3 || d.calls != 3 {
		t.Fatalf("max concurrency = %d (calls %d), want 3", d.max, d.calls)
	}

	// Default (MaxConcurrency unset): exactly one batch per pass, sequentially.
	st2 := newStore(t)
	for i := 0; i < 3; i++ {
		if _, err := st2.Send(ctx, "demo", "q", []byte("m"), "text/plain", 0, ""); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	fd := &fakeDispatcher{}
	sequential := func(context.Context) ([]queue.Ref, error) {
		return []queue.Ref{{Namespace: "demo", Name: "q", Worker: "consumer", BundleSHA: "sha1", MaxBatchSize: 1}}, nil
	}
	r2 := &queue.Runner{Store: st2, Dispatch: fd, Batch: 10, Queues: sequential}
	n2, err := r2.Pass(ctx)
	if err != nil || n2 != 1 || fd.calls != 1 {
		t.Fatalf("sequential pass = %d calls=%d err=%v; want 1,1,nil", n2, fd.calls, err)
	}
	if depth, _ := st2.Depth(ctx, "demo", "q"); depth != 2 {
		t.Fatalf("depth = %d, want 2 left", depth)
	}
}

// TestRunnerFilterSkipsQueues covers Runner.Filter (ADR-119): a queue this node
// must not consume is not even claimed.
func TestRunnerFilterSkipsQueues(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	if _, err := st.Send(ctx, "demo", "q", []byte("m1"), "text/plain", 0, ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	fd := &fakeDispatcher{}
	filtered := 0
	r := &queue.Runner{
		Store: st, Dispatch: fd, Batch: 10, Queues: staticRefs("demo", "q"),
		Filter: func(context.Context, queue.Ref) (bool, error) { filtered++; return false, nil },
	}
	n, err := r.Pass(ctx)
	if err != nil || n != 0 || fd.calls != 0 || filtered != 1 {
		t.Fatalf("pass = %d, calls=%d, filtered=%d, err=%v; want 0,0,1,nil", n, fd.calls, filtered, err)
	}
	if depth, _ := st.Depth(ctx, "demo", "q"); depth != 1 {
		t.Fatalf("depth = %d, want 1 (untouched)", depth)
	}
}

// TestRunnerCommitWrapsMutations covers Runner.Commit (ADR-119): claim and ack
// go through the commit hook so they are captured with a durability proof.
func TestRunnerCommitWrapsMutations(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	for _, b := range []string{"m1", "m2"} {
		if _, err := st.Send(ctx, "demo", "q", []byte(b), "text/plain", 0, ""); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	fd := &fakeDispatcher{}
	var mu sync.Mutex
	wrapped := 0
	commit := func(_ context.Context, ns, name string, fn func() error) error {
		mu.Lock()
		wrapped++
		mu.Unlock()
		return fn()
	}
	r := &queue.Runner{Store: st, Dispatch: fd, Batch: 10, Queues: staticRefs("demo", "q"), Commit: commit}
	n, err := r.Pass(ctx)
	if err != nil || n != 2 {
		t.Fatalf("pass = %d, %v; want 2", n, err)
	}
	mu.Lock()
	got := wrapped
	mu.Unlock()
	if got != 3 { // one claim + one ack per message (2 messages)
		t.Fatalf("commit wrapped %d mutations, want 3 (claim + 2 acks)", got)
	}
	if depth, _ := st.Depth(ctx, "demo", "q"); depth != 0 {
		t.Fatalf("depth = %d, want 0", depth)
	}
}
