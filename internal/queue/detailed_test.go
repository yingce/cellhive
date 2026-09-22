package queue

import (
	"context"
	"testing"
	"time"

	"cellhive/internal/cellstore"
)

// detailedDispatcher reports per-message outcomes (CF ack()/retry()).
type detailedDispatcher struct {
	res DispatchResult
	err error
}

func (d *detailedDispatcher) Dispatch(context.Context, Ref, []Message) error { return d.err }

func (d *detailedDispatcher) DispatchDetailed(context.Context, Ref, []Message) (DispatchResult, error) {
	return d.res, d.err
}

func newInternalStore(t *testing.T) *Store {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	return New(cs)
}

func send(t *testing.T, st *Store, body string, delay int) string {
	t.Helper()
	id, err := st.Send(context.Background(), "demo", "q", []byte(body), "text/plain", delay, "")
	if err != nil {
		t.Fatalf("send %s: %v", body, err)
	}
	return id
}

// TestSendDelayVisibility: a delayed send is invisible until its visible_at
// passes (delayed producers, ADR-155).
func TestSendDelayVisibility(t *testing.T) {
	ctx := context.Background()
	st := newInternalStore(t)
	now := time.Unix(1000, 0)
	st.now = func() time.Time { return now }
	send(t, st, "later", 2)

	if msgs, err := st.Claim(ctx, "demo", "q", 10, 1000); err != nil || len(msgs) != 0 {
		t.Fatalf("claim before delay = %+v, %v; want nothing", msgs, err)
	}
	now = now.Add(3 * time.Second)
	msgs, err := st.Claim(ctx, "demo", "q", 10, 1000)
	if err != nil || len(msgs) != 1 || string(msgs[0].Body) != "later" {
		t.Fatalf("claim after delay = %+v, %v; want the message", msgs, err)
	}
}

// TestRunnerPerMessageAckRetry: explicit ack()/retry() are honored per message
// and unmentioned messages are acked implicitly (ADR-155).
func TestRunnerPerMessageAckRetry(t *testing.T) {
	ctx := context.Background()
	st := newInternalStore(t)
	m1 := send(t, st, "one", 0)
	m2 := send(t, st, "two", 0)
	_ = send(t, st, "three", 0) // implicit ack

	fd := &detailedDispatcher{res: DispatchResult{Ack: []string{m1}, Retry: []RetrySpec{{ID: m2}}}}
	r := &Runner{
		Store: st, Dispatch: fd, Batch: 10,
		Queues: func(context.Context) ([]Ref, error) {
			return []Ref{{Namespace: "demo", Name: "q", Worker: "consumer", BundleSHA: "sha"}}, nil
		},
	}
	if _, err := r.Pass(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	// Only the retried message comes back; m1 (explicit ack) and m3 (implicit
	// ack) are gone, and the retry keeps the body.
	msgs, err := st.Claim(ctx, "demo", "q", 10, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].ID != m2 || string(msgs[0].Body) != "two" {
		t.Fatalf("claim after per-message outcomes = %+v, want only m2", msgs)
	}
	if msgs[0].Attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (retry re-claim)", msgs[0].Attempts)
	}
}

// TestRunnerRetryDelay: retry({delaySeconds}) makes the message invisible for
// that long.
func TestRunnerRetryDelay(t *testing.T) {
	ctx := context.Background()
	st := newInternalStore(t)
	now := time.Unix(2000, 0)
	st.now = func() time.Time { return now }
	m1 := send(t, st, "slow", 0)

	fd := &detailedDispatcher{res: DispatchResult{Retry: []RetrySpec{{ID: m1, DelaySeconds: 60}}}}
	r := &Runner{
		Store: st, Dispatch: fd, Batch: 10,
		Queues: func(context.Context) ([]Ref, error) {
			return []Ref{{Namespace: "demo", Name: "q", Worker: "consumer", BundleSHA: "sha"}}, nil
		},
	}
	if _, err := r.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if msgs, err := st.Claim(ctx, "demo", "q", 10, 1000); err != nil || len(msgs) != 0 {
		t.Fatalf("claim during retry delay = %+v, %v; want nothing", msgs, err)
	}
	now = now.Add(61 * time.Second)
	if msgs, err := st.Claim(ctx, "demo", "q", 10, 1000); err != nil || len(msgs) != 1 {
		t.Fatalf("claim after retry delay = %+v, %v; want the message", msgs, err)
	}
}

// TestRunnerRetryExhaustedGoesToDLQ: a consumer-requested retry beyond
// max_retries dead-letters instead of retrying forever.
func TestRunnerRetryExhaustedGoesToDLQ(t *testing.T) {
	ctx := context.Background()
	st := newInternalStore(t)
	m1 := send(t, st, "boom", 0)

	fd := &detailedDispatcher{res: DispatchResult{Retry: []RetrySpec{{ID: m1}}}}
	r := &Runner{
		Store: st, Dispatch: fd, Batch: 10,
		Queues: func(context.Context) ([]Ref, error) {
			return []Ref{{Namespace: "demo", Name: "q", Worker: "consumer", BundleSHA: "sha",
				MaxRetries: 2, DeadLetterQueue: "dlq"}}, nil
		},
	}
	// First pass: attempts becomes 1, consumer retries (message stays queued).
	if _, err := r.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if d, err := st.Depth(ctx, "demo", "q"); err != nil || d != 1 {
		t.Fatalf("depth after retry = %d, %v; want the message queued", d, err)
	}
	// Second pass: attempts = 2 >= max_retries 2 -> dead-letter.
	if _, err := r.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if msgs, _ := st.Claim(ctx, "demo", "q", 10, 1000); len(msgs) != 0 {
		t.Fatalf("message still in queue after DLQ: %+v", msgs)
	}
	dlq, err := st.Claim(ctx, "demo", "dlq", 10, 1000)
	if err != nil || len(dlq) != 1 || string(dlq[0].Body) != "boom" {
		t.Fatalf("dlq = %+v, %v; want the dead-lettered message", dlq, err)
	}
}

func TestRunnerDeadLetterUsesOwnerAwareSend(t *testing.T) {
	ctx := context.Background()
	st := newInternalStore(t)
	id := send(t, st, "poison", 0)
	var sent Message
	r := &Runner{
		Store: st, Dispatch: &detailedDispatcher{res: DispatchResult{Retry: []RetrySpec{{ID: id}}}}, Batch: 1,
		Queues: func(context.Context) ([]Ref, error) {
			return []Ref{{Namespace: "demo", Name: "q", MaxRetries: 1, DeadLetterQueue: "dlq"}}, nil
		},
		DeadLetterSend: func(_ context.Context, ns, name string, m Message) error {
			if ns != "demo" || name != "dlq" {
				t.Fatalf("destination %s/%s", ns, name)
			}
			sent = m
			return nil
		},
	}
	if _, err := r.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if string(sent.Body) != "poison" {
		t.Fatalf("owner send body = %q", sent.Body)
	}
	if depth, err := st.Depth(ctx, "demo", "q"); err != nil || depth != 0 {
		t.Fatalf("source depth = %d, %v", depth, err)
	}
	if depth, err := st.Depth(ctx, "demo", "dlq"); err != nil || depth != 0 {
		t.Fatalf("bypassed owner send: local depth = %d, %v", depth, err)
	}
}

// TestStatusCounts: Status reports depth/visible/leased without listing bodies
// (ADR-156).
func TestStatusCounts(t *testing.T) {
	ctx := context.Background()
	st := newInternalStore(t)
	now := time.Unix(3000, 0)
	st.now = func() time.Time { return now }
	send(t, st, "a", 0)
	send(t, st, "b", 60) // delayed: invisible
	send(t, st, "c", 0)
	if _, err := st.Claim(ctx, "demo", "q", 1, 30_000); err != nil {
		t.Fatal(err)
	}
	got, err := st.Status(ctx, "demo", "q")
	if err != nil {
		t.Fatal(err)
	}
	if got.Depth != 3 || got.Visible != 1 || got.Leased != 1 {
		t.Fatalf("status = %+v, want depth 3, visible 1 (c), leased 1 (a)", got)
	}
	// ADR-157 lag indicators: the earliest visible_at_ms among queued messages
	// (the immediate ones, i.e. now) and the highest attempt count.
	if got.OldestVisibleMs != now.UnixMilli() {
		t.Fatalf("oldest visible = %d, want %d (immediate messages)", got.OldestVisibleMs, now.UnixMilli())
	}
	if got.MaxAttempts != 1 {
		t.Fatalf("max attempts = %d, want 1 (claimed once)", got.MaxAttempts)
	}
	ds, err := st.DiskStats(ctx, "demo", "q")
	if err != nil || ds.PageCount <= 0 || ds.TotalBytes <= 0 {
		t.Fatalf("queue disk stats = %+v, %v", ds, err)
	}
	// After the delay (and the 30s lease) everything is visible again.
	now = now.Add(61 * time.Second)
	got, _ = st.Status(ctx, "demo", "q")
	if got.Visible != 3 || got.Leased != 0 {
		t.Fatalf("status after delay = %+v, want visible 3, leased 0", got)
	}
}
