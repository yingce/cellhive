package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cellstore"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	return New(cs)
}

func TestQueueSendClaimAck(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	id1, err := s.Send(ctx, "acme", "jobs", []byte("m1"), "text/plain", 0, "")
	if err != nil || id1 == "" {
		t.Fatalf("send = %q, %v", id1, err)
	}
	if _, err := s.Send(ctx, "acme", "jobs", []byte("m2"), "text/plain", 0, ""); err != nil {
		t.Fatalf("send2: %v", err)
	}
	msgs, err := s.Claim(ctx, "acme", "jobs", 10, 30_000)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("claim = %+v, %v", msgs, err)
	}
	// A second claim while leased returns nothing.
	if m, _ := s.Claim(ctx, "acme", "jobs", 10, 30_000); len(m) != 0 {
		t.Fatalf("leased messages reclaimed: %+v", m)
	}
	if err := s.Ack(ctx, "acme", "jobs", msgs[0].ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if n, _ := s.Depth(ctx, "acme", "jobs"); n != 1 {
		t.Fatalf("depth = %d, want 1", n)
	}
}

func TestQueueRetryAndDelay(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	base := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return base }
	id, _ := s.Send(ctx, "acme", "jobs", []byte("m"), "", 0, "")
	msgs, _ := s.Claim(ctx, "acme", "jobs", 1, 1000)
	if len(msgs) != 1 {
		t.Fatalf("claim = %+v", msgs)
	}
	// Retry pushes it into the future.
	if err := s.Retry(ctx, "acme", "jobs", id, 5000); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if m, _ := s.Claim(ctx, "acme", "jobs", 1, 1000); len(m) != 0 {
		t.Fatalf("retried message immediately claimed: %+v", m)
	}
	s.now = func() time.Time { return base.Add(6 * time.Second) }
	m2, _ := s.Claim(ctx, "acme", "jobs", 1, 1000)
	if len(m2) != 1 || m2[0].Attempts != 2 {
		t.Fatalf("post-retry claim = %+v", m2)
	}
	if err := s.Ack(ctx, "acme", "jobs", m2[0].ID); err != nil {
		t.Fatalf("ack: %v", err)
	}
	// A delayed send is not visible until due.
	if _, err := s.Send(ctx, "acme", "jobs", []byte("d"), "", 60, ""); err != nil {
		t.Fatalf("delayed send: %v", err)
	}
	if m, _ := s.Claim(ctx, "acme", "jobs", 10, 1000); len(m) != 0 {
		t.Fatalf("delayed message visible early: %+v", m)
	}
	s.now = func() time.Time { return base.Add(70 * time.Second) }
	m3, _ := s.Claim(ctx, "acme", "jobs", 10, 1000)
	if len(m3) != 1 || string(m3[0].Body) != "d" {
		t.Fatalf("delayed message not visible after delay: %+v", m3)
	}
}

func TestQueueIdempotency(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	id1, err := s.Send(ctx, "acme", "jobs", []byte("m"), "", 0, "key-1")
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	id2, err := s.Send(ctx, "acme", "jobs", []byte("m"), "", 0, "key-1")
	if err != nil || id2 != id1 {
		t.Fatalf("idempotent send = %q vs %q, %v", id2, id1, err)
	}
	if n, _ := s.Depth(ctx, "acme", "jobs"); n != 1 {
		t.Fatalf("depth = %d, want 1", n)
	}
}

// TestReadOnlyCallsDoNotAdvanceTxID is the regression for running the schema
// migration on every call: pure reads must not advance the capture txid (which
// would turn read traffic into replication traffic).
func TestReadOnlyCallsDoNotAdvanceTxID(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	s := New(cs)
	if _, err := s.Depth(ctx, "acme", "jobs"); err != nil {
		t.Fatalf("depth: %v", err)
	}
	c, err := cs.Cell(ctx, Scope("acme", "jobs"))
	if err != nil {
		t.Fatalf("cell: %v", err)
	}
	before, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Depth(ctx, "acme", "jobs"); err != nil {
			t.Fatalf("depth %d: %v", i, err)
		}
		if _, err := s.Status(ctx, "acme", "jobs"); err != nil {
			t.Fatalf("status %d: %v", i, err)
		}
	}
	after, err := c.TxID(ctx)
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	if after != before {
		t.Fatalf("read calls advanced txid: %d -> %d", before, after)
	}
}

// TestSendIdempotencyConcurrent is the regression for the SELECT-then-INSERT
// TOCTOU: concurrent resends of one idempotency key must dedupe, not fail the
// unique index.
func TestSendIdempotencyConcurrent(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	const key = "idem-1"
	const n = 8
	ids := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], errs[i] = s.Send(ctx, "acme", "jobs", []byte("v"), "text/plain", 0, key)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("ids differ: %v", ids)
		}
	}
	depth, err := s.Depth(ctx, "acme", "jobs")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if depth != 1 {
		t.Fatalf("depth = %d, want 1", depth)
	}
}
