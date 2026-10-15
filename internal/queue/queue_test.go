package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cellstore"
)

func TestQueueSendBatchAtomicAndMetadata(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	meta := json.RawMessage(`{"tenant":"a"}`)
	ids, err := s.SendBatch(ctx, "acme", "jobs", []SendMessage{
		{Body: []byte(`{"n":1}`), ContentType: "json", Metadata: meta},
		{Body: []byte("hello"), ContentType: "text", IdempotencyKey: "k"},
	})
	if err != nil || len(ids) != 2 {
		t.Fatalf("sendBatch = %#v, %v", ids, err)
	}
	msgs, err := s.Claim(ctx, "acme", "jobs", 10, 1000)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("claim = %#v, %v", msgs, err)
	}
	foundMeta := false
	for _, msg := range msgs {
		if msg.TimestampMs == 0 {
			t.Fatalf("message timestamp = %#v", msg)
		}
		if string(msg.Metadata) == string(meta) {
			foundMeta = true
		}
	}
	if !foundMeta {
		t.Fatalf("message metadata missing = %#v", msgs)
	}
	ids2, err := s.SendBatch(ctx, "acme", "jobs", []SendMessage{{Body: []byte("hello"), ContentType: "text", IdempotencyKey: "k"}})
	if err != nil || len(ids2) != 1 || ids2[0] != ids[1] {
		t.Fatalf("idempotent batch = %#v, %v", ids2, err)
	}
	if _, err := s.SendBatch(ctx, "acme", "jobs", []SendMessage{{Body: []byte("different"), ContentType: "text", IdempotencyKey: "k"}}); err == nil {
		t.Fatal("expected idempotency conflict")
	}
}

func TestQueueSendBatchRejectsBeforeCommit(t *testing.T) {
	s := newStore(t)
	if _, err := s.SendBatch(context.Background(), "acme", "jobs", []SendMessage{{Body: make([]byte, 128*1024+1), ContentType: "bytes"}, {Body: []byte("ok"), ContentType: "text"}}); err == nil {
		t.Fatal("expected oversized batch rejection")
	}
	if n, _ := s.Depth(context.Background(), "acme", "jobs"); n != 0 {
		t.Fatalf("partial batch committed: %d", n)
	}
}

func newStore(t *testing.T) *Store {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	return New(cs)
}

func TestIdempotencySurvivesAckFor24Hours(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	now := time.Unix(1000000, 0)
	s.now = func() time.Time { return now }
	id, err := s.Send(ctx, "acme", "q", []byte("one"), "text", 0, "key")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Ack(ctx, "acme", "q", id); err != nil {
		t.Fatal(err)
	}
	s = New(s.cs)
	s.now = func() time.Time { return now }
	now = now.Add(24*time.Hour - time.Millisecond)
	again, err := s.Send(ctx, "acme", "q", []byte("one"), "text", 0, "key")
	if err != nil || again != id {
		t.Fatalf("dedup lost after ack: %s %s %v", id, again, err)
	}
	if _, err := s.Send(ctx, "acme", "q", []byte("different"), "text", 0, "key"); err == nil {
		t.Fatal("conflict lost after ack")
	}
	now = now.Add(time.Millisecond)
	again, err = s.Send(ctx, "acme", "q", []byte("two"), "text", 0, "key")
	if err != nil || again == id {
		t.Fatalf("expired key cannot be reused: %s %v", again, err)
	}
}

func TestExpiredIdempotencyKeyCanCoexistWithOldMessage(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	now := time.Unix(1000000, 0)
	s.now = func() time.Time { return now }
	old, err := s.Send(ctx, "acme", "q", []byte("old"), "text", 0, "key")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(24 * time.Hour)
	id, err := s.Send(ctx, "acme", "q", []byte("new"), "text", 0, "key")
	if err != nil || id == old {
		t.Fatalf("expired live key blocked: %s %v", id, err)
	}
	if depth, _ := s.Depth(ctx, "acme", "q"); depth != 2 {
		t.Fatalf("old message overwritten: %d", depth)
	}
}

func TestIdempotencyCanonicalMetadataAndTransportDelay(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	a := SendMessage{Body: []byte("one"), ContentType: "text/plain", IdempotencyKey: "key", Metadata: json.RawMessage(`{"b":2,"a":9007199254740993}`)}
	ids, err := s.SendBatch(ctx, "acme", "q", []SendMessage{a})
	if err != nil {
		t.Fatal(err)
	}
	a.ContentType = "text"
	a.DelaySeconds = 10
	a.Metadata = json.RawMessage(`{ "a":9007199254740993, "b":2 }`)
	got, err := s.SendBatch(ctx, "acme", "q", []SendMessage{a})
	if err != nil || got[0] != ids[0] {
		t.Fatalf("same logical message conflicts: %v %v", got, err)
	}
	a.Metadata = json.RawMessage(`{"a":9007199254740992,"b":2}`)
	if _, err := s.SendBatch(ctx, "acme", "q", []SendMessage{a}); err == nil {
		t.Fatal("metadata integer precision lost")
	}
}

func TestTransferPreservesMetadataAndHasIndependentReceipt(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	ids, err := s.SendBatch(ctx, "acme", "jobs", []SendMessage{{Body: []byte("body"), ContentType: "text", IdempotencyKey: "business", Metadata: json.RawMessage(`{"trace":"t","n":9007199254740993}`)}})
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := s.Claim(ctx, "acme", "jobs", 1, 1000)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("claim: %v %v", msgs, err)
	}
	dlqID, err := s.Transfer(ctx, "acme", "dlq", msgs[0])
	if err != nil {
		t.Fatal(err)
	}
	if again, err := s.Transfer(ctx, "acme", "dlq", msgs[0]); err != nil || again != dlqID {
		t.Fatalf("DLQ duplicate: %s %v", again, err)
	}
	if err := s.Ack(ctx, "acme", "jobs", ids[0]); err != nil {
		t.Fatal(err)
	}
	dlq, err := s.Claim(ctx, "acme", "dlq", 1, 1000)
	if err != nil || len(dlq) != 1 {
		t.Fatal(err)
	}
	replayID, err := s.Transfer(ctx, "acme", "jobs", dlq[0])
	if err != nil || replayID == ids[0] {
		t.Fatalf("explicit replay suppressed: %s %v", replayID, err)
	}
	replayed, err := s.Claim(ctx, "acme", "jobs", 1, 1000)
	if err != nil || len(replayed) != 1 || string(replayed[0].Metadata) != string(msgs[0].Metadata) || replayed[0].IdempotencyKey != "business" {
		t.Fatalf("replay lost context: %+v %v", replayed, err)
	}
	if err := s.Ack(ctx, "acme", "jobs", replayID); err != nil {
		t.Fatal(err)
	}
	if again, err := s.Transfer(ctx, "acme", "jobs", dlq[0]); err != nil || again != replayID {
		t.Fatalf("replay receipt lost after ack: %s %v", again, err)
	}
	if depth, _ := s.Depth(ctx, "acme", "jobs"); depth != 0 {
		t.Fatalf("transfer redelivered after ack: %d", depth)
	}
}

func TestTransferDistinctSourcesDoNotCollideOnReusedBusinessKey(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	// Source producers may legitimately reuse a business key after their 24h
	// window; both older messages can still be awaiting dead-letter delivery.
	for _, m := range []Message{{ID: "old-source", Body: []byte("old"), ContentType: "text", IdempotencyKey: "key"}, {ID: "new-source", Body: []byte("new"), ContentType: "text", IdempotencyKey: "key"}} {
		if _, err := s.Transfer(ctx, "acme", "dlq", m); err != nil {
			t.Fatal(err)
		}
	}
	if depth, _ := s.Depth(ctx, "acme", "dlq"); depth != 2 {
		t.Fatalf("distinct source suppressed: %d", depth)
	}
}

func TestIdempotencyMigratesLiveMessages(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	c, err := s.cs.Cell(ctx, Scope("acme", "q"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE messages(id TEXT PRIMARY KEY,body BLOB NOT NULL,content_type TEXT NOT NULL,visible_at_ms INTEGER NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,lease_until_ms INTEGER NOT NULL DEFAULT 0,idempotency_key TEXT,idempotency_fingerprint TEXT,metadata TEXT,created_ms INTEGER NOT NULL); CREATE UNIQUE INDEX messages_idem ON messages(idempotency_key); INSERT INTO messages(id,body,content_type,visible_at_ms,idempotency_key,created_ms) VALUES('legacy',?,'text',?,'key',?)`, []byte("body"), now, now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	id, err := s.Send(ctx, "acme", "q", []byte("body"), "text", 0, "key")
	if err != nil || id != "legacy" {
		t.Fatalf("legacy dedup: %s %v", id, err)
	}
	if err := s.Ack(ctx, "acme", "q", id); err != nil {
		t.Fatal(err)
	}
	id, err = s.Send(ctx, "acme", "q", []byte("body"), "text", 0, "key")
	if err != nil || id != "legacy" {
		t.Fatalf("legacy ack dedup: %s %v", id, err)
	}
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
