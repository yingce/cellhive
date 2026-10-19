package queue

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"cellhive/internal/cellstore"
)

func TestProducerContextSurvivesRetryIdempotencyAndTransfer(t *testing.T) {
	ctx := context.Background()
	store := newInternalStore(t)
	first := SendMessage{Body: []byte("first"), ContentType: "text", IdempotencyKey: "same", ProducerRequestID: "request-a", ProducerTraceparent: "00-11111111111111111111111111111111-2222222222222222-01"}
	second := SendMessage{Body: []byte("second"), ContentType: "text", ProducerRequestID: "request-b", ProducerTraceparent: "00-33333333333333333333333333333333-4444444444444444-01"}
	ids, err := store.SendBatch(ctx, "demo", "source", []SendMessage{first, second})
	if err != nil {
		t.Fatal(err)
	}
	first.ProducerRequestID, first.ProducerTraceparent = "replay-request", "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
	replayed, err := store.SendBatch(ctx, "demo", "source", []SendMessage{first})
	if err != nil || replayed[0] != ids[0] {
		t.Fatalf("idempotent replay: %v %v", replayed, err)
	}
	items, err := store.Claim(ctx, "demo", "source", 10, 1000)
	if err != nil || len(items) != 2 {
		t.Fatalf("claim: %+v %v", items, err)
	}
	byID := map[string]Message{}
	for _, item := range items {
		byID[item.ID] = item
	}
	if byID[ids[0]].ProducerRequestID != "request-a" || byID[ids[0]].ProducerTraceparent != "00-11111111111111111111111111111111-2222222222222222-01" || byID[ids[1]].ProducerRequestID != "request-b" {
		t.Fatalf("mixed contexts: %+v", items)
	}
	if err := store.Retry(ctx, "demo", "source", ids[0], 0); err != nil {
		t.Fatal(err)
	}
	retried, err := store.Claim(ctx, "demo", "source", 1, 1000)
	if err != nil || len(retried) != 1 || retried[0].ProducerRequestID != "request-a" {
		t.Fatalf("retry context: %+v %v", retried, err)
	}
	dlqID, err := store.Transfer(ctx, "demo", "dlq", byID[ids[1]])
	if err != nil {
		t.Fatal(err)
	}
	dlq, _, err := store.PeekPage(ctx, "demo", "dlq", "", 10)
	if err != nil || len(dlq) != 1 || dlq[0].ID != dlqID || dlq[0].ProducerRequestID != "request-b" || dlq[0].ProducerTraceparent != second.ProducerTraceparent {
		t.Fatalf("dlq context: %+v %v", dlq, err)
	}
}

func TestProducerContextMigratesExistingQueue(t *testing.T) {
	ctx := context.Background()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	cell, err := cs.Cell(ctx, Scope("demo", "old"))
	if err != nil {
		t.Fatal(err)
	}
	oldSchema := strings.ReplaceAll(schema, "  producer_request_id TEXT NOT NULL DEFAULT '',\n", "")
	oldSchema = strings.ReplaceAll(oldSchema, "  producer_traceparent TEXT NOT NULL DEFAULT '',\n", "")
	if _, err := cell.Tx(ctx, func(tx *sql.Tx) error { _, err := tx.ExecContext(ctx, oldSchema); return err }); err != nil {
		t.Fatal(err)
	}
	store := New(cs)
	ids, err := store.SendBatch(ctx, "demo", "old", []SendMessage{{Body: []byte("new"), ContentType: "text", ProducerRequestID: "r1"}})
	if err != nil {
		t.Fatal(err)
	}
	items, _, err := store.PeekPage(ctx, "demo", "old", "", 10)
	if err != nil || len(items) != 1 || items[0].ID != ids[0] || items[0].ProducerRequestID != "r1" {
		t.Fatalf("migrated queue: %+v %v", items, err)
	}
}
