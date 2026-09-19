package peer

import (
	"context"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestLatencyTransportInjectsRoundTripDelay(t *testing.T) {
	inner := &fakeBatchTransport{}
	oneWay := 5 * time.Millisecond
	transport := NewLatencyTransport(inner, oneWay)
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "latency"}
	started := time.Now()
	if err := transport.AppendBatch(context.Background(), "http://f1", scope, 1, [][]byte{{1, 2, 3}}); err != nil {
		t.Fatalf("append batch: %v", err)
	}
	elapsed := time.Since(started)
	if elapsed < 2*oneWay {
		t.Fatalf("elapsed %s, want >= %s (request + ack)", elapsed, 2*oneWay)
	}
	inner.mu.Lock()
	defer inner.mu.Unlock()
	if inner.batches != 1 {
		t.Fatalf("inner batches = %d, want 1", inner.batches)
	}
}
