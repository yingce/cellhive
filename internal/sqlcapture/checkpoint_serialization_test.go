package sqlcapture

import (
	"context"
	"os"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

// A poll uses Stat followed by ReadAt. Truncating between those operations
// can produce an EOF instead of the checkpoint notification that rebaselines
// the durable chain. The public checkpoint must share the poll boundary.
func TestCheckpointWaitsForPollBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scope := cell.Scope{Namespace: "test", Class: "kv", ID: "checkpoint-boundary"}
	db, err := store.Open(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	baseline, err := db.TxID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := New(db, scope, 1, baseline, &recordingCommitter{})
	if err != nil {
		t.Fatal(err)
	}
	defer capture.cursor.Close()
	if _, err := db.PutTx(ctx, "key", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}
	capture.pollMu.Lock()
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() { close(started); done <- capture.Checkpoint(ctx) }()
	<-started
	select {
	case err := <-done:
		capture.pollMu.Unlock()
		t.Fatalf("checkpoint crossed active poll boundary: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	after, err := os.Stat(db.WALPath())
	capture.pollMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("WAL changed during active poll: %d -> %d", before.Size(), after.Size())
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := capture.poll(ctx); err != nil {
		t.Fatal(err)
	}
	if capture.Stats().Snapshots != 1 {
		t.Fatalf("checkpoint did not rebaseline: %+v", capture.Stats())
	}
}
