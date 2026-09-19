package wake

import (
	"context"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

func TestWakeIndexPutDueDelete(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	ix := New(b)
	now := time.Now().UnixMilli()

	if err := ix.Put(ctx, "acme/__kv__/sessions", now-1, "kv-expire", "tok"); err != nil {
		t.Fatalf("put due: %v", err)
	}
	if err := ix.Put(ctx, "acme/__workflow__/far", now+time.Hour.Milliseconds(), "workflow-sleep", ""); err != nil {
		t.Fatalf("put far: %v", err)
	}
	due, err := ix.Due(ctx, now)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	if len(due) != 1 || due[0].Scope != "acme/__kv__/sessions" || due[0].Kind != "kv-expire" {
		t.Fatalf("due = %+v, want only the sessions scope", due)
	}
	if n, _ := ix.Count(ctx); n != 2 {
		t.Fatalf("count = %d, want 2", n)
	}

	if err := ix.Delete(ctx, "acme/__kv__/sessions"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	due, err = ix.Due(ctx, now+time.Hour.Milliseconds()*2)
	if err != nil || len(due) != 1 || due[0].Scope != "acme/__workflow__/far" {
		t.Fatalf("due after delete = %+v err=%v", due, err)
	}
}
