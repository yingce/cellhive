package wake

import (
	"context"
	"fmt"
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

func TestWakeDueMinuteBucketsAndCleanup(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ix := New(b)
	now := time.Now().UnixMilli()

	// A current entry is discoverable without scanning future minutes.
	if err := ix.Put(ctx, "acme/__kv__/live", now-1000, "kv-expire", "t1"); err != nil {
		t.Fatal(err)
	}
	if err := ix.Put(ctx, "acme/__workflow__/future", now+time.Hour.Milliseconds(), "workflow-sleep", "t2"); err != nil {
		t.Fatal(err)
	}
	due, err := ix.Due(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Scope != "acme/__kv__/live" {
		t.Fatalf("due = %+v", due)
	}

	// Simulate a crashed reschedule: an old due entry remains while the
	// pointer names a newer minute. Cleanup retires the orphan.
	oldDue := now - 2*time.Hour.Milliseconds()
	orphanKey := dueKey(oldDue, "acme/__kv__/live")
	if _, err := b.Put(ctx, orphanKey, []byte(`{"scope":"acme/__kv__/live","due_ms":1}`)); err != nil {
		t.Fatal(err)
	}
	removed, err := ix.Cleanup(ctx, now-time.Hour.Milliseconds(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("cleanup removed %d, want 1", removed)
	}
	if _, _, err := b.Get(ctx, orphanKey); err == nil {
		t.Fatal("orphan due entry still exists")
	}
	if _, _, err := b.Get(ctx, ptrKey("acme/__kv__/live")); err != nil {
		t.Fatalf("live pointer was removed: %v", err)
	}
}

func TestWakeDeleteRemovesDueEntry(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ix := New(b)
	now := time.Now().UnixMilli()
	if err := ix.Put(ctx, "acme/__kv__/gone", now, "kv-expire", "t"); err != nil {
		t.Fatal(err)
	}
	key := dueKey(now, "acme/__kv__/gone")
	if _, _, err := b.Get(ctx, key); err != nil {
		t.Fatalf("due entry missing before delete: %v", err)
	}
	if err := ix.Delete(ctx, "acme/__kv__/gone"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Get(ctx, key); err == nil {
		t.Fatal("due entry survived delete")
	}
}

func TestWakeMigratesLegacyEntry(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ix := New(b)
	now := time.Now().UnixMilli()
	legacy := legacyKey("acme/__kv__/legacy")
	if _, err := b.Put(ctx, legacy, []byte(`{"scope":"acme/__kv__/legacy","due_ms":`+fmt.Sprint(now-1000)+`,"kind":"kv-expire"}`)); err != nil {
		t.Fatal(err)
	}
	if n, err := ix.Migrate(ctx, 0); err != nil || n != 1 {
		t.Fatalf("migrate = %d, %v", n, err)
	}
	if _, _, err := b.Get(ctx, legacy); err == nil {
		t.Fatal("legacy entry survived migration")
	}
	due, err := ix.Due(ctx, now)
	if err != nil || len(due) != 1 || due[0].Scope != "acme/__kv__/legacy" {
		t.Fatalf("due after migration = %+v, %v", due, err)
	}
}

func TestWakeCleanupRemovesNoPointerOrphan(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ix := New(b)
	now := time.Now().UnixMilli()
	key := dueKey(now-2*time.Hour.Milliseconds(), "acme/__kv__/orphan")
	if _, err := b.Put(ctx, key, []byte(`{"scope":"acme/__kv__/orphan","due_ms":1}`)); err != nil {
		t.Fatal(err)
	}
	removed, err := ix.Cleanup(ctx, now-time.Hour.Milliseconds(), 10)
	if err != nil || removed != 1 {
		t.Fatalf("cleanup = %d, %v", removed, err)
	}
	if _, _, err := b.Get(ctx, key); err == nil {
		t.Fatal("no-pointer orphan survived cleanup")
	}
}
