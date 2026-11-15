package bucket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestFSColdGCScansOnce(t *testing.T) {
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gc, ok := any(b).(interface {
		ListGCPage(context.Context, string, string, int) ([]ObjectInfo, string, error)
	})
	if !ok {
		t.Fatal("FS lacks a cold-GC scan without repeated full walks")
	}
	const prefix = "r2/.mpu/v2/states/"
	for i := 0; i < 257; i++ {
		if _, err := b.Put(t.Context(), fmt.Sprintf("%sns/files/v2-%032x/created", prefix, i), []byte("state")); err != nil {
			t.Fatal(err)
		}
	}
	var cursor string
	count := 0
	for {
		items, next, err := gc.ListGCPage(t.Context(), prefix, cursor, 7)
		if err != nil {
			t.Fatal(err)
		}
		count += len(items)
		cursor = next
		if cursor == "" {
			break
		}
	}
	if count != 257 || b.Stats()["gc_scan_entries"] != 257 {
		t.Fatalf("count=%d physical file visits=%d", count, b.Stats()["gc_scan_entries"])
	}
}

func TestFSColdGCSnapshotOrderAndLifecycle(t *testing.T) {
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"cold/a/x", "cold/a!", "cold/a0/x"} {
		if _, err := b.Put(t.Context(), key, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	first, cursor, err := b.ListGCPage(t.Context(), "cold/", "", 1)
	if err != nil || len(first) != 1 || first[0].Key != "cold/a!" {
		t.Fatalf("lexical order: %v %v", first, err)
	}
	_, other, err := b.ListGCPage(t.Context(), "cold/", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.ListGCPage(t.Context(), "cold/", "", 1); !errors.Is(err, ErrGCScanBusy) {
		t.Fatalf("unbounded snapshots: %v", err)
	}
	var decoded fsGCCursor
	if err := json.Unmarshal([]byte(cursor), &decoded); err != nil {
		t.Fatal(err)
	}
	b.gcMu.Lock()
	previous := b.gcScans[decoded.ID].expires
	b.gcMu.Unlock()
	if err := b.KeepGCScanAlive(t.Context(), "cold/", cursor); err != nil {
		t.Fatal(err)
	}
	b.gcMu.Lock()
	renewed := b.gcScans[decoded.ID].expires
	b.gcMu.Unlock()
	if !renewed.After(previous) {
		t.Fatal("pending-page lease was not renewed")
	}
	if _, err := b.Put(t.Context(), "cold/new", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete(t.Context(), "cold/a0/x"); err != nil {
		t.Fatal(err)
	}
	items, next, err := b.ListGCPage(t.Context(), "cold/", cursor, 10)
	if err != nil || next != "" || !reflect.DeepEqual(items, []ObjectInfo{{Key: "cold/a/x"}, {Key: "cold/a0/x"}}) {
		t.Fatalf("not a stable discovery snapshot: %v %q %v", items, next, err)
	}
	if err := b.KeepGCScanAlive(t.Context(), "cold/", cursor); !errors.Is(err, ErrGCScanExpired) {
		t.Fatalf("exhausted snapshot retained: %v", err)
	}
	live, err := b.List(t.Context(), "cold/")
	if err != nil || !reflect.DeepEqual(live, []string{"cold/a!", "cold/a/x", "cold/new"}) {
		t.Fatalf("live listing cached: %v %v", live, err)
	}
	if err := json.Unmarshal([]byte(other), &decoded); err != nil {
		t.Fatal(err)
	}
	// Fire the real abandonment timer promptly, without a production test hook.
	b.gcMu.Lock()
	b.gcScans[decoded.ID].timer.Reset(time.Millisecond)
	b.gcMu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		b.gcMu.Lock()
		count := len(b.gcScans)
		b.gcMu.Unlock()
		if count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("abandoned snapshot not released by timer")
		}
		time.Sleep(time.Millisecond)
	}
	if _, _, err := b.ListGCPage(t.Context(), "cold/", other, 1); !errors.Is(err, ErrGCScanExpired) {
		t.Fatalf("stale cursor accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := b.Stats()["gc_scan_entries"]
	if _, _, err := b.ListGCPage(ctx, "cold/", "", 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if b.Stats()["gc_scan_entries"] != before {
		t.Fatal("cancelled scan performed I/O")
	}
}
