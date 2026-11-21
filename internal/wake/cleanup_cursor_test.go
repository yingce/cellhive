package wake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

// countingBucket records how many keys each cleanup pass reads, so we can prove
// a retry-free minute is not re-listed from its start on every pass.
type countingBucket struct {
	*bucket.FSBucket
	mu     sync.Mutex
	listed int
	gets   int
}

func (b *countingBucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	page, next, err := b.FSBucket.ListPage(ctx, prefix, after, limit)
	b.mu.Lock()
	b.listed += len(page)
	b.mu.Unlock()
	return page, next, err
}

func (b *countingBucket) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := b.FSBucket.List(ctx, prefix)
	b.mu.Lock()
	b.listed += len(keys)
	b.mu.Unlock()
	return keys, err
}

func (b *countingBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.mu.Lock()
	b.gets++
	b.mu.Unlock()
	return b.FSBucket.Get(ctx, key)
}

func (b *countingBucket) reset() {
	b.mu.Lock()
	b.listed, b.gets = 0, 0
	b.mu.Unlock()
}

func (b *countingBucket) counts() (int, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.listed, b.gets
}

// A single minute holding far more entries than the per-pass budget must resume
// from the last inspected key instead of re-listing the whole minute. With a
// minute-grained cursor every pass pays the full ListPrefix again and re-reads
// the retained entries that sort before the budget cut.
func TestCleanupPageResumesWithinOneMinuteWithoutReListing(t *testing.T) {
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &countingBucket{FSBucket: raw}
	ix := New(b)
	ctx := t.Context()

	base := time.Now().Add(-3 * time.Hour).Truncate(time.Minute)
	// One old minute. Even entries are orphans (pointer dropped), odd entries
	// must be retained: they keep their pointer and sort interleaved.
	const total = 40
	for n := 0; n < total; n++ {
		scope := fmt.Sprintf("acme/__kv__/mix-%02d", n)
		if err := ix.Put(ctx, scope, base.UnixMilli(), "", ""); err != nil {
			t.Fatal(err)
		}
		if n%2 == 0 {
			if err := raw.Delete(ctx, ptrKey(scope)); err != nil {
				t.Fatal(err)
			}
		}
	}

	cut := time.Now().UnixMilli()
	var (
		afterMinute string
		afterKey    string
		removed     int
		passes      int
		work        []int
	)
	for pass := 0; pass < 200; pass++ {
		b.reset()
		n, next, err := ix.CleanupPage(ctx, cut, 4, wakeCursor{Minute: afterMinute, Key: afterKey})
		if err != nil {
			t.Fatal(err)
		}
		read, _ := b.counts()
		if read > 0 {
			work = append(work, read)
		}
		removed += n
		passes++
		if next.Minute == "" && next.Key == "" {
			break
		}
		afterMinute, afterKey = next.Minute, next.Key
	}
	if removed != total/2 {
		t.Fatalf("removed=%d want %d orphans", removed, total/2)
	}
	if passes < 3 {
		t.Fatalf("passes=%d: the run was not split across budgets", passes)
	}
	// Re-reading the whole minute on every pass shows up as each pass touching
	// the full key count. Allow one full listing for the first pass plus small
	// bounded work afterwards.
	for i, read := range work {
		if i > 0 && read >= total/2 {
			t.Fatalf("pass %d re-listed %d keys; cursor did not advance within the minute", i, read)
		}
	}
}

// A transient read/pointer error must not pin the cleanup cursor: the pass skips
// that key, keeps advancing, and still drains the rest of the minute.
func TestCleanupPageSkipsUnreadableEntriesAndKeepsAdvancing(t *testing.T) {
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &flakyGetBucket{FSBucket: raw, failKeySuffix: "/__kv__/poison"}
	ix := New(raw)
	ctx := t.Context()

	base := time.Now().Add(-3 * time.Hour).Truncate(time.Minute)
	for _, id := range []string{"a-ok", "poison", "z-ok"} {
		scope := "acme/__kv__/" + id
		if err := ix.Put(ctx, scope, base.UnixMilli(), "", ""); err != nil {
			t.Fatal(err)
		}
		if err := raw.Delete(ctx, ptrKey(scope)); err != nil {
			t.Fatal(err)
		}
	}
	// Install the fault only for the cleanup pass: Put itself reads the pointer.
	ix = New(b)
	cut := time.Now().UnixMilli()
	var (
		cur     wakeCursor
		removed int
	)
	for pass := 0; pass < 10; pass++ {
		n, next, err := ix.CleanupPage(ctx, cut, 1024, cur)
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		removed += n
		if next.Minute == "" && next.Key == "" {
			break
		}
		cur = next
	}
	// Two orphans are removable; the unreadable one is skipped, not retried.
	if removed != 2 {
		t.Fatalf("removed=%d, want 2 (unreadable entry skipped, others drained)", removed)
	}
	if _, _, err := b.Get(ctx, ptrKey("acme/__kv__/poison")); err == nil {
		t.Fatal("poison pointer unexpectedly present")
	}
}

type flakyGetBucket struct {
	*bucket.FSBucket
	failKeySuffix string
}

func (b *flakyGetBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.Contains(key, b.failKeySuffix) {
		return nil, "", errors.New("injected transient read failure")
	}
	return b.FSBucket.Get(ctx, key)
}

// CleanupDrain must report bounded per-pass work as well.
func TestCleanupDrainWorkIsBoundedWithinOneMinute(t *testing.T) {
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &countingBucket{FSBucket: raw}
	ix := New(b)
	ctx := t.Context()

	base := time.Now().Add(-3 * time.Hour).Truncate(time.Minute)
	const total = 30
	for n := 0; n < total; n++ {
		scope := fmt.Sprintf("acme/__kv__/drain-%02d", n)
		if err := ix.Put(ctx, scope, base.UnixMilli(), "", ""); err != nil {
			t.Fatal(err)
		}
		if err := raw.Delete(ctx, ptrKey(scope)); err != nil {
			t.Fatal(err)
		}
	}
	cut := time.Now().UnixMilli()
	removed, err := ix.CleanupDrain(ctx, cut, 3, 100, 100)
	if err != nil || removed != total {
		t.Fatalf("removed=%d err=%v want %d", removed, err, total)
	}
	read, _ := b.counts()
	// Re-listing a minute per page would read total*ceil(total/3) keys.
	if read > total*2 {
		t.Fatalf("cleanup read %d keys for %d entries; minute was re-listed per page", read, total)
	}
}

// Pointer-held entries cannot be deleted. One drain must stop after its scan
// budget and the next drain must continue past those keys, not walk the whole
// minute again from the start.
func TestCleanupDrainStopsWhenNothingCanBeDeleted(t *testing.T) {
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &keyListBucket{FSBucket: raw}
	ix := New(b)
	ctx := t.Context()
	base := time.Now().Add(-3 * time.Hour).Truncate(time.Minute)
	const total = 40
	for n := 0; n < total; n++ {
		scope := fmt.Sprintf("acme/__kv__/live-%02d", n)
		if err := ix.Put(ctx, scope, base.UnixMilli(), "", ""); err != nil {
			t.Fatal(err)
		}
	}
	cut := time.Now().UnixMilli()
	removed, err := ix.CleanupDrain(ctx, cut, 10, 1000, 10)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("removed=%d, live pointers must stay", removed)
	}
	first := b.snapshot()
	if len(first) >= total {
		t.Fatalf("first drain listed %d keys; scan budget did not stop", len(first))
	}
	if len(first) > 12 {
		t.Fatalf("first drain listed %d keys, budget is 10", len(first))
	}
	b.resetKeys()
	removed, err = ix.CleanupDrain(ctx, cut, 10, 1000, 10)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("removed=%d", removed)
	}
	second := b.snapshot()
	if len(second) == 0 {
		t.Fatal("second drain did not continue")
	}
	if len(second) > 12 {
		t.Fatalf("second drain listed %d keys, budget is 10", len(second))
	}
	overlap := map[string]bool{}
	for _, key := range first {
		overlap[key] = true
	}
	shared := 0
	for _, key := range second {
		if overlap[key] {
			shared++
		}
	}
	if shared > 1 {
		t.Fatalf("second drain re-read %d keys from the first pass", shared)
	}
}

func TestCleanupDrainStopsWhenTheContextEndsAndResumes(t *testing.T) {
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	b := &cancelingBucket{FSBucket: raw, stopAfter: 1, cancel: cancel}
	ix := New(b)
	base := time.Now().Add(-3 * time.Hour).Truncate(time.Minute)
	const total = 4
	for n := 0; n < total; n++ {
		scope := fmt.Sprintf("acme/__kv__/stop-%02d", n)
		if err := ix.Put(t.Context(), scope, base.UnixMilli(), "", ""); err != nil {
			t.Fatal(err)
		}
		if err := raw.Delete(t.Context(), ptrKey(scope)); err != nil {
			t.Fatal(err)
		}
	}
	if got := dueKeyCount(t, ix, t.Context()); got != total {
		t.Fatalf("planted %d due keys", got)
	}
	removed, err := ix.CleanupDrain(ctx, time.Now().UnixMilli(), 10, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d, want the one key inspected before cancel", removed)
	}
	if got := dueKeyCount(t, ix, t.Context()); got != total-1 {
		t.Fatalf("due keys=%d", got)
	}
	b.stopAfter = 1 << 30
	removed, err = ix.CleanupDrain(t.Context(), time.Now().UnixMilli(), 10, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if removed != total-1 {
		t.Fatalf("resumed removed=%d, want %d", removed, total-1)
	}
	if got := dueKeyCount(t, ix, t.Context()); got != 0 {
		t.Fatalf("due keys left=%d", got)
	}
}

func TestCleanupDrainAlreadyCanceledRemovesNothing(t *testing.T) {
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ix := New(raw)
	base := time.Now().Add(-3 * time.Hour).Truncate(time.Minute)
	scope := "acme/__kv__/already-canceled"
	if err := ix.Put(t.Context(), scope, base.UnixMilli(), "", ""); err != nil {
		t.Fatal(err)
	}
	if err := raw.Delete(t.Context(), ptrKey(scope)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	removed, err := ix.CleanupDrain(ctx, time.Now().UnixMilli(), 10, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("removed=%d", removed)
	}
	if got := dueKeyCount(t, ix, t.Context()); got != 1 {
		t.Fatalf("due keys=%d", got)
	}
	removed, err = ix.CleanupDrain(t.Context(), time.Now().UnixMilli(), 10, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d", removed)
	}
}

func dueKeyCount(t *testing.T, ix *Index, ctx context.Context) int {
	t.Helper()
	keys, err := ix.store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, key := range keys {
		if strings.Contains(key, "/due/") && strings.HasSuffix(key, ".json") {
			n++
		}
	}
	return n
}

type cancelingBucket struct {
	*bucket.FSBucket
	mu        sync.Mutex
	dueGets   int
	stopAfter int
	cancel    context.CancelFunc
}

func (b *cancelingBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.Contains(key, "/due/") {
		b.mu.Lock()
		b.dueGets++
		n := b.dueGets
		stop := b.stopAfter
		b.mu.Unlock()
		if n > stop {
			b.cancel()
			return nil, "", context.Canceled
		}
	}
	return b.FSBucket.Get(ctx, key)
}

type keyListBucket struct {
	*bucket.FSBucket
	mu   sync.Mutex
	keys []string
}

func (b *keyListBucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	page, next, err := b.FSBucket.ListPage(ctx, prefix, after, limit)
	b.mu.Lock()
	for _, info := range page {
		b.keys = append(b.keys, info.Key)
	}
	b.mu.Unlock()
	return page, next, err
}

func (b *keyListBucket) snapshot() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.keys))
	copy(out, b.keys)
	return out
}

func (b *keyListBucket) resetKeys() {
	b.mu.Lock()
	b.keys = nil
	b.mu.Unlock()
}
