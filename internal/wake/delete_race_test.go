package wake

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

type pausedWakeDelete struct {
	bucket.Bucket
	key              string
	entered, release chan struct{}
	once             bool
}

func (b *pausedWakeDelete) pause(key string) {
	if key == b.key && !b.once {
		b.once = true
		close(b.entered)
		<-b.release
	}
}
func (b *pausedWakeDelete) Delete(ctx context.Context, key string) error {
	b.pause(key)
	return b.Bucket.Delete(ctx, key)
}
func (b *pausedWakeDelete) ConditionalDelete(ctx context.Context, key, etag string) error {
	b.pause(key)
	return b.Bucket.ConditionalDelete(ctx, key, etag)
}

// Same payload/minute must still be a distinct publication, so even a correct
// content-based If-Match cannot delete a later incarnation with the old token.
func TestWakeStaleDeletePreservesIdenticalRepublication(t *testing.T) {
	for _, target := range []string{"pointer", "due"} {
		t.Run(target, func(t *testing.T) {
			base, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			scope := "acme/__kv__/recreated"
			due := time.Now().Add(-time.Second).UnixMilli()
			fresh := New(base)
			if err = fresh.Put(t.Context(), scope, due, "", ""); err != nil {
				t.Fatal(err)
			}
			key := ptrKey(scope)
			if target == "due" {
				key = dueKey(due, scope)
			}
			blocked := &pausedWakeDelete{Bucket: base, key: key, entered: make(chan struct{}), release: make(chan struct{})}
			done := make(chan error, 1)
			go func() { done <- New(blocked).Delete(t.Context(), scope) }()
			select {
			case <-blocked.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("delete did not reach pause")
			}
			if err = fresh.Put(t.Context(), scope, due, "", ""); err != nil {
				close(blocked.release)
				t.Fatal(err)
			}
			expected := map[string]string{}
			for _, k := range []string{ptrKey(scope), dueKey(due, scope)} {
				data, _, err := base.Get(t.Context(), k)
				if err != nil {
					t.Fatal(err)
				}
				expected[k] = string(data)
			}
			close(blocked.release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			for k, want := range expected {
				got, _, err := base.Get(t.Context(), k)
				if err != nil || string(got) != want {
					t.Fatalf("successor %s lost: %q err=%v", strings.TrimPrefix(k, "wake/"), got, err)
				}
			}
			entries, err := fresh.Due(t.Context(), time.Now().UnixMilli())
			if err != nil || len(entries) != 1 || entries[0].Scope != scope {
				t.Fatalf("successor not discoverable: %+v err=%v", entries, err)
			}
		})
	}
}

func TestWakeMaintenanceDeletePreservesRepublication(t *testing.T) {
	for _, mode := range []string{"reschedule", "orphan-cleanup"} {
		t.Run(mode, func(t *testing.T) {
			base, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			scope := "acme/__kv__/maintenance"
			oldDue := time.Now().Add(-2 * time.Hour).UnixMilli()
			future := time.Now().Add(time.Hour).UnixMilli()
			ix := New(base)
			if err = ix.Put(t.Context(), scope, oldDue, "", ""); err != nil {
				t.Fatal(err)
			}
			key := dueKey(oldDue, scope)
			original, _, err := base.Get(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "orphan-cleanup" {
				if err = ix.Put(t.Context(), scope, future, "", ""); err != nil {
					t.Fatal(err)
				}
				if _, err = base.Put(t.Context(), key, original); err != nil {
					t.Fatal(err)
				}
			}
			paused := &pausedWakeDelete{Bucket: base, key: key, entered: make(chan struct{}), release: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				p := New(paused)
				if mode == "reschedule" {
					done <- p.Put(t.Context(), scope, future, "", "")
				} else {
					_, err := p.Cleanup(t.Context(), time.Now().UnixMilli(), 10)
					done <- err
				}
			}()
			select {
			case <-paused.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("delete did not reach pause")
			}
			if err = ix.Put(t.Context(), scope, oldDue, "", ""); err != nil {
				close(paused.release)
				t.Fatal(err)
			}
			expected, _, err := base.Get(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}
			close(paused.release)
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			got, _, err := base.Get(t.Context(), key)
			if err != nil || string(got) != string(expected) {
				t.Fatalf("successor hint lost: %q %v", got, err)
			}
		})
	}
}

func TestWakeDeleteRevalidatesAuthorityAfterCandidates(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scope := "acme/__kv__/lease"
	due := time.Now().UnixMilli()
	ix := New(base)
	if err = ix.Put(t.Context(), scope, due, "", ""); err != nil {
		t.Fatal(err)
	}
	called := false
	err = ix.DeleteValidated(t.Context(), scope, func() error {
		called = true
		// A successor becomes current after the candidates were captured.
		if err := ix.Put(t.Context(), scope, due, "", ""); err != nil {
			return err
		}
		return context.Canceled
	})
	if err != context.Canceled || !called {
		t.Fatalf("authority check: %v called=%v", err, called)
	}
	for _, key := range []string{ptrKey(scope), dueKey(due, scope)} {
		if _, _, err := base.Get(t.Context(), key); err != nil {
			t.Fatalf("successor missing: %v", err)
		}
	}
}

type unsupportedWakeDelete struct {
	bucket.Bucket
	unconditional int
}

func (b *unsupportedWakeDelete) SupportsConditionalDelete(string) bool { return false }
func (b *unsupportedWakeDelete) ConditionalDelete(context.Context, string, string) error {
	return bucket.ErrNotSupported
}
func (b *unsupportedWakeDelete) Delete(ctx context.Context, key string) error {
	b.unconditional++
	return b.Bucket.Delete(ctx, key)
}
func TestWakeUnsupportedConditionalDeleteUsesImmutableReclaim(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &unsupportedWakeDelete{Bucket: base}
	ix := New(b)
	if !ix.immutableHints {
		t.Fatal("expected immutable hints when ConditionalDelete is unsupported")
	}
	scope := "acme/__kv__/unsupported"
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	future := time.Now().Add(time.Hour).UnixMilli()
	if err = ix.Put(t.Context(), scope, old, "", ""); err != nil {
		t.Fatal(err)
	}
	if err = ix.Put(t.Context(), scope, future, "", ""); err != nil {
		t.Fatalf("reschedule blocked by hint reclaim: %v", err)
	}
	current, err := ix.pointer(t.Context(), scope)
	if err != nil || current.DueMs != future {
		t.Fatalf("current=%+v err=%v", current, err)
	}
	// Prior due revision must be physically gone after reschedule reclaim.
	oldKeys, err := base.List(t.Context(), duePrefix(old))
	if err != nil || len(oldKeys) != 0 {
		t.Fatalf("stale due minute retained: %v %v", oldKeys, err)
	}
	if b.unconditional == 0 {
		t.Fatal("expected Delete reclaim of obsolete immutable hint")
	}
	if err = ix.Delete(t.Context(), scope); err != nil {
		t.Fatalf("delete blocked: %v", err)
	}
}

func TestWakeCleanupPageResumesAcrossBudget(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ix := New(immutableHintBucket{b})
	ctx := t.Context()
	baseMs := time.Now().Add(-3 * time.Hour).UnixMilli()
	for n := 0; n < 6; n++ {
		scope := fmt.Sprintf("acme/__kv__/drain-%d", n)
		due := baseMs + int64(n)*60000
		if err := ix.Put(ctx, scope, due, "", ""); err != nil {
			t.Fatal(err)
		}
		// Drop the pointer only: due entry becomes a Cleanup orphan.
		if err := b.Delete(ctx, ptrKey(scope)); err != nil {
			t.Fatal(err)
		}
	}
	cut := time.Now().UnixMilli()
	total := 0
	var cur wakeCursor
	for pass := 0; pass < 20; pass++ {
		n, next, err := ix.CleanupPage(ctx, cut, 2, cur)
		if err != nil {
			t.Fatal(err)
		}
		total += n
		if next.Minute == "" && next.Key == "" {
			break
		}
		cur = next
	}
	if total < 6 {
		t.Fatalf("cleanup drained too few orphans: %d", total)
	}
	// Same pile drained via CleanupDrain with a tight page budget.
	for n := 0; n < 6; n++ {
		scope := fmt.Sprintf("acme/__kv__/drain2-%d", n)
		due := baseMs + int64(n)*60000
		if err := ix.Put(ctx, scope, due, "", ""); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete(ctx, ptrKey(scope)); err != nil {
			t.Fatal(err)
		}
	}
	drained, err := ix.CleanupDrain(ctx, cut, 2, 100, 100)
	if err != nil || drained < 6 {
		t.Fatalf("CleanupDrain removed=%d err=%v", drained, err)
	}
}
