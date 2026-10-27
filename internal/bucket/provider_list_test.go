package bucket

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestResolveListedWindowsBoundedParallelAndFiltering(t *testing.T) {
	windows := make(map[string]int64)
	for i := 0; i < 32; i++ {
		windows[fmt.Sprint(i)] = int64(i)
	}
	var active, maximum atomic.Int32
	started := make(chan struct{}, 32)
	release := make(chan struct{})
	type result struct {
		keys []string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		keys, err := resolveListedWindows(t.Context(), windows, func(ctx context.Context, key string, window int64) (bool, error) {
			n := active.Add(1)
			defer active.Add(-1)
			for old := maximum.Load(); n > old; old = maximum.Load() {
				if maximum.CompareAndSwap(old, n) {
					break
				}
			}
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return false, ctx.Err()
			}
			return window%2 == 0, nil
		})
		done <- result{keys, err}
	}()
	// A serial implementation cannot reach eight reads while the first is blocked.
	for i := 0; i < 8; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			<-done
			t.Fatal("discovery reads are serialized")
		}
	}
	close(release)
	r := <-done
	if r.err != nil || len(r.keys) != 16 || maximum.Load() != 8 {
		t.Fatalf("keys=%d max=%d err=%v", len(r.keys), maximum.Load(), r.err)
	}
	seen := map[string]bool{}
	for _, key := range r.keys {
		if seen[key] || windows[key]%2 != 0 {
			t.Fatalf("invalid logical key %s", key)
		}
		seen[key] = true
	}
}

func TestResolveListedWindowsFailureNeverReturnsPartial(t *testing.T) {
	sentinel := errors.New("read failed")
	keys, err := resolveListedWindows(t.Context(), map[string]int64{"live": 1, "failed": 2}, func(_ context.Context, key string, _ int64) (bool, error) {
		if key == "failed" {
			return false, sentinel
		}
		return true, nil
	})
	if keys != nil || !errors.Is(err, sentinel) {
		t.Fatalf("partial=%v err=%v", keys, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	keys, err = resolveListedWindows(ctx, map[string]int64{"live": 1}, func(context.Context, string, int64) (bool, error) {
		t.Error("read after cancellation")
		return true, nil
	})
	if keys != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("partial=%v err=%v", keys, err)
	}
}

func TestProviderListCloud(t *testing.T) {
	b := openProviderForFenceTest(t)
	prefix := fmt.Sprintf("nodes/probe/list-parallel-%d/", time.Now().UnixNano())
	live, deleted := prefix+"live.json", prefix+"deleted.json"
	for _, key := range []string{live, deleted} {
		if _, err := b.ConditionalCreate(t.Context(), key, []byte(`{"value":1}`)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Delete(context.Background(), key) })
	}
	if err := b.Delete(t.Context(), deleted); err != nil {
		t.Fatal(err)
	}
	keys, err := b.List(t.Context(), prefix)
	if err != nil || len(keys) != 1 || keys[0] != live {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
}
