package lease

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

type delayedDiscovery struct {
	bucket.Bucket
	active, max atomic.Int32
}

func (b *delayedDiscovery) Get(ctx context.Context, k string) ([]byte, string, error) {
	n := b.active.Add(1)
	defer b.active.Add(-1)
	for {
		old := b.max.Load()
		if n <= old || b.max.CompareAndSwap(old, n) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-time.After(20 * time.Millisecond):
	}
	return b.Bucket.Get(ctx, k)
}
func TestDiscoveryReachesLateNodesWithinDeadline(t *testing.T) {
	raw, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 80; i++ {
		if _, err := raw.Put(t.Context(), fmt.Sprintf("nodes/n%03d.json", i), []byte(fmt.Sprintf(`{"node":"n%03d"}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	b := &delayedDiscovery{Bucket: raw}
	m := NewManager(b, "self", "session", "", "", time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	leases, err := m.Sample(ctx)
	if err != nil || len(leases) != 80 {
		t.Fatalf("sample=%d %v", len(leases), err)
	}
	if b.max.Load() > 8 || b.max.Load() < 2 {
		t.Fatalf("concurrency=%d", b.max.Load())
	}
	ctx2, cancel2 := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel2()
	if partial, err := m.Sample(ctx2); err == nil || partial != nil {
		t.Fatalf("published incomplete sample %d %v", len(partial), err)
	}
}
