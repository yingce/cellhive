package replica

import (
	"context"
	"errors"
	"sync"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

func gcTestSnapshot(t *testing.T, epoch, txid uint64) []byte {
	t.Helper()
	return nativeBlob(t, 1, txid, byte(epoch))
}

func gcTestManager(t *testing.T) (*Manager, bucket.Bucket, cell.Scope) {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return New(b), b, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "gcproto"}
}

func TestDefaultRetireStillDefers(t *testing.T) {
	ctx := context.Background()
	m, _, sc := gcTestManager(t)
	for epoch := uint64(1); epoch <= 2; epoch++ {
		if _, _, err := m.Append(ctx, sc, epoch, gcTestSnapshot(t, epoch, epoch)); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := m.RetireEpochs(ctx, sc, 2, 0); !errors.Is(err, ErrGCDeferred) || n != 0 {
		t.Fatalf("default retire = %d, %v; want deferred", n, err)
	}
}

type failGetOnce struct {
	*bucket.FSBucket
	key    string
	mu     sync.Mutex
	failed bool
}

func (b *failGetOnce) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.mu.Lock()
	fail := key == b.key && !b.failed
	if fail {
		b.failed = true
	}
	b.mu.Unlock()
	if fail {
		return nil, "", bucket.ErrNotFound
	}
	return b.FSBucket.Get(ctx, key)
}

func TestRestoreRetriesTransientObjectDisappearance(t *testing.T) {
	ctx := context.Background()
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &failGetOnce{FSBucket: fs}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "restore-retry"}
	snap := gcTestSnapshot(t, 1, 1)
	if _, _, err := m.Append(ctx, sc, 1, snap); err != nil {
		t.Fatal(err)
	}
	deltaSeg := nativeBlob(t, 2, 2, 9)
	key, _, err := m.Append(ctx, sc, 1, deltaSeg)
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.key = key
	b.failed = false
	b.mu.Unlock()
	if segs, err := m.RestoreChain(ctx, sc, 1); err != nil || len(segs) == 0 {
		t.Fatalf("chain restore after transient miss = %d, %v", len(segs), err)
	}
	b.mu.Lock()
	b.failed = false
	b.mu.Unlock()
	if segs, err := m.Restore(ctx, sc, 1); err != nil || len(segs) == 0 {
		t.Fatalf("epoch restore after transient miss = %d, %v", len(segs), err)
	}
}
