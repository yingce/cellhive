package replica

import (
	"context"
	"sync"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

type restoreReadBucket struct {
	bucket.Bucket
	mu   sync.Mutex
	read map[string]int
}

func (b *restoreReadBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.mu.Lock()
	b.read[key]++
	b.mu.Unlock()
	return b.Bucket.Get(ctx, key)
}
func TestNativeRestoreDoesNotDownloadCompactedL0(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &restoreReadBucket{Bucket: base, read: map[string]int{}}
	m := New(b)
	sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: "prune"}
	high := putNativeEpochFile(t, m, sc, 1, 1, 1, 10, false)
	var covered []string
	for i := uint64(1); i <= 12; i++ {
		k := putNativeEpochFile(t, m, sc, 1, 0, i, i, false)
		if i <= 10 {
			covered = append(covered, k)
		}
	}
	overlap := putNativeEpochFile(t, m, sc, 1, 0, 9, 11, false)
	segs, err := m.Restore(t.Context(), sc, 1)
	if err != nil || len(segs) != 3 {
		t.Fatalf("restore=%d %v", len(segs), err)
	}
	for _, key := range covered {
		if b.read[key] != 0 {
			t.Fatalf("downloaded covered %s", key)
		}
	}
	for _, key := range []string{high, overlap, EpochPrefix(sc, 1) + "0000/000000000000000c-000000000000000c.ltx"} {
		if b.read[key] == 0 {
			t.Fatalf("missing required read %s", key)
		}
	}
}
