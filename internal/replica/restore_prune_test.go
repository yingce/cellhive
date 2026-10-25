package replica

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
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
func TestRestoreDoesNotDownloadCompactedL0(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &restoreReadBucket{Bucket: base, read: map[string]int{}}
	m := New(b)
	sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: "prune"}
	ctx := t.Context()
	snap := ltx.Encode(ltx.Header{Kind: ltx.KindSnapshot, Epoch: 1, StartTxID: 10, EndTxID: 10}, []byte("snapshot"))
	k, _, err := m.AppendL1(ctx, sc, 1, snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.PutManifest(ctx, sc, 1, Manifest{MinTxID: 1, MaxTxID: 10, Objects: []string{k}}); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 12; i++ {
		putSeg(t, base, sc, 1, fmt.Sprintf("%d-%d.ltx", i, i), ltx.KindDelta, i, i)
	}
	// Keep an overlap to let restore validate it; never trim its newer tail.
	putSeg(t, base, sc, 1, "9-11.d.batch", ltx.KindDelta, 9, 11)
	// Legacy/mixed batches and epoch markers cannot be safely excluded by name.
	putSeg(t, base, sc, 1, "1-5.m.batch", ltx.KindDelta, 1, 5)
	if _, err := m.MarkEpoch(ctx, sc, 1, 5); err != nil {
		t.Fatal(err)
	}
	segs, err := m.Restore(ctx, sc, 1)
	if err != nil {
		t.Fatal(err)
	}
	for key, count := range b.read {
		name := strings.TrimPrefix(key, Prefix(sc, 1))
		sp, known := parseSegmentSpan(name)
		if known && !sp.marker && strings.HasSuffix(name, ".ltx") && sp.end <= 10 && count != 0 {
			t.Errorf("downloaded compacted object %s", key)
		}
	}
	for _, name := range []string{"9-11.d.batch", "11-11.ltx", "12-12.ltx", "1-5.m.batch", "6.marker"} {
		if b.read[Prefix(sc, 1)+name] == 0 {
			t.Errorf("required object not read: %s", name)
		}
	}
	if len(segs) == 0 {
		t.Fatal("lost restore chain")
	}
}
