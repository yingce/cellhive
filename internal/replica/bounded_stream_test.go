package replica

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
)

type rangeOnlyProofBucket struct {
	*bucket.FSBucket
	changed  bool
	requests atomic.Int64
	largest  atomic.Int64
}

func (b *rangeOnlyProofBucket) OpenReader(context.Context, string) (io.ReadCloser, int64, string, error) {
	return nil, 0, "", errors.New("long plans must not hold remote streams")
}
func (b *rangeOnlyProofBucket) RangedGet(ctx context.Context, k string, o, n int64) ([]byte, string, error) {
	b.requests.Add(1)
	for old := b.largest.Load(); n > old; old = b.largest.Load() {
		if b.largest.CompareAndSwap(old, n) {
			break
		}
	}
	raw, tag, err := b.FSBucket.RangedGet(ctx, k, o, n)
	if b.changed {
		tag = "changed"
	}
	return raw, tag, err
}
func TestLongNativePlanUsesBoundedVersionedRanges(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &rangeOnlyProofBucket{FSBucket: fs}
	m := New(b)
	s := cell.Scope{Namespace: "n", Class: "c", ID: "range"}
	raw := nativeBlob(t, 1, 5, 7)
	key := EpochPrefix(s, 1) + "0000/0000000000000001-0000000000000005.ltx"
	if _, err := fs.ConditionalCreate(t.Context(), key, raw); err != nil {
		t.Fatal(err)
	}
	refs := make([]NativeFileRef, 17)
	for i := range refs {
		refs[i] = NativeFileRef{Key: key, MinTXID: 1, MaxTXID: 5}
	}
	readers, _, close, err := m.OpenNativeFiles(t.Context(), refs)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range readers {
		d, err := ltx.NewNativeDecoder(r)
		if err != nil {
			t.Fatal(err)
		}
		for {
			_, err = d.ReadPage()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	close()
	close()
	if b.largest.Load() > 64<<10 {
		t.Fatalf("unbounded range %d", b.largest.Load())
	}
	b.changed = true
	if _, _, cleanup, err := m.OpenNativeFiles(t.Context(), refs); err == nil {
		cleanup()
		t.Fatal("changed ETag accepted")
	}
}
func TestNativeReadBudgetCancellation(t *testing.T) {
	a, err := acquireNativeRead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer a()
	b, err := acquireNativeRead(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer b()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := acquireNativeRead(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
