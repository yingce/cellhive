package replica

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"context"
	"errors"
	"testing"
	"time"
)

type retiredReadBucket struct {
	bucket.Bucket
	reads int
	fail  bool
}

func (b *retiredReadBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.reads++
	if b.fail {
		return nil, "", errors.New("read failed")
	}
	return b.Bucket.Get(ctx, key)
}
func TestNativeRetiredCacheTTLMonotonicAndExactScope(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &retiredReadBucket{Bucket: fs}
	m := New(b)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "retired"}
	for i := 0; i < 2; i++ {
		if yes, err := m.EpochRetired(t.Context(), sc, 1); err != nil || yes {
			t.Fatalf("absent: %v %v", yes, err)
		}
	}
	if b.reads != 1 {
		t.Fatalf("absent reads=%d", b.reads)
	}
	if _, err := fs.Put(t.Context(), retiredMarkKey(sc), []byte(`{"retired_below":3}`)); err != nil {
		t.Fatal(err)
	}
	m.retiredMu.Lock()
	e := m.retiredCache[sc.String()]
	e.at = time.Now().Add(-31 * time.Second)
	m.retiredCache[sc.String()] = e
	m.retiredMu.Unlock()
	for i := 0; i < 2; i++ {
		if yes, err := m.EpochRetired(t.Context(), sc, 2); err != nil || !yes {
			t.Fatalf("retired: %v %v", yes, err)
		}
	}
	if b.reads != 2 {
		t.Fatalf("present reads=%d", b.reads)
	}
	lower := uint64(2)
	m.rememberRetiredMark(sc, &lower, time.Now())
	if yes, err := m.EpochRetired(t.Context(), sc, 2); err != nil || !yes {
		t.Fatalf("mark regressed: %v %v", yes, err)
	}
	child := sc
	child.ID += "-child"
	if yes, err := m.EpochRetired(t.Context(), child, 1); err != nil || yes {
		t.Fatalf("mark leaked scope: %v %v", yes, err)
	}
	if yes, err := m.Covers(t.Context(), sc, 1, ^uint64(0)); err != nil || !yes {
		t.Fatalf("retired coverage: %v %v", yes, err)
	}
	m.retiredMu.Lock()
	e = m.retiredCache[sc.String()]
	e.at = time.Now().Add(-6 * time.Minute)
	m.retiredCache[sc.String()] = e
	m.retiredMu.Unlock()
	b.fail = true
	if _, err := m.EpochRetired(t.Context(), sc, 1); err == nil {
		t.Fatal("read failure treated as coverage")
	}
	b.fail = false
	if yes, err := m.EpochRetired(t.Context(), sc, 1); err != nil || !yes {
		t.Fatalf("failed read cached: %v %v", yes, err)
	}
}
