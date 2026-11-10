package replica

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// Legacy test labels are descriptive only: every fixture is a native file.
func putSeg(t *testing.T, b bucket.Bucket, sc cell.Scope, epoch uint64, _ string, kind ltx.Kind, start, end uint64) {
	t.Helper()
	if kind == ltx.KindSnapshot {
		start = 1
	}
	if kind != ltx.KindSnapshot && kind != ltx.KindDelta {
		t.Fatal("fixture requires native snapshot/delta")
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: start, MaxTXID: end}, []ltx.WALPage{{PageNo: 1, Data: make([]byte, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := New(b).Append(t.Context(), sc, epoch, raw); err != nil {
		t.Fatal(err)
	}
}

func TestNativeContinuationConcurrentShapeConflict(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "seed-race"}
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = New(fs).AppendContinuation(t.Context(), sc, 2, ltx.NativeHeader{PageSize: 512, Commit: uint32(i + 1), MinTXID: 8, MaxTXID: 8, WALOffset: 32})
		}(i)
	}
	wg.Wait()
	success := 0
	for _, err := range errs {
		if err == nil {
			success++
		} else if !errors.Is(err, bucket.ErrPrecondition) {
			t.Fatalf("conflict=%v", err)
		}
	}
	if success != 1 {
		t.Fatalf("successful different shapes=%d errs=%v", success, errs)
	}
	keys, err := fs.List(t.Context(), sc.Key()+"/")
	if err != nil || len(keys) != 1 || !strings.HasSuffix(keys[0], "0000000000000008-0000000000000008.ltx") {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
}

type continuationLostReply struct {
	bucket.Bucket
	once bool
}

func (b *continuationLostReply) Put(ctx context.Context, key string, raw []byte) (string, error) {
	etag, err := b.Bucket.Put(ctx, key, raw)
	if err == nil && strings.HasSuffix(key, ".ltx") && !b.once {
		b.once = true
		return "", fmt.Errorf("publication reply lost")
	}
	return etag, err
}
func TestNativeContinuationRetryAfterLostPublicationReply(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &continuationLostReply{Bucket: fs}
	m := New(b)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "seed-retry"}
	h := ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 8, MaxTXID: 8, WALOffset: 32, WALSalt1: 3}
	if err := m.AppendContinuation(t.Context(), sc, 2, h); err == nil {
		t.Fatal("lost reply ignored")
	}
	if err := m.CheckContinuation(t.Context(), sc, 2, h); err != nil {
		t.Fatal(err)
	}
	if err := m.AppendContinuation(t.Context(), sc, 2, h); err != nil {
		t.Fatal(err)
	}
	keys, err := fs.List(t.Context(), sc.Key()+"/")
	if err != nil || len(keys) != 1 {
		t.Fatalf("retry objects=%v err=%v", keys, err)
	}
}
