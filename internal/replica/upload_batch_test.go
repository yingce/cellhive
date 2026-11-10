package replica

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

type failUploadCreateOnce struct {
	*bucket.FSBucket
	failed bool
}

func (b *failUploadCreateOnce) Put(ctx context.Context, key string, data []byte) (string, error) {
	if strings.HasSuffix(key, "0000000000000004-0000000000000005.ltx") && !b.failed {
		b.failed = true
		return "", errors.New("injected publication failure")
	}
	return b.FSBucket.Put(ctx, key, data)
}
func TestNativeUploadBatchRetryPreservesRangeIdentity(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(&failUploadCreateOnce{FSBucket: fs})
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "upload-retry"}
	inputs := [][]byte{nativeBlob(t, 5, 5, 5), nativeBlob(t, 1, 1, 1), nativeBlob(t, 4, 4, 4), nativeBlob(t, 2, 2, 2)}
	if err = m.UploadBatch(t.Context(), sc, 1, inputs); err == nil {
		t.Fatal("failure hidden")
	}
	if err = m.UploadBatch(t.Context(), sc, 1, inputs); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if err = m.UploadBatch(t.Context(), sc, 1, inputs); err != nil {
		t.Fatalf("repeat: %v", err)
	}
	keys, err := m.ListLevel(t.Context(), sc, 1, 0)
	if err != nil || len(keys) != 2 {
		t.Fatalf("range retry created extra objects: %v %v", keys, err)
	}
	if _, err = m.RestoreChain(t.Context(), sc, 1); err == nil {
		t.Fatal("missing TXID3 concealed")
	}
	if err = m.UploadBatch(t.Context(), sc, 1, [][]byte{nativeBlob(t, 3, 3, 3)}); err != nil {
		t.Fatal(err)
	}
	if wm, err := m.DurableWatermarkChain(t.Context(), sc, 1); err != nil || wm != 5 {
		t.Fatalf("watermark=%d err=%v", wm, err)
	}
	if err = m.UploadBatch(t.Context(), sc, 1, [][]byte{nativeBlob(t, 6, 6, 6), nativeBlob(t, 6, 6, 7)}); !errors.Is(err, bucket.ErrPrecondition) {
		t.Fatalf("conflict hidden: %v", err)
	}
}
