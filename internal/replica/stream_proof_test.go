package replica

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

type proofStreamBucket struct {
	*bucket.FSBucket
	active atomic.Int64
}

func (b *proofStreamBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasSuffix(key, ".ltx") {
		return nil, "", errors.New("full LTX read forbidden")
	}
	return b.FSBucket.Get(ctx, key)
}

type proofReadCloser struct {
	io.ReadCloser
	active *atomic.Int64
}

func (r *proofReadCloser) Close() error { r.active.Add(-1); return r.ReadCloser.Close() }
func (b *proofStreamBucket) OpenReader(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	r, n, tag, err := b.FSBucket.OpenReader(ctx, key)
	if err != nil {
		return nil, 0, "", err
	}
	b.active.Add(1)
	return &proofReadCloser{r, &b.active}, n, tag, nil
}
func TestCompleteSnapshotProofStreamsAndClosesOnCorruption(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid", true: "corrupt"}[corrupt], func(t *testing.T) {
			fs, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			b := &proofStreamBucket{FSBucket: fs}
			m := New(b)
			sc := cell.Scope{Namespace: "n", Class: "c", ID: "proof"}
			raw := nativeBlob(t, 1, 5, 7)
			if corrupt {
				raw[len(raw)-1] ^= 1
			}
			key := EpochPrefix(sc, 1) + "0009/0000000000000001-0000000000000005.ltx"
			if _, err := fs.ConditionalCreate(t.Context(), key, raw); err != nil {
				t.Fatal(err)
			}
			ok, err := m.HasCompleteSnapshot(t.Context(), sc, 1)
			if corrupt {
				if ok || err == nil {
					t.Fatal("corrupt snapshot accepted")
				}
			} else if err != nil || !ok {
				t.Fatalf("proof=%v err=%v", ok, err)
			}
			if b.active.Load() != 0 {
				t.Fatal("proof leaked input stream")
			}
		})
	}
}
