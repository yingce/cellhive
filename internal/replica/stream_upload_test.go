package replica

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

type streamOnlyBucket struct {
	*bucket.FSBucket
	uploads  int
	maxRange int64
}

func (b *streamOnlyBucket) ConditionalCreate(ctx context.Context, key string, raw []byte) (string, error) {
	if strings.HasSuffix(key, ".ltx") {
		return "", errors.New("buffered LTX publication")
	}
	return b.FSBucket.ConditionalCreate(ctx, key, raw)
}
func (b *streamOnlyBucket) PutReader(ctx context.Context, key string, r io.ReadSeeker, n int64) (string, error) {
	b.uploads++
	return b.FSBucket.PutReader(ctx, key, r, n)
}
func (b *streamOnlyBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasSuffix(key, ".ltx") {
		return nil, "", errors.New("full LTX GET")
	}
	return b.FSBucket.Get(ctx, key)
}
func (b *streamOnlyBucket) RangedGet(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if n > b.maxRange {
		b.maxRange = n
	}
	return b.FSBucket.RangedGet(ctx, key, off, n)
}
func TestSnapshotFileStreamsAndRetriesWithoutFullRead(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &streamOnlyBucket{FSBucket: fs}
	m := New(b)
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "stream"}
	write := func(value byte) string {
		p := filepath.Join(t.TempDir(), "snapshot.ltx")
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		enc, err := ltx.NewNativeEncoder(f, ltx.NativeHeader{PageSize: 4096, Commit: 600, MinTXID: 1, MaxTXID: 40})
		if err != nil {
			t.Fatal(err)
		}
		// Incompressible pages exercise multiple bounded retry ranges.
		var x uint32 = 1234 + uint32(value)
		page := make([]byte, 4096)
		for pg := uint32(1); pg <= 600; pg++ {
			for i := range page {
				x ^= x << 13
				x ^= x >> 17
				x ^= x << 5
				page[i] = byte(x)
			}
			if err := enc.WritePage(ltx.WALPage{PageNo: pg, Data: page}); err != nil {
				t.Fatal(err)
			}
		}
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		return p
	}
	p := write(1)
	info, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	key, tag, err := m.AppendSnapshotFile(t.Context(), sc, 1, p, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	retryKey, retryTag, err := m.AppendSnapshotFile(t.Context(), sc, 1, p, info.Size())
	if err != nil || key != retryKey || tag != retryTag {
		t.Fatalf("retry=%s %s %v", retryKey, retryTag, err)
	}
	if b.uploads != 2 || b.maxRange != 0 {
		t.Fatalf("uploads=%d range=%d", b.uploads, b.maxRange)
	}
	different := write(2)
	differentInfo, _ := os.Stat(different)
	if _, _, err := m.AppendSnapshotFile(t.Context(), sc, 1, different, differentInfo.Size()); err != nil {
		t.Fatalf("conflict=%v", err)
	}
	want, _ := os.ReadFile(different)
	got, _, err := fs.Get(t.Context(), key)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("overwrite bytes mismatch")
	}
	if _, _, err := m.AppendSnapshotFile(t.Context(), sc, 1, p, info.Size()-1); err == nil {
		t.Fatal("accepted wrong size")
	}
	corrupt := append([]byte(nil), want...)
	corrupt[len(corrupt)-1] ^= 1
	bad := filepath.Join(t.TempDir(), "bad.ltx")
	if err := os.WriteFile(bad, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	before := b.uploads
	if _, _, err := m.AppendSnapshotFile(t.Context(), sc, 1, bad, int64(len(corrupt))); err == nil {
		t.Fatal("accepted corrupt spool")
	}
	if b.uploads != before {
		t.Fatal("corrupt spool uploaded before validation")
	}
}
