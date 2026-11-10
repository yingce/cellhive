package compaction

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"cellhive/internal/bucket"
)

type trackedStream struct {
	io.ReadCloser
	active *atomic.Int64
}

func (s *trackedStream) Close() error { s.active.Add(-1); return s.ReadCloser.Close() }

type streamingCompactionBucket struct {
	*bucket.FSBucket
	active  atomic.Int64
	uploads atomic.Int64
}

func (b *streamingCompactionBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasSuffix(key, ".ltx") {
		return nil, "", errors.New("buffered LTX read")
	}
	return b.FSBucket.Get(ctx, key)
}
func (b *streamingCompactionBucket) ConditionalCreate(ctx context.Context, key string, raw []byte) (string, error) {
	if strings.HasSuffix(key, ".ltx") {
		return "", errors.New("buffered LTX write")
	}
	return b.FSBucket.ConditionalCreate(ctx, key, raw)
}
func (b *streamingCompactionBucket) OpenReader(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	r, size, tag, err := b.FSBucket.OpenReader(ctx, key)
	if err != nil {
		return nil, 0, "", err
	}
	b.active.Add(1)
	return &trackedStream{r, &b.active}, size, tag, nil
}
func (b *streamingCompactionBucket) PutReader(ctx context.Context, key string, r io.ReadSeeker, size int64) (string, error) {
	b.uploads.Add(1)
	return b.FSBucket.PutReader(ctx, key, r, size)
}

func TestCompactionStreamsAllLevelsAndCleansSpools(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	m, s := nativeManager(t)
	appendNative(t, m, s, 0, 1, 1, 1, 2)
	appendNative(t, m, s, 0, 2, 2, 2)
	b := &streamingCompactionBucket{FSBucket: m.B.(*bucket.FSBucket)}
	m.B = b
	for _, level := range []uint16{1, 2, 3, 9} {
		var err error
		if level == 9 {
			_, err = Snapshot(t.Context(), m, s, 1, Options{})
		} else {
			_, err = CompactLevel(t.Context(), m, s, 1, level, Options{})
		}
		if err != nil {
			t.Fatalf("level %d: %v", level, err)
		}
		if b.active.Load() != 0 {
			t.Fatal("input streams leaked")
		}
	}
	if b.uploads.Load() != 4 {
		t.Fatalf("uploads=%d", b.uploads.Load())
	}
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spools leaked: %v", entries)
	}
}

func TestStreamingCompactionRejectsCorruptionBeforePublication(t *testing.T) {
	for _, corruption := range []string{"header", "trailer"} {
		t.Run(corruption, func(t *testing.T) {
			t.Setenv("TMPDIR", t.TempDir())
			m, s := nativeManager(t)
			appendNative(t, m, s, 0, 1, 1, 1, 2)
			key := appendNative(t, m, s, 0, 2, 2, 2)
			fs := m.B.(*bucket.FSBucket)
			raw, _, err := fs.Get(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}
			if corruption == "header" {
				raw[0] ^= 1
			} else {
				raw[len(raw)-1] ^= 1
			}
			if err := fs.Delete(t.Context(), key); err != nil {
				t.Fatal(err)
			}
			if _, err := fs.ConditionalCreate(t.Context(), key, raw); err != nil {
				t.Fatal(err)
			}
			b := &streamingCompactionBucket{FSBucket: fs}
			m.B = b
			if _, err := Compact(t.Context(), m, s, 1, Options{}); err == nil {
				t.Fatal("accepted corruption")
			}
			if b.uploads.Load() != 0 || b.active.Load() != 0 {
				t.Fatalf("uploads=%d active=%d", b.uploads.Load(), b.active.Load())
			}
			entries, err := os.ReadDir(os.TempDir())
			if err != nil || len(entries) != 0 {
				t.Fatalf("spools=%v err=%v", entries, err)
			}
		})
	}
}
