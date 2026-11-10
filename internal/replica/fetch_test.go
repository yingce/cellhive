package replica

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

type fetchCountingBucket struct {
	bucket.Bucket
	mu      sync.Mutex
	objects map[string][]byte
	gets    map[string]int
}

func (b *fetchCountingBucket) Get(_ context.Context, key string) ([]byte, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gets[key]++
	data, ok := b.objects[key]
	if !ok {
		return nil, "", bucket.ErrNotFound
	}
	return data, "test", nil
}

func (b *fetchCountingBucket) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	for key := range b.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func TestNativePageFetcherSizeThreshold(t *testing.T) {
	for _, threshold := range []int64{511, 512, 513, 1 << 20} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			fs, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			cb := &countingBucket{Bucket: fs}
			m := New(cb)
			sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "threshold"}
			if _, _, err = m.AppendL1(t.Context(), sc, 1, nativeBlob(t, 1, 10, 7)); err != nil {
				t.Fatal(err)
			}
			cb.mu.Lock()
			cb.gets, cb.rangedGets, cb.getBytes, cb.rangedBytes = 0, 0, 0, 0
			cb.mu.Unlock()
			pf, use, err := m.NewPageFetcherMinBytes(t.Context(), sc, 1, threshold)
			if err != nil {
				t.Fatal(err)
			}
			if use != (threshold <= 512) || (pf != nil) != use {
				t.Fatalf("threshold %d: use=%v pf=%v", threshold, use, pf)
			}
			_, ranges, _, rangeBytes := cb.stats()
			if !use && (ranges != 1 || rangeBytes != ltx.NativeHeaderSize) {
				t.Fatalf("small image fetched index/payload: ranges=%d bytes=%d", ranges, rangeBytes)
			}
			if use {
				defer pf.Close()
				page, err := pf.Page(t.Context(), 1)
				if err != nil || !bytes.Equal(page, bytes.Repeat([]byte{7}, 512)) {
					t.Fatalf("page: %v", err)
				}
			}
		})
	}
}

func TestPageFetcherMinBytesNoOverflow(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "large-size"}
	if _, _, err := m.Append(t.Context(), sc, 1, nativeBlob(t, 1, 1, 1)); err != nil {
		t.Fatal(err)
	}
	// A 2 TiB declared image must not wrap to zero and bypass index validation.
	// The missing extended pages make this chain invalid, so it must fail closed.
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1<<32 - 1, MinTXID: 2, MaxTXID: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.Append(t.Context(), sc, 1, raw); err != nil {
		t.Fatal(err)
	}
	pf, use, err := m.NewPageFetcherMinBytes(t.Context(), sc, 1, 1<<20)
	if err == nil || pf != nil || use {
		t.Fatalf("incomplete huge image accepted or skipped: pf=%v use=%v err=%v", pf, use, err)
	}
}

func TestPageFetcherMinBytesRequiresIndex(t *testing.T) {
	m := New(&fetchCountingBucket{objects: map[string][]byte{}, gets: map[string]int{}})
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "runs"}
	pf, use, err := m.NewPageFetcherMinBytes(t.Context(), sc, 1, 1)
	if !errors.Is(err, ErrNoPageIndex) || use || pf != nil {
		t.Fatalf("no index: pf=%v use=%v err=%v", pf, use, err)
	}
}

type failRangedOnce struct {
	*bucket.FSBucket
	mu     sync.Mutex
	failed bool
}

func (b *failRangedOnce) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	b.mu.Lock()
	first := !b.failed
	b.failed = true
	b.mu.Unlock()
	if first {
		return nil, "", bucket.ErrNotFound
	}
	return b.FSBucket.RangedGet(ctx, key, off, length)
}

// Failed activation can retry, but an active fetcher never changes its cut.
func TestPageFetcherActivationRetriesAfterObjectDisappears(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &failRangedOnce{FSBucket: fs}
	m := New(b)
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "ranged-retry"}
	if _, _, err = m.AppendL1(t.Context(), sc, 1, nativeBlob(t, 1, 1, 7)); err != nil {
		t.Fatal(err)
	}
	if pf, err := m.NewPageFetcher(t.Context(), sc, 1); !errors.Is(err, bucket.ErrNotFound) || pf != nil {
		t.Fatalf("missing object activation: pf=%v err=%v", pf, err)
	}
	pf, err := m.NewPageFetcher(t.Context(), sc, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	page, err := pf.Page(t.Context(), 1)
	if err != nil || !bytes.Equal(page, bytes.Repeat([]byte{7}, 512)) {
		t.Fatalf("retry page: %v", err)
	}
}

func TestPageFetcherBatchTruncateReextend(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprint(replace), func(t *testing.T) {
			m := newManager(t)
			m.RestoreParallel = 2
			sc := cell.Scope{Namespace: "n", Class: "c", ID: "batch"}
			for tx := uint64(1); tx <= 4; tx++ {
				commit := uint32(2)
				pages := []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{byte(tx)}, 512)}}
				if tx == 1 || (tx == 4 && replace) {
					pages = append(pages, ltx.WALPage{PageNo: 2, Data: bytes.Repeat([]byte{byte(tx)}, 512)})
				}
				if tx == 2 || tx == 3 {
					commit = 1
				}
				raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: commit, MinTXID: tx, MaxTXID: tx}, pages)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err = m.Append(t.Context(), sc, 1, raw); err != nil {
					t.Fatal(err)
				}
			}
			pf, err := m.NewPageFetcher(t.Context(), sc, 1)
			if !replace {
				if err == nil {
					t.Fatal("resurrected truncated page across batches")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer pf.Close()
			p, err := pf.Page(t.Context(), 2)
			if err != nil || !bytes.Equal(p, bytes.Repeat([]byte{4}, 512)) {
				t.Fatalf("page 2: %v", err)
			}
		})
	}
}

func TestRestoreWorkLimiterCancellation(t *testing.T) {
	releases := make([]func(), cap(restoreWorkSlots))
	for i := range releases {
		var err error
		releases[i], err = acquireRestoreWork(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer releases[i]()
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := New(nil).NewPageFetcher(ctx, cell.Scope{}, 1); done <- err }()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("index cancellation: %v", err)
	}
	if _, err := (&PageFetcher{}).Materialize(ctx, "", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("materialize cancellation: %v", err)
	}
}
