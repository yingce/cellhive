package replica

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/objectstore"
	"cellhive/internal/restore"
)

// NativeFileRef is a range selected from canonical LTX object names.
type NativeFileRef struct {
	Key              string
	MinTXID, MaxTXID uint64
}

// OpenNativeFiles validates selected headers and returns ordered streams. Full
// payload integrity is checked by the native decoder/compactor consuming them.
// Call close even when compaction fails; no body is retained here.
func (m *Manager) OpenNativeFiles(ctx context.Context, refs []NativeFileRef) (readers []io.Reader, size int64, cleanup func(), err error) {
	release, err := acquireNativeRead(ctx)
	if err != nil {
		return nil, 0, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	objects := objectstore.NewOwned(m.B, objectstore.PrefixCells, objectstore.OwnerReplica)
	readers = make([]io.Reader, len(refs))
	handles := make([]io.ReadCloser, len(refs))
	sizes := make([]int64, len(refs))
	errs := make([]error, len(refs))
	var once sync.Once
	cleanup = func() {
		once.Do(func() {
			cancel()
			for _, h := range handles {
				if h != nil {
					h.Close()
				}
			}
			release()
		})
	}
	// Bound concurrent requests; open bodies remain live until consumed/closed.
	jobs := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < min(16, len(refs)); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				ref := refs[i]
				var r io.ReadCloser
				var n int64
				var e error
				var hit bool
				r, n, _, hit, e = m.openCachedNative(ctx, ref.Key)
				if e != nil {
					errs[i] = e
					continue
				}
				if hit {
					r = &bufferedNativeCloser{Reader: bufio.NewReaderSize(r, 64<<10), Closer: r}
				} else if len(refs) > 16 {
					r, n, _, e = openNativeRanges(ctx, objects, ref.Key)
				} else {
					r, n, _, e = objects.OpenReader(ctx, ref.Key)
				}
				if e != nil {
					errs[i] = e
					continue
				}
				handles[i], sizes[i] = r, n
				var header [ltx.NativeHeaderSize]byte
				if _, e = io.ReadFull(r, header[:]); e != nil {
					errs[i] = e
					continue
				}
				h, e := ltx.ParseNativeHeader(header[:])
				if e == nil && (h.MinTXID != ref.MinTXID || h.MaxTXID != ref.MaxTXID) {
					e = fmt.Errorf("replica: source range/header mismatch: %s", ref.Key)
				}
				if e != nil {
					errs[i] = e
					continue
				}
				readers[i] = io.MultiReader(bytes.NewReader(header[:]), r)
			}
		}()
	}
	for i := range refs {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			cleanup()
			return nil, 0, nil, e
		}
		size += sizes[i]
	}
	return readers, size, cleanup, nil
}

func (m *Manager) OpenRestoreChain(ctx context.Context, s cell.Scope, epoch uint64) ([]io.Reader, int64, func(), error) {
	plan, err := m.nativeRestorePlan(ctx, s, epoch)
	if err != nil {
		return nil, 0, nil, err
	}
	refs := make([]NativeFileRef, len(plan))
	for i, f := range plan {
		refs[i] = NativeFileRef{f.key, f.min, f.max}
	}
	return m.OpenNativeFiles(ctx, refs)
}

// ProvenWatermark verifies full native integrity and final page coverage while
// retaining page identities rather than entire input bodies.
func (m *Manager) ProvenWatermark(ctx context.Context, s cell.Scope, epoch uint64) (uint64, error) {
	readers, _, cleanup, err := m.OpenRestoreChain(ctx, s, epoch)
	if err != nil {
		return 0, err
	}
	defer cleanup()
	if len(readers) == 0 {
		return 0, nil
	}
	return restore.ValidateChainReaders(readers)
}

type bufferedNativeCloser struct {
	io.Reader
	io.Closer
}
