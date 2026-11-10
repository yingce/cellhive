package compaction

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"cellhive/internal/bucket"
	"cellhive/internal/objectstore"
	"cellhive/internal/replica"
)

const (
	DefaultMaxInputs              = 256
	DefaultMaxInputBytes    int64 = 64 << 20
	DefaultSnapshotMaxBytes int64 = 80 << 20
)

// One active merge per process bounds aggregate decoder buffers and spool I/O
// across scheduler, admin and ownership-release callers.
var jobSlot = make(chan struct{}, 1)

func acquireJob(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case jobSlot <- struct{}{}:
		return func() { <-jobSlot }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func inputLimits(opts Options) (int, int64) {
	n, size := opts.MaxInputs, opts.MaxInputBytes
	if n <= 0 {
		n = DefaultMaxInputs
	}
	if size <= 0 {
		size = DefaultMaxInputBytes
	}
	return n, size
}

// Select a contiguous prefix before opening any streams. A single oversized
// object still makes progress. Without size metadata, conservatively select
// one object instead of downloading a body merely to discover its size.
func boundInputs(ctx context.Context, m *replica.Manager, files []nativeRange, opts Options) ([]nativeRange, error) {
	if len(files) <= 1 {
		return files, nil
	}
	n, limit := inputLimits(opts)
	if len(files) > n {
		files = files[:n]
	}
	objects := objectstore.NewOwned(m.B, objectstore.PrefixCells, objectstore.OwnerReplica)
	var total int64
	for i, f := range files {
		size, _, err := objects.Stat(ctx, f.key)
		if errors.Is(err, bucket.ErrNotSupported) {
			return files[:1], nil
		}
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, fmt.Errorf("compaction: negative source size")
		}
		if i > 0 && size > limit-total {
			return files[:i], nil
		}
		if size >= limit-total {
			return files[:i+1], nil
		}
		total += size
	}
	return files, nil
}

// Serialize merges and reserve filesystem headroom before creating any output.
// This is admission control, not permission to publish a partial output when
// another process consumes disk later: normal write/fsync errors still abort.
func checkSpoolSpace(inputBytes int64) error {
	var st syscall.Statfs_t
	if err := syscall.Statfs(os.TempDir(), &st); err != nil {
		return err
	}
	if inputBytes < 0 {
		return fmt.Errorf("compaction: invalid input size")
	}
	available := uint64(st.Bavail) * uint64(st.Bsize)
	needed := uint64(inputBytes) + uint64(inputBytes)/8 + (512 << 20)
	if available < needed {
		return fmt.Errorf("compaction: insufficient temporary disk: available %d need %d", available, needed)
	}
	return nil
}
