package replica

import (
	"cellhive/internal/objectstore"
	"context"
	"fmt"
	"io"
)

// Across managers, only two native merge/proof plans can own input streams.
// Long plans use short ranged requests rather than retaining one connection per
// source. This also bounds background pressure during frequent epoch changes.
var nativeReadJobs = make(chan struct{}, 2)

func acquireNativeRead(ctx context.Context) (func(), error) {
	select {
	case nativeReadJobs <- struct{}{}:
		return func() { <-nativeReadJobs }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type nativeRangeStream struct {
	ctx          context.Context
	objects      *objectstore.Objects
	key, tag     string
	size, offset int64
	buffer       []byte
}

func (r *nativeRangeStream) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.offset == r.size {
		return 0, io.EOF
	}
	if len(r.buffer) == 0 {
		n := min(int64(64<<10), r.size-r.offset)
		if r.offset == 0 {
			n = min(int64(100), n)
		}
		raw, tag, err := r.objects.RangedGet(r.ctx, r.key, r.offset, n)
		if err != nil {
			return 0, err
		}
		if tag != r.tag || int64(len(raw)) != n {
			return 0, fmt.Errorf("replica: changed or short native stream %s", r.key)
		}
		r.buffer = raw
	}
	n := copy(p, r.buffer)
	r.buffer = r.buffer[n:]
	r.offset += int64(n)
	if len(r.buffer) == 0 {
		r.buffer = nil
	}
	return n, nil
}
func openNativeRanges(ctx context.Context, objects *objectstore.Objects, key string) (io.ReadCloser, int64, string, error) {
	size, tag, err := objects.Stat(ctx, key)
	if err != nil {
		return nil, 0, "", err
	}
	if size < 100 || tag == "" {
		return nil, 0, "", fmt.Errorf("replica: missing native stream size/version")
	}
	// At most 64KiB per active input, rather than one 1MiB buffer per file.
	r := &nativeRangeStream{ctx: ctx, objects: objects, key: key, tag: tag, size: size}
	return io.NopCloser(r), size, tag, nil
}
