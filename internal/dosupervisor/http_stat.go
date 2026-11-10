package dosupervisor

import (
	"bufio"
	"cellhive/internal/cell"
	"cellhive/internal/replica"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"cellhive/internal/bearertoken"
	"cellhive/internal/bucket"
)

// Stat gives credential-free paged restoration size and immutable version
// without downloading the LTX body.
func (b *httpBucket) Stat(ctx context.Context, key string) (int64, string, error) {
	endpoint := strings.TrimRight(b.store.Base, "/") + "/v1/internal/segment?key=" + url.QueryEscape(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, endpoint, nil)
	if err != nil {
		return 0, "", err
	}
	if err := bearertoken.SetInternal(req, b.store.Token, "internal"); err != nil {
		return 0, "", err
	}
	resp, err := b.store.client().Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, "", bucket.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return 0, "", fmt.Errorf("dosupervisor: stat segment: status %d", resp.StatusCode)
	}
	tag := strings.Trim(resp.Header.Get("ETag"), `"`)
	if resp.ContentLength < 0 || tag == "" {
		return 0, "", fmt.Errorf("dosupervisor: stat segment lacks size or ETag")
	}
	return resp.ContentLength, tag, nil
}

func (h *HTTPStore) ProvenWatermark(ctx context.Context, s cell.Scope, e uint64) (uint64, error) {
	return replica.New(h.bucket(s, e)).ProvenWatermark(ctx, s, e)
}

// Stream over bounded ranges: the agent never buffers a full object either.
func (b *httpBucket) OpenReader(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	size, tag, err := b.Stat(ctx, key)
	if err != nil {
		return nil, 0, "", err
	}
	ctx, cancel := context.WithCancel(ctx)
	source := &httpObjectRanges{ctx: ctx, bucket: b, key: key, tag: tag, size: size}
	return &httpObjectStream{Reader: bufio.NewReaderSize(source, 1<<20), cancel: cancel}, size, tag, nil
}

type httpObjectStream struct {
	io.Reader
	cancel context.CancelFunc
}

func (r *httpObjectStream) Close() error { r.cancel(); return nil }

type httpObjectRanges struct {
	ctx          context.Context
	bucket       *httpBucket
	key, tag     string
	size, offset int64
}

func (r *httpObjectRanges) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.offset == r.size {
		return 0, io.EOF
	}
	n := min(int64(len(p)), r.size-r.offset, 1<<20)
	raw, tag, err := r.bucket.RangedGet(r.ctx, r.key, r.offset, n)
	if err != nil {
		return 0, err
	}
	if tag != r.tag || int64(len(raw)) != n {
		return 0, fmt.Errorf("dosupervisor: immutable stream version or size changed")
	}
	copy(p, raw)
	r.offset += n
	return int(n), nil
}
