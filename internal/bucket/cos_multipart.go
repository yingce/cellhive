package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

// Parts are service-internal; only Complete publishes the canonical object.
// Keep the single-PUT threshold independent of the tuned upload part size.
const (
	nativeMultipartThresholdBytes = int64(5 << 20)
	nativeMultipartPartBytes      = int64(32 << 20)
	nativeMultipartConcurrency    = 4
)

// Shared across provider instances and byte/file-backed objects. Small single
// PUTs do not queue behind snapshots. Each admitted upload owns its buffers
// until every UploadPart has returned and incomplete parts have been aborted.
var nativeMultipartUploads = make(chan struct{}, 2)

func acquireMultipart(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case nativeMultipartUploads <- struct{}{}:
		return func() { <-nativeMultipartUploads }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Division before addition avoids overflowing for large declared sizes.
func multipartPartBytes(size int64) int64 {
	partBytes := size / 10000
	if size%10000 != 0 {
		partBytes++
	}
	return max(nativeMultipartPartBytes, partBytes)
}

func (c *COSAppender) createMultipart(ctx context.Context, key string, src io.Reader, size int64, metadata http.Header) (etag string, err error) {
	return c.writeMultipart(ctx, key, src, size, metadata, true)
}
func (c *COSAppender) writeMultipart(ctx context.Context, key string, src io.Reader, size int64, metadata http.Header, create bool) (etag string, err error) {
	release, err := acquireMultipart(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	partBytes := multipartPartBytes(size)
	init, _, err := c.client.Object.InitiateMultipartUpload(ctx, key, &cos.InitiateMultipartUploadOptions{ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{XOptionHeader: &metadata}})
	if err != nil {
		return "", nativeCOSError(err)
	}
	if init == nil || init.UploadID == "" {
		return "", fmt.Errorf("bucket: multipart response lacks upload id")
	}
	completed := false
	defer func() {
		if completed {
			return
		}
		// A cancelled caller must still release the provider's incomplete parts.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if _, abortErr := c.client.Object.AbortMultipartUpload(cleanup, key, init.UploadID); abortErr != nil {
			err = errors.Join(err, fmt.Errorf("bucket: abort incomplete upload: %w", nativeCOSError(abortErr)))
		}
	}()
	parts, err := c.uploadMultipartParts(ctx, key, init.UploadID, src, size, partBytes)
	if err != nil {
		return "", err
	}
	headers := http.Header{}
	if create {
		headers.Set("x-cos-forbid-overwrite", "true")
	}
	result, resp, err := c.client.Object.CompleteMultipartUpload(ctx, key, init.UploadID, &cos.CompleteMultipartUploadOptions{Parts: parts, XOptionHeader: &headers})
	if err != nil {
		var svc *cos.ErrorResponse
		if errors.As(err, &svc) && svc.Response != nil && svc.Response.StatusCode == http.StatusConflict && svc.Code == "FileAlreadyExists" {
			return "", ErrPrecondition
		}
		return "", nativeCOSError(err)
	}
	completed = true
	if result != nil {
		etag = strings.Trim(result.ETag, `"`)
	}
	if etag == "" && resp != nil {
		etag = strings.Trim(resp.Header.Get("ETag"), `"`)
	}
	if etag == "" {
		return "", fmt.Errorf("bucket: completed multipart upload lacks etag")
	}
	return etag, nil
}

// One reader pipelines source bytes into bounded reusable buffers. Each upload
// owns its buffer until the SDK returns, including retries. A failed upload is
// fully joined before createMultipart aborts its remote upload ID.
func (c *COSAppender) uploadMultipartParts(ctx context.Context, key, uploadID string, src io.Reader, size, partBytes int64) (parts []cos.Object, err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	count := int(1 + (size-1)/partBytes)
	parts = make([]cos.Object, count)
	// Larger objects use larger parts to stay below 10000 parts. Reduce their
	// parallelism to keep one upload near 128MiB; a single part larger than
	// that budget necessarily needs one part-sized buffer.
	parallel := min(nativeMultipartConcurrency, count, max(1, int((4*nativeMultipartPartBytes)/partBytes)))
	buffers := make(chan []byte, parallel)
	for i := 0; i < cap(buffers); i++ {
		buffers <- make([]byte, min(partBytes, size))
	}
	var workers sync.WaitGroup
	defer func() {
		if err != nil {
			cancel(err)
		}
		workers.Wait()
		if cause := context.Cause(ctx); cause != nil {
			err = cause
		}
	}()
	for i, off := 0, int64(0); off < size; i++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		var buf []byte
		select {
		case buf = <-buffers:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
		n := min(partBytes, size-off)
		if _, err = io.ReadFull(src, buf[:n]); err != nil {
			return nil, err
		}
		// Keep the slice independent of the named result: error returns may set
		// parts=nil while outstanding workers finish writing their own slots.
		result := parts[i : i+1]
		workers.Add(1)
		go func(number int, data []byte, result []cos.Object) {
			defer workers.Done()
			defer func() { buffers <- data[:cap(data)] }()
			resp, uploadErr := c.client.Object.UploadPart(ctx, key, uploadID, number, bytes.NewReader(data), &cos.ObjectUploadPartOptions{ContentLength: int64(len(data))})
			if uploadErr != nil {
				cancel(nativeCOSError(uploadErr))
				return
			}
			if resp == nil || resp.Header.Get("ETag") == "" {
				cancel(fmt.Errorf("bucket: multipart part lacks etag"))
				return
			}
			result[0] = cos.Object{PartNumber: number, ETag: resp.Header.Get("ETag")}
		}(i+1, buf[:n], result)
		off += n
	}
	var extra [1]byte
	if n, e := src.Read(extra[:]); n != 0 || !errors.Is(e, io.EOF) {
		return nil, fmt.Errorf("bucket: multipart source size differs")
	}
	return parts, nil
}
