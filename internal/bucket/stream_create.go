package bucket

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// StreamCreator atomically publishes a complete immutable object from a stable
// seekable source positioned at zero. The source must remain unchanged until
// return. Implementations must not close the caller-owned source. A failed
// upload must never expose a partial final object.
type StreamPutter interface {
	PutReader(context.Context, string, io.ReadSeeker, int64) (string, error)
}

type StreamCreator interface {
	ConditionalCreateReader(context.Context, string, io.ReadSeeker, int64) (string, error)
}

func (b *S3Bucket) ConditionalCreateReader(ctx context.Context, key string, src io.ReadSeeker, size int64) (string, error) {
	return b.writeReader(ctx, key, src, size, true)
}

func (b *S3Bucket) PutReader(ctx context.Context, key string, src io.ReadSeeker, size int64) (string, error) {
	return b.writeReader(ctx, key, src, size, false)
}

func (b *S3Bucket) writeReader(ctx context.Context, key string, src io.ReadSeeker, size int64, create bool) (string, error) {
	if size < 0 {
		return "", fmt.Errorf("bucket: negative object size")
	}
	timestamp, err := ltxTimestamp(key, src, size)
	if err != nil {
		return "", err
	}
	var metadata map[string]string
	if timestamp != "" {
		metadata = map[string]string{ltxTimestampMetadataKey: timestamp}
	}
	if create {
		b.creates.Add(1)
	} else {
		b.puts.Add(1)
	}
	var condition *string
	if create {
		condition = aws.String("*")
	}
	out, err := b.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &b.bucket, Key: &key, Body: borrowedReadSeeker{src}, ContentLength: aws.Int64(size), IfNoneMatch: condition, Metadata: metadata})
	if err != nil {
		if isPrecondition(err) {
			return "", ErrPrecondition
		}
		return "", err
	}
	return unquote(aws.ToString(out.ETag)), nil
}

// ProviderBucket must not inherit S3 conditional semantics: COS/OSS native
// forbid-overwrite is required, just as for ordinary immutable assets.
func (b *ProviderBucket) ConditionalCreateReader(ctx context.Context, key string, src io.ReadSeeker, size int64) (string, error) {
	if !(strings.HasPrefix(key, "cells/") && strings.Contains(key, "/ltx/e")) {
		return "", ErrNotSupported
	}
	c, ok := b.appender.(interface {
		CreateObjectReader(context.Context, string, io.ReadSeeker, int64) (string, error)
	})
	if !ok {
		return "", ErrNotSupported
	}
	return c.CreateObjectReader(ctx, key, src, size)
}

func (b *FSBucket) ConditionalCreateReader(ctx context.Context, key string, src io.ReadSeeker, size int64) (string, error) {
	return b.writeReader(ctx, key, src, size, true)
}

func (b *FSBucket) PutReader(ctx context.Context, key string, src io.ReadSeeker, size int64) (string, error) {
	return b.writeReader(ctx, key, src, size, false)
}

func (b *FSBucket) writeReader(ctx context.Context, key string, src io.ReadSeeker, size int64, create bool) (string, error) {
	if size < 0 {
		return "", fmt.Errorf("bucket: negative object size")
	}
	if create {
		b.creates.Add(1)
	} else {
		b.puts.Add(1)
	}
	unlock := b.acquire(key)
	defer unlock()
	p := b.path(key)
	if create {
		if _, err := os.Stat(p); err == nil {
			return "", ErrPrecondition
		} else if !os.IsNotExist(err) {
			return "", err
		}
	}
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(filepath.Dir(p), ".upload-*.tmp")
	if err != nil {
		return "", err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	hash := sha256.New()
	if _, err = io.CopyN(io.MultiWriter(f, hash), contextReader{ctx, src}, size); err != nil {
		return "", err
	}
	var extra [1]byte
	if n, err := src.Read(extra[:]); n != 0 || !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("bucket: source size differs")
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	if err = f.Sync(); err != nil {
		return "", err
	}
	if err = f.Close(); err != nil {
		return "", err
	}
	if create {
		err = os.Link(f.Name(), p)
	} else {
		err = os.Rename(f.Name(), p)
	}
	if err != nil {
		if os.IsExist(err) {
			return "", ErrPrecondition
		}
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// borrowedReadSeeker hides io.Closer from HTTP SDKs. The caller owns the spool
// and needs it after upload for retries and post-publication validation.
type borrowedReadSeeker struct{ io.ReadSeeker }

// PutReader uploads LTX with the provider native ordinary PUT/multipart path.
func (b *ProviderBucket) PutReader(ctx context.Context, key string, src io.ReadSeeker, size int64) (string, error) {
	if !(strings.HasPrefix(key, "cells/") && strings.Contains(key, "/ltx/e")) {
		return "", ErrNotSupported
	}
	p, ok := b.appender.(interface {
		PutObjectReader(context.Context, string, io.ReadSeeker, int64) (string, error)
	})
	if !ok {
		return "", ErrNotSupported
	}
	return p.PutObjectReader(ctx, key, src, size)
}
