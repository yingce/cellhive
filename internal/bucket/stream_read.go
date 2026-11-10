package bucket

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// StreamReader opens one immutable object version. The caller must close it.
// Unlike Get, this API does not allocate the entire object or impose a body cap.
type StreamReader interface {
	OpenReader(context.Context, string) (io.ReadCloser, int64, string, error)
}

func (b *S3Bucket) OpenReader(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	b.gets.Add(1)
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &b.bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			err = ErrNotFound
		}
		return nil, 0, "", err
	}
	return out.Body, aws.ToInt64(out.ContentLength), unquote(aws.ToString(out.ETag)), nil
}

func (b *ProviderBucket) OpenReader(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	if b.framedAsset(key) {
		return b.openAsset(ctx, key)
	}
	// Never bypass segmented coordination-record semantics.
	if authorityKey(key) || pointFenceKey(key) {
		return nil, 0, "", ErrNotSupported
	}
	return b.S3Bucket.OpenReader(ctx, key)
}

type contextReadCloser struct {
	io.Reader
	io.Closer
}

func (b *FSBucket) OpenReader(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	b.gets.Add(1)
	f, err := os.Open(b.path(key))
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotFound
		}
		return nil, 0, "", err
	}
	// Hash and serve the same open inode, even if the path is later replaced.
	h := sha256.New()
	n, err := io.Copy(h, contextReader{ctx, f})
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		f.Close()
		return nil, 0, "", err
	}
	return contextReadCloser{contextReader{ctx, f}, f}, n, hex.EncodeToString(h.Sum(nil)), nil
}
