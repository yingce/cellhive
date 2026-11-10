package bucket

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// S3ControlBucket fences logical deletion with conditional PUT. Control roots
// are never physically deleted: their random revisions prevent recreate ABA.
// Immutable data retains its original bytes and streaming paths.
type S3ControlBucket struct{ *S3Bucket }

func NewS3ControlBucket(b *S3Bucket) *S3ControlBucket { return &S3ControlBucket{b} }
func s3ControlKey(k string) bool {
	return authorityKey(k) || strings.HasPrefix(k, "r2meta/") || (strings.HasPrefix(k, "wake/") && !ImmutableWakeHintKey(k))
}

const controlHeader = 21 // magic, deleted flag, random revision
func controlEncode(data []byte, deleted bool) []byte {
	b := make([]byte, controlHeader+len(data))
	copy(b, "CHC1")
	if deleted {
		b[4] = 1
	}
	if _, err := rand.Read(b[5:21]); err != nil {
		panic(err)
	}
	copy(b[21:], data)
	return b
}
func controlDecode(b []byte) ([]byte, bool, error) {
	if len(b) < controlHeader || !bytes.Equal(b[:4], []byte("CHC1")) || b[4] > 1 {
		return nil, false, fmt.Errorf("bucket: incompatible S3 control record")
	}
	if b[4] == 1 && len(b) != controlHeader {
		return nil, false, fmt.Errorf("bucket: malformed S3 control tombstone")
	}
	return b[21:], b[4] == 1, nil
}
func (b *S3ControlBucket) Get(ctx context.Context, k string) ([]byte, string, error) {
	raw, e, err := b.S3Bucket.Get(ctx, k)
	if err != nil || !s3ControlKey(k) {
		return raw, e, err
	}
	v, dead, err := controlDecode(raw)
	if err != nil {
		return nil, "", err
	}
	if dead {
		return nil, "", ErrNotFound
	}
	return v, e, nil
}
func (b *S3ControlBucket) Put(ctx context.Context, k string, v []byte) (string, error) {
	if s3ControlKey(k) {
		if int64(len(v)) > b.maxRead-controlHeader {
			return "", objectTooLarge(k, int64(len(v))+controlHeader, b.maxRead)
		}
		v = controlEncode(v, false)
	}
	return b.S3Bucket.Put(ctx, k, v)
}
func (b *S3ControlBucket) CAS(ctx context.Context, k string, v []byte, e string) (string, error) {
	if s3ControlKey(k) {
		if int64(len(v)) > b.maxRead-controlHeader {
			return "", objectTooLarge(k, int64(len(v))+controlHeader, b.maxRead)
		}
		v = controlEncode(v, false)
	}
	return b.S3Bucket.CAS(ctx, k, v, e)
}
func (b *S3ControlBucket) ConditionalCreate(ctx context.Context, k string, v []byte) (string, error) {
	if !s3ControlKey(k) {
		return b.S3Bucket.ConditionalCreate(ctx, k, v)
	}
	if int64(len(v)) > b.maxRead-controlHeader {
		return "", objectTooLarge(k, int64(len(v))+controlHeader, b.maxRead)
	}
	encoded := controlEncode(v, false)
	e, err := b.S3Bucket.ConditionalCreate(ctx, k, encoded)
	if !errors.Is(err, ErrPrecondition) {
		return e, err
	}
	raw, old, err := b.S3Bucket.Get(ctx, k)
	if err != nil {
		return "", err
	}
	_, dead, err := controlDecode(raw)
	if err != nil {
		return "", err
	}
	if !dead {
		return "", ErrPrecondition
	}
	return b.S3Bucket.CAS(ctx, k, encoded, old)
}
func (b *S3ControlBucket) SupportsConditionalDelete(k string) bool { return s3ControlKey(k) }
func (b *S3ControlBucket) ConditionalDelete(ctx context.Context, k, e string) error {
	if !s3ControlKey(k) {
		return ErrNotSupported
	}
	_, err := b.S3Bucket.CAS(ctx, k, controlEncode(nil, true), e)
	return err
}
func (b *S3ControlBucket) Delete(ctx context.Context, k string) error {
	if !s3ControlKey(k) {
		return b.S3Bucket.Delete(ctx, k)
	}
	_, e, err := b.Get(ctx, k)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return b.ConditionalDelete(ctx, k, e)
}
func (b *S3ControlBucket) filter(ctx context.Context, in []ObjectInfo) ([]ObjectInfo, error) {
	out := make([]ObjectInfo, 0, len(in))
	for _, o := range in {
		if s3ControlKey(o.Key) {
			v, e, err := b.Get(ctx, o.Key)
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			o.Size = int64(len(v))
			o.ETag = e
		}
		out = append(out, o)
	}
	return out, nil
}
func (b *S3ControlBucket) ListSizes(ctx context.Context, p string) ([]ObjectInfo, error) {
	v, err := b.S3Bucket.ListSizes(ctx, p)
	if err != nil {
		return nil, err
	}
	return b.filter(ctx, v)
}
func (b *S3ControlBucket) List(ctx context.Context, p string) ([]string, error) {
	v, err := b.ListSizes(ctx, p)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(v))
	for _, o := range v {
		out = append(out, o.Key)
	}
	return out, nil
}
func (b *S3ControlBucket) ListImmediate(ctx context.Context, p string) ([]string, error) {
	v, err := b.S3Bucket.ListSizes(ctx, p)
	if err != nil {
		return nil, err
	}
	in := v[:0]
	for _, o := range v {
		if !strings.Contains(strings.TrimPrefix(o.Key, p), "/") {
			in = append(in, o)
		}
	}
	v, err = b.filter(ctx, in)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(v))
	for _, o := range v {
		out = append(out, o.Key)
	}
	return out, nil
}
func (b *S3ControlBucket) ListPage(ctx context.Context, p, a string, n int) ([]ObjectInfo, string, error) {
	v, next, err := b.S3Bucket.ListPage(ctx, p, a, n)
	if err != nil {
		return nil, "", err
	}
	v, err = b.filter(ctx, v)
	return v, next, err
}
func (b *S3ControlBucket) Stat(ctx context.Context, k string) (int64, string, error) {
	if !s3ControlKey(k) {
		return b.S3Bucket.Stat(ctx, k)
	}
	v, e, err := b.Get(ctx, k)
	return int64(len(v)), e, err
}

func (b *S3ControlBucket) RangedGet(ctx context.Context, k string, off, n int64) ([]byte, string, error) {
	if !s3ControlKey(k) {
		return b.S3Bucket.RangedGet(ctx, k, off, n)
	}
	v, e, err := b.Get(ctx, k)
	if err != nil {
		return nil, "", err
	}
	if off < 0 || n <= 0 || off >= int64(len(v)) {
		return nil, "", fmt.Errorf("bucket: invalid control range")
	}
	end := int64(len(v))
	if n < end-off {
		end = off + n
	}
	return v[off:end], e, nil
}
func (b *S3ControlBucket) TailGet(ctx context.Context, k string, n int) ([]byte, int64, string, error) {
	if !s3ControlKey(k) {
		return b.S3Bucket.TailGet(ctx, k, n)
	}
	v, e, err := b.Get(ctx, k)
	if err != nil {
		return nil, 0, "", err
	}
	size := int64(len(v))
	if n <= 0 {
		n = segProbeBytes
	}
	if len(v) > n {
		v = v[len(v)-n:]
	}
	return v, size, e, nil
}
func (b *S3ControlBucket) OpenReader(ctx context.Context, k string) (io.ReadCloser, int64, string, error) {
	if !s3ControlKey(k) {
		return b.S3Bucket.OpenReader(ctx, k)
	}
	v, e, err := b.Get(ctx, k)
	if err != nil {
		return nil, 0, "", err
	}
	return io.NopCloser(bytes.NewReader(v)), int64(len(v)), e, nil
}
func (b *S3ControlBucket) ConditionalCreateReader(ctx context.Context, k string, r io.ReadSeeker, n int64) (string, error) {
	if !s3ControlKey(k) {
		return b.S3Bucket.ConditionalCreateReader(ctx, k, r, n)
	}
	if n < 0 || n > b.maxRead-controlHeader {
		return "", objectTooLarge(k, n, b.maxRead-controlHeader)
	}
	v, err := readAllBounded(k, r, n, b.maxRead-controlHeader)
	if err != nil {
		return "", err
	}
	return b.ConditionalCreate(ctx, k, v)
}
func (b *S3ControlBucket) PresignGet(ctx context.Context, k string, ttl time.Duration) (string, error) {
	if s3ControlKey(k) {
		return "", ErrNotSupported
	}
	return b.S3Bucket.PresignGet(ctx, k, ttl)
}

// ImmutableWakeHints asks the wake index to publish unique, immutable due keys.
// Mutable pointers still use fenced logical deletion.
func (b *S3ControlBucket) ImmutableWakeHints() bool { return true }

// ImmutableWakeHintKey recognizes wake/due/<minute>/<ns>/<class>/<id>/<revision>.json.
// The revision is a fresh 192-bit random value for each publication.
func ImmutableWakeHintKey(k string) bool {
	if !strings.HasPrefix(k, "wake/due/") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(k, "wake/due/"), "/")
	if len(parts) != 5 {
		return false
	}
	if _, err := time.Parse("2006-01-02T15:04", parts[0]); err != nil {
		return false
	}
	for _, part := range parts[1:4] {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	rev := strings.TrimSuffix(parts[4], ".json")
	if len(rev) != 48 || !strings.HasSuffix(parts[4], ".json") {
		return false
	}
	for _, c := range rev {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (b *S3ControlBucket) PutReader(ctx context.Context, k string, r io.ReadSeeker, n int64) (string, error) {
	if !s3ControlKey(k) {
		return b.S3Bucket.PutReader(ctx, k, r, n)
	}
	if n < 0 || n > b.maxRead-controlHeader {
		return "", objectTooLarge(k, n, b.maxRead-controlHeader)
	}
	v, err := readAllBounded(k, r, n, b.maxRead-controlHeader)
	if err != nil {
		return "", err
	}
	return b.Put(ctx, k, v)
}
