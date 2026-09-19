package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	aws "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// S3Options configures an S3-compatible bucket.
type S3Options struct {
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
	Bucket    string
	PathStyle bool
	Debug     bool
}

// S3Bucket is an S3-compatible Bucket implementation (AWS S3 / R2 / MinIO).
//
// Conditional create uses If-None-Match:* and CAS uses If-Match; both surface
// ErrPrecondition on HTTP 412.
type S3Bucket struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string

	puts    atomic.Uint64
	gets    atomic.Uint64
	lists   atomic.Uint64
	creates atomic.Uint64
	casOps  atomic.Uint64
	deletes atomic.Uint64
}

// NewS3Bucket builds an S3 bucket client.
func NewS3Bucket(ctx context.Context, o S3Options) (*S3Bucket, error) {
	if o.Region == "" {
		o.Region = "us-east-1"
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(o.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(o.AccessKey, o.SecretKey, "")),
	)
	if err != nil {
		return nil, err
	}
	if o.Debug {
		cfg.ClientLogMode = aws.LogRequestWithBody | aws.LogResponseWithBody
	}
	client := s3.NewFromConfig(cfg, func(opts *s3.Options) {
		if o.Endpoint != "" {
			opts.BaseEndpoint = aws.String(o.Endpoint)
			opts.UsePathStyle = o.PathStyle
		}
	})
	return &S3Bucket{client: client, presign: s3.NewPresignClient(client), bucket: o.Bucket}, nil
}

func unquote(etag string) string { return strings.Trim(etag, `"`) }

func quote(etag string) string { return `"` + unquote(etag) + `"` }

func isPrecondition(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict", "412":
			return true
		}
	}
	return false
}

func isNotFound(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return true
		}
	}
	return false
}

// Get implements Bucket.
func (b *S3Bucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.gets.Add(1)
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &b.bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			return nil, "", ErrNotFound
		}
		return nil, "", err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
	}
	return data, unquote(aws.ToString(out.ETag)), nil
}

// RangedGet implements Bucket.
func (b *S3Bucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	b.gets.Add(1)
	rng := fmt.Sprintf("bytes=%d-%d", off, off+length-1)
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &b.bucket, Key: &key, Range: &rng})
	if err != nil {
		if isNotFound(err) {
			return nil, "", ErrNotFound
		}
		return nil, "", err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
	}
	return data, unquote(aws.ToString(out.ETag)), nil
}

// Put implements Bucket.
func (b *S3Bucket) Put(ctx context.Context, key string, data []byte) (string, error) {
	b.puts.Add(1)
	out, err := b.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &b.bucket, Key: &key, Body: bytes.NewReader(data)})
	if err != nil {
		return "", err
	}
	return unquote(aws.ToString(out.ETag)), nil
}

// ConditionalCreate implements Bucket.
func (b *S3Bucket) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	b.creates.Add(1)
	star := "*"
	out, err := b.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &b.bucket, Key: &key, Body: bytes.NewReader(data), IfNoneMatch: &star})
	if err != nil {
		if isPrecondition(err) {
			return "", ErrPrecondition
		}
		return "", err
	}
	return unquote(aws.ToString(out.ETag)), nil
}

// CAS implements Bucket.
func (b *S3Bucket) CAS(ctx context.Context, key string, data []byte, expectEtag string) (string, error) {
	b.casOps.Add(1)
	em := quote(expectEtag)
	out, err := b.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &b.bucket, Key: &key, Body: bytes.NewReader(data), IfMatch: &em})
	if err != nil {
		if isPrecondition(err) {
			return "", ErrPrecondition
		}
		return "", err
	}
	return unquote(aws.ToString(out.ETag)), nil
}

// ConditionalDelete implements Bucket via S3's If-Match on DeleteObject. On
// stores that ignore the header this degrades to the previous unconditional
// delete (documented in docs/known-issues.md).
func (b *S3Bucket) ConditionalDelete(ctx context.Context, key, expectEtag string) error {
	b.deletes.Add(1)
	em := quote(expectEtag)
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &b.bucket, Key: &key, IfMatch: &em})
	if err != nil {
		if isPrecondition(err) {
			return ErrPrecondition
		}
		return err
	}
	return nil
}

// Stat implements Statter with HeadObject (no body transfer).
func (b *S3Bucket) Stat(ctx context.Context, key string) (int64, string, error) {
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &b.bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			return 0, "", ErrNotFound
		}
		return 0, "", err
	}
	return aws.ToInt64(out.ContentLength), unquote(aws.ToString(out.ETag)), nil
}

// ListSizes implements SizeLister from the S3 list response (no per-object GET).
func (b *S3Bucket) ListSizes(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	b.lists.Add(1)
	var out []ObjectInfo
	var token *string
	for {
		res, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &b.bucket, Prefix: &prefix, ContinuationToken: token})
		if err != nil {
			return nil, err
		}
		for _, o := range res.Contents {
			out = append(out, ObjectInfo{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), ETag: unquote(aws.ToString(o.ETag))})
		}
		if res.IsTruncated == nil || !*res.IsTruncated {
			break
		}
		token = res.NextContinuationToken
	}
	return out, nil
}

// ListPage implements PagedLister with S3 StartAfter/MaxKeys: one bounded list
// call per page, sizes from the list response, no per-object GET.
func (b *S3Bucket) ListPage(ctx context.Context, prefix, after string, limit int) ([]ObjectInfo, string, error) {
	if limit <= 0 {
		limit = 100
	}
	b.lists.Add(1)
	in := &s3.ListObjectsV2Input{Bucket: &b.bucket, Prefix: &prefix, MaxKeys: aws.Int32(int32(limit))}
	if after != "" {
		in.StartAfter = &after
	}
	res, err := b.client.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, "", err
	}
	out := make([]ObjectInfo, 0, len(res.Contents))
	for _, o := range res.Contents {
		out = append(out, ObjectInfo{Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), ETag: unquote(aws.ToString(o.ETag))})
	}
	next := ""
	if res.IsTruncated != nil && *res.IsTruncated && len(out) > 0 {
		next = out[len(out)-1].Key
	}
	return out, next, nil
}

// Delete implements Bucket.
func (b *S3Bucket) Delete(ctx context.Context, key string) error {
	b.deletes.Add(1)
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &b.bucket, Key: &key})
	return err
}

// List implements Bucket (operator/diagnostic use only).
func (b *S3Bucket) List(ctx context.Context, prefix string) ([]string, error) {
	b.lists.Add(1)
	var keys []string
	var token *string
	for {
		out, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &b.bucket, Prefix: &prefix, ContinuationToken: token})
		if err != nil {
			return nil, err
		}
		for _, o := range out.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}
	return keys, nil
}

// PresignGet implements Bucket.
func (b *S3Bucket) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	req, err := b.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: &b.bucket, Key: &key},
		func(o *s3.PresignOptions) { o.Expires = ttl })
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

// Stats returns operation counters.
func (b *S3Bucket) Stats() map[string]uint64 {
	return map[string]uint64{
		"put":                b.puts.Load(),
		"get":                b.gets.Load(),
		"list":               b.lists.Load(),
		"conditional_create": b.creates.Load(),
		"cas":                b.casOps.Load(),
		"delete":             b.deletes.Load(),
	}
}

var _ Bucket = (*S3Bucket)(nil)
