package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	oss "github.com/aliyun/aliyun-oss-go-sdk/oss"
	cos "github.com/tencentyun/cos-go-sdk-v5"
)

type OSSAppender struct {
	bucket  *oss.Bucket
	maxRead int64
}

// conditionalObjectCreator creates immutable ordinary objects, including
// empty assets. Callers require atomic create-if-absent. Live COS tests show
// forbid-overwrite PUT does not satisfy concurrent uniqueness on the tested
// bucket; see the 2026-11-10 acceptance report. Append-at-zero also cannot
// reserve an empty object, which still has append position zero.
type conditionalObjectCreator interface {
	CreateObject(context.Context, string, []byte) (string, error)
}

func (o *OSSAppender) CreateObject(ctx context.Context, key string, data []byte) (string, error) {
	return o.CreateObjectReader(ctx, key, bytes.NewReader(data), int64(len(data)))
}

func (o *OSSAppender) CreateObjectReader(ctx context.Context, key string, src io.ReadSeeker, size int64) (string, error) {
	return o.writeObjectReader(ctx, key, src, size, true)
}
func (o *OSSAppender) PutObjectReader(ctx context.Context, key string, src io.ReadSeeker, size int64) (string, error) {
	return o.writeObjectReader(ctx, key, src, size, false)
}
func (o *OSSAppender) writeObjectReader(ctx context.Context, key string, src io.ReadSeeker, size int64, create bool) (string, error) {
	if size < 0 {
		return "", fmt.Errorf("bucket: negative object size")
	}
	timestamp, err := ltxTimestamp(key, src, size)
	if err != nil {
		return "", err
	}
	var headers http.Header
	options := []oss.Option{oss.WithContext(ctx), oss.ContentLength(size), oss.GetResponseHeader(&headers)}
	if create {
		options = append(options, oss.ForbidOverWrite(true))
	}
	if timestamp != "" {
		options = append(options, oss.Meta(ltxTimestampMetadataKey, timestamp))
	}
	err = o.bucket.PutObject(key, borrowedReadSeeker{src}, options...)
	if err != nil {
		var svc oss.ServiceError
		if errors.As(err, &svc) && svc.StatusCode == http.StatusConflict && svc.Code == "FileAlreadyExists" {
			return "", ErrPrecondition
		}
		return "", nativeOSSError(err)
	}
	return strings.Trim(headers.Get("ETag"), `"`), nil
}

func NewOSSAppender(endpoint, name, access, secret string) (*OSSAppender, error) {
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	if !strings.HasPrefix(endpoint, "https://") {
		return nil, fmt.Errorf("OSS endpoint must use HTTPS")
	}
	client, err := oss.New(endpoint, access, secret, oss.HTTPClient(newBoundedHTTPClient()))
	if err != nil {
		return nil, err
	}
	b, err := client.Bucket(name)
	if err != nil {
		return nil, err
	}
	return &OSSAppender{bucket: b}, nil
}
func (o *OSSAppender) Read(ctx context.Context, key string) ([]byte, error) {
	result, err := o.bucket.GetObject(key, oss.WithContext(ctx))
	if err != nil {
		return nil, nativeOSSError(err)
	}
	defer result.Close()
	return readAllBounded(key, result, -1, o.readLimit())
}

func (o *OSSAppender) readLimit() int64 {
	if o.maxRead > 0 {
		return o.maxRead
	}
	return DefaultMaxObjectBytes
}
func (o *OSSAppender) Append(ctx context.Context, key string, position int64, data []byte) error {
	_, err := o.bucket.AppendObject(key, bytes.NewReader(data), position, oss.WithContext(ctx))
	return nativeOSSError(err)
}
func nativeOSSError(err error) error {
	if err == nil {
		return nil
	}
	var svc oss.ServiceError
	if errors.As(err, &svc) {
		if svc.Code == "NoSuchKey" || svc.Code == "NoSuchBucket" {
			return ErrNotFound
		}
		if svc.Code == "PositionNotEqualToLength" || svc.Code == "ObjectNotAppendable" {
			return ErrPrecondition
		}
	}
	return err
}

type COSAppender struct {
	client  *cos.Client
	maxRead int64
}

func (c *COSAppender) CreateObject(ctx context.Context, key string, data []byte) (string, error) {
	return c.CreateObjectReader(ctx, key, bytes.NewReader(data), int64(len(data)))
}

func (c *COSAppender) CreateObjectReader(ctx context.Context, key string, src io.ReadSeeker, size int64) (string, error) {
	return c.writeObjectReader(ctx, key, src, size, true)
}
func (c *COSAppender) PutObjectReader(ctx context.Context, key string, src io.ReadSeeker, size int64) (string, error) {
	return c.writeObjectReader(ctx, key, src, size, false)
}
func (c *COSAppender) writeObjectReader(ctx context.Context, key string, src io.ReadSeeker, size int64, create bool) (string, error) {
	if size < 0 {
		return "", fmt.Errorf("bucket: negative object size")
	}
	timestamp, err := ltxTimestamp(key, src, size)
	if err != nil {
		return "", err
	}
	headers := http.Header{}
	if timestamp != "" {
		headers.Set("x-cos-meta-"+ltxTimestampMetadataKey, timestamp)
	}
	if size >= nativeMultipartThresholdBytes {
		return c.writeMultipart(ctx, key, src, size, headers, create)
	}
	if create {
		headers.Set("x-cos-forbid-overwrite", "true")
	}
	resp, err := c.client.Object.Put(ctx, key, borrowedReadSeeker{src}, &cos.ObjectPutOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{XOptionHeader: &headers, ContentLength: size},
	})
	if err != nil {
		var svc *cos.ErrorResponse
		if errors.As(err, &svc) && svc.Response != nil && svc.Response.StatusCode == http.StatusConflict && svc.Code == "FileAlreadyExists" {
			return "", ErrPrecondition
		}
		return "", nativeCOSError(err)
	}
	if resp == nil {
		return "", nil
	}
	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

func NewCOSAppender(endpoint, access, secret string) (*COSAppender, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("COS endpoint must be an HTTPS bucket URL")
	}
	c := cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  access,
			SecretKey: secret,
			Transport: debugTransport(newBoundedHTTPTransport(), nil),
		},
	})
	return &COSAppender{client: c, maxRead: DefaultMaxObjectBytes}, nil
}

func (c *COSAppender) checkAZ(ctx context.Context) error {
	resp, err := c.client.Bucket.Head(ctx)
	if err != nil {
		return fmt.Errorf("COS bucket HEAD: %w", err)
	}
	if strings.EqualFold(resp.Header.Get("X-Cos-Bucket-Az-Type"), "MAZ") {
		return fmt.Errorf("COS multi-AZ bucket does not support APPEND Object; use a single-AZ bucket for owner/lease authority")
	}
	return nil
}

// COSServiceEndpoint converts the bucket URL required by the native SDK into
// the service endpoint expected by the S3 resolver, which prepends the bucket.
func COSServiceEndpoint(bucketURL, name string) (string, string, error) {
	u, err := url.Parse(bucketURL)
	if err != nil || u.Scheme != "https" || u.Port() != "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return "", "", fmt.Errorf("invalid COS bucket endpoint")
	}
	service, ok := strings.CutPrefix(u.Hostname(), name+".")
	if !ok || !strings.HasPrefix(service, "cos.") || !strings.HasSuffix(service, ".myqcloud.com") {
		return "", "", fmt.Errorf("COS endpoint must match bucket %q", name)
	}
	region := strings.TrimSuffix(strings.TrimPrefix(service, "cos."), ".myqcloud.com")
	if region == "" || strings.Contains(region, ".") {
		return "", "", fmt.Errorf("invalid COS region")
	}
	return "https://" + service, region, nil
}

func (c *COSAppender) Read(ctx context.Context, key string) ([]byte, error) {
	r, err := c.client.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, nativeCOSError(err)
	}
	defer r.Body.Close()
	return readAllBounded(key, r.Body, r.ContentLength, c.readLimit())
}

func (c *COSAppender) readLimit() int64 {
	if c.maxRead > 0 {
		return c.maxRead
	}
	return DefaultMaxObjectBytes
}
func (c *COSAppender) Append(ctx context.Context, key string, position int64, data []byte) error {
	_, err := c.AppendWithETag(ctx, key, position, data)
	return err
}

// AppendWithETag returns the ETag carried by a successful COS append. For a
// newly created immutable object this is the object's ETag, avoiding a
// separate HEAD after the append. Older/compatible endpoints may omit it.
func (c *COSAppender) AppendWithETag(ctx context.Context, key string, position int64, data []byte) (string, error) {
	if int64(int(position)) != position {
		return "", fmt.Errorf("COS append position overflow")
	}
	_, resp, err := c.client.Object.Append(ctx, key, int(position), bytes.NewReader(data), nil)
	if err != nil {
		return "", nativeCOSError(err)
	}
	if resp == nil {
		return "", nil
	}
	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}
func nativeCOSError(err error) error {
	if err == nil {
		return nil
	}
	var svc *cos.ErrorResponse
	if errors.As(err, &svc) {
		if svc.Code == "NoSuchKey" || svc.Response != nil && svc.Response.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}
		if svc.Code == "PositionNotEqualToLength" || svc.Code == "AppendPositionErr" || svc.Code == "ObjectNotAppendable" {
			return ErrPrecondition
		}
	}
	return err
}
