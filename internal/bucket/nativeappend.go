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

type OSSAppender struct{ bucket *oss.Bucket }

func NewOSSAppender(endpoint, name, access, secret string) (*OSSAppender, error) {
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	if !strings.HasPrefix(endpoint, "https://") {
		return nil, fmt.Errorf("OSS endpoint must use HTTPS")
	}
	client, err := oss.New(endpoint, access, secret)
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
	return io.ReadAll(result)
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
		if svc.Code == "PositionNotEqualToLength" {
			return ErrPrecondition
		}
	}
	return err
}

type COSAppender struct{ client *cos.Client }

func NewCOSAppender(endpoint, access, secret string) (*COSAppender, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("COS endpoint must be an HTTPS bucket URL")
	}
	c := cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{Transport: &cos.AuthorizationTransport{SecretID: access, SecretKey: secret}})
	return &COSAppender{client: c}, nil
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
	return io.ReadAll(r.Body)
}
func (c *COSAppender) Append(ctx context.Context, key string, position int64, data []byte) error {
	if int64(int(position)) != position {
		return fmt.Errorf("COS append position overflow")
	}
	_, _, err := c.client.Object.Append(ctx, key, int(position), bytes.NewReader(data), nil)
	return nativeCOSError(err)
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
		if svc.Code == "PositionNotEqualToLength" || svc.Code == "AppendPositionErr" {
			return ErrPrecondition
		}
	}
	return err
}
