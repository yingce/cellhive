package bucket

import (
	"bytes"
	"cellhive/internal/ltx"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	oss "github.com/aliyun/aliyun-oss-go-sdk/oss"
	cos "github.com/tencentyun/cos-go-sdk-v5"
	"hash/crc64"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type assetCreatorProbe struct {
	etagAppender
	calls int
	body  []byte
	err   error
}

func (a *assetCreatorProbe) CreateObject(ctx context.Context, key string, data []byte) (string, error) {
	a.calls++
	a.body = data
	return "created-etag", a.err
}
func TestProviderAssetsUseNativeImmutableCreate(t *testing.T) {
	for _, body := range [][]byte{nil, []byte("asset")} {
		a := &assetCreatorProbe{}
		b := NewProviderBucket(nil, a)
		etag, err := b.ConditionalCreate(t.Context(), "assets/ns/worker/manifest/index.html", body)
		if err != nil || etag != "created-etag" || a.calls != 1 || string(a.body) != string(body) {
			t.Fatalf("create: etag=%q calls=%d err=%v", etag, a.calls, err)
		}
		a.err = ErrPrecondition
		if _, err = b.ConditionalCreate(t.Context(), "assets/ns/worker/manifest/index.html", body); !errors.Is(err, ErrPrecondition) {
			t.Fatalf("conflict: %v", err)
		}
	}
	b := NewProviderBucket(nil, etagAppender{})
	if _, err := b.ConditionalCreate(t.Context(), "assets/ns/worker/manifest/empty", nil); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("unsupported creator: %v", err)
	}
}

type assetRoundTripFunc func(*http.Request) (*http.Response, error)

func (f assetRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestProviderAssetCreateNativeHeadersAndConflicts(t *testing.T) {
	native, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, Timestamp: 500}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"cos", "oss"} {
		t.Run(kind, func(t *testing.T) {
			var mu sync.Mutex
			objects := map[string][]byte{}
			transport := assetRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if kind == "cos" && strings.HasPrefix(r.URL.Path, "/assets/") {
					if r.Method != http.MethodPost || r.URL.Query().Get("position") != "0" || !r.URL.Query().Has("append") {
						t.Errorf("COS assets must use atomic append at zero, got %s %s", r.Method, r.URL.RawQuery)
					}
				} else if r.Method != http.MethodPut || r.URL.RawQuery != "" || r.Header.Get("x-"+kind+"-forbid-overwrite") != "true" {
					t.Errorf("expected native forbid-overwrite PUT, got %s query=%q", r.Method, r.URL.RawQuery)
				}
				var body []byte
				var err error
				if r.Body != nil {
					body, err = io.ReadAll(r.Body)
				}
				if err != nil {
					return nil, err
				}
				if bytes.Equal(body, native) && r.Header.Get("x-"+kind+"-meta-litestream-timestamp") != "1970-01-01T00:00:00.5Z" {
					t.Errorf("missing native timestamp metadata")
				}
				mu.Lock()
				defer mu.Unlock()
				status, response := http.StatusOK, ""
				if _, exists := objects[r.URL.Path]; exists {
					status = http.StatusConflict
					response = "<Error><Code>FileAlreadyExists</Code><Message>exists</Message></Error>"
					if r.Method == http.MethodPost {
						response = "<Error><Code>PositionNotEqualToLength</Code></Error>"
					}
				} else {
					objects[r.URL.Path] = append([]byte(nil), body...)
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Etag": []string{`"asset-etag"`}, "X-Cos-Content-Sha1": []string{fmt.Sprintf("%x", md5.Sum(body))}, "X-Cos-Next-Append-Position": []string{strconv.Itoa(len(body))}, "X-Cos-Hash-Crc64ecma": []string{strconv.FormatUint(crc64.Checksum(body, crc64.MakeTable(crc64.ECMA)), 10)}}, Body: io.NopCloser(strings.NewReader(response)), Request: r}, nil
			})
			client := &http.Client{Transport: transport}
			var appender PositionAppender
			if kind == "cos" {
				u, _ := url.Parse("https://bucket.invalid")
				appender = &COSAppender{client: cos.NewClient(&cos.BaseURL{BucketURL: u}, client)}
			} else {
				c, err := oss.New("https://oss.invalid", "test", "test", oss.HTTPClient(client))
				if err != nil {
					t.Fatal(err)
				}
				b, err := c.Bucket("test-bucket")
				if err != nil {
					t.Fatal(err)
				}
				appender = &OSSAppender{bucket: b}
			}
			provider := NewProviderBucket(nil, appender)
			for i, body := range [][]byte{nil, []byte("original"), native} {
				key := fmt.Sprintf("assets/ns/worker/manifest/file%d", i)
				if i == 2 {
					key = "cells/n/c/i/ltx/e1/0000/0000000000000002-0000000000000002.ltx"
				}
				etag, err := provider.ConditionalCreate(t.Context(), key, body)
				if err != nil || etag != "asset-etag" {
					t.Fatalf("first create: etag=%q error=%v", etag, err)
				}
				for _, replacement := range [][]byte{body, nil, []byte("changed")} {
					if _, err := provider.ConditionalCreate(t.Context(), key, replacement); !errors.Is(err, ErrPrecondition) {
						t.Fatalf("repeat: %v", err)
					}
				}
				mu.Lock()
				got := objects["/"+key]
				mu.Unlock()
				if kind == "cos" && strings.HasPrefix(key, "assets/") {
					if _, err := assetLength(got[:assetHeaderSize], int64(len(got))); err != nil {
						t.Fatal(err)
					}
					got = got[assetHeaderSize:]
				}
				if !bytes.Equal(got, body) {
					t.Fatal("existing bytes changed")
				}
			}
		})
	}
}
