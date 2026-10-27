package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	oss "github.com/aliyun/aliyun-oss-go-sdk/oss"
	cos "github.com/tencentyun/cos-go-sdk-v5"
)

// These are SDK wire-contract tests, not live provider acceptance.
func TestNativeObjectCreateReviewErrorMapping(t *testing.T) {
	for _, provider := range []string{"cos", "oss"} {
		for _, tc := range []struct {
			name     string
			body     []byte
			status   int
			code     string
			conflict bool
		}{
			{"exists", nil, 409, "FileAlreadyExists", true},
			{"other-conflict", nil, 409, "OperationAborted", false},
			{"wrong-status", nil, 403, "FileAlreadyExists", false},
			{"access-denied", nil, 403, "AccessDenied", false},
		} {
			t.Run(provider+"/"+tc.name, func(t *testing.T) {
				requests := make(chan struct{}, 16)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests <- struct{}{}
					if r.Method != http.MethodPut {
						t.Errorf("method = %s", r.Method)
					}
					expected := "/assets/token/index.html"
					if provider == "oss" {
						expected = "/test-bucket" + expected
					}
					if r.URL.Path != expected || r.URL.RawQuery != "" {
						t.Errorf("target = %s", r.URL)
					}
					if got := r.Header.Get("x-" + provider + "-forbid-overwrite"); got != "true" {
						t.Errorf("forbid-overwrite = %q", got)
					}
					body, err := io.ReadAll(r.Body)
					if err != nil || !bytes.Equal(body, tc.body) {
						t.Errorf("body mismatch: err=%v bytes=%d", err, len(body))
					}
					if tc.status == 200 {
						w.Header().Set("ETag", `"object-etag"`)
					} else {
						w.Header().Set("Content-Type", "application/xml")
					}
					w.WriteHeader(tc.status)
					if tc.code != "" {
						fmt.Fprintf(w, "<Error><Code>%s</Code><Message>fixture</Message><RequestId>fixture</RequestId></Error>", tc.code)
					}
				}))
				defer server.Close()
				var creator conditionalObjectCreator
				if provider == "cos" {
					u, err := url.Parse(server.URL)
					if err != nil {
						t.Fatal(err)
					}
					creator = &COSAppender{client: cos.NewClient(&cos.BaseURL{BucketURL: u}, server.Client())}
				} else {
					client, err := oss.New(server.URL, "test-access", "test-secret", oss.ForcePathStyle(true), oss.HTTPClient(server.Client()))
					if err != nil {
						t.Fatal(err)
					}
					b, err := client.Bucket("test-bucket")
					if err != nil {
						t.Fatal(err)
					}
					creator = &OSSAppender{bucket: b}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				etag, err := creator.CreateObject(ctx, "assets/token/index.html", tc.body)
				if tc.status == 200 {
					if err != nil || etag != "object-etag" {
						t.Fatalf("etag=%q err=%v", etag, err)
					}
				} else {
					if err == nil || etag != "" {
						t.Fatalf("expected error with no etag: %q %v", etag, err)
					}
					if errors.Is(err, ErrPrecondition) != tc.conflict {
						t.Fatalf("incorrect precondition mapping: %v", err)
					}
					if !tc.conflict {
						if provider == "cos" {
							var e *cos.ErrorResponse
							if !errors.As(err, &e) || e.Code != tc.code {
								t.Fatalf("lost COS error: %v", err)
							}
						} else {
							var e oss.ServiceError
							if !errors.As(err, &e) || e.Code != tc.code {
								t.Fatalf("lost OSS error: %v", err)
							}
						}
					}
				}
				if len(requests) != 1 {
					t.Errorf("requests=%d, want single PUT", len(requests))
				}
			})
		}
	}
}
