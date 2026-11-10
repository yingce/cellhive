package bucket

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

func TestCOSMultipartAdmissionCancellation(t *testing.T) {
	first, err := acquireMultipart(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	second, err := acquireMultipart(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		release, err := acquireMultipart(ctx)
		if release != nil {
			release()
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("admission exceeded shared limit: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued upload ignored cancellation")
	}
}

// Part1 cannot finish before part2 arrives. This verifies real overlap rather
// than inferring concurrency from wall-clock throughput on a shared machine.
func TestCOSMultipartOverlapsPartsAndCompletesInOrder(t *testing.T) {
	second := make(chan struct{})
	completed := make(chan struct{}, 1)
	data := bytes.Repeat([]byte{0x71}, int(nativeMultipartPartBytes)+19)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			fmt.Fprint(w, "<InitiateMultipartUploadResult><UploadId>overlap</UploadId></InitiateMultipartUploadResult>")
		case r.Method == http.MethodPut:
			n, _ := strconv.Atoi(q.Get("partNumber"))
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			switch n {
			case 1:
				if !bytes.Equal(body, data[:nativeMultipartPartBytes]) {
					t.Error("part1 bytes changed")
				}
				select {
				case <-second:
				case <-time.After(2 * time.Second):
					w.WriteHeader(400)
					fmt.Fprint(w, "<Error><Code>InvalidArgument</Code><Message>no concurrent second part</Message></Error>")
					return
				}
			case 2:
				if !bytes.Equal(body, data[nativeMultipartPartBytes:]) {
					t.Error("part2 bytes changed")
				}
				close(second)
			default:
				t.Errorf("unexpected part %d", n)
			}
			w.Header().Set("ETag", fmt.Sprintf("part-%d", n))
			w.Header().Set("x-cos-hash-crc64ecma", strconv.FormatUint(crc64.Checksum(body, crc64.MakeTable(crc64.ECMA)), 10))
		case r.Method == http.MethodPost:
			var req cos.CompleteMultipartUploadOptions
			if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			if len(req.Parts) != 2 || req.Parts[0].PartNumber != 1 || req.Parts[1].PartNumber != 2 || req.Parts[0].ETag != "part-1" || req.Parts[1].ETag != "part-2" {
				t.Errorf("unordered completion: %+v", req.Parts)
			}
			completed <- struct{}{}
			fmt.Fprint(w, "<CompleteMultipartUploadResult><ETag>complete</ETag></CompleteMultipartUploadResult>")
		case r.Method == http.MethodDelete:
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	c := &COSAppender{client: cos.NewClient(&cos.BaseURL{BucketURL: u}, server.Client())}
	tag, err := c.CreateObjectReader(t.Context(), "cells/n/c/id/ltx/e1/0009/overlap.ltx", bytes.NewReader(data), int64(len(data)))
	if err != nil || tag != "complete" {
		t.Fatalf("concurrent upload failed: tag=%q err=%v", tag, err)
	}
	select {
	case <-completed:
	default:
		t.Fatal("multipart did not complete")
	}
}

func TestCOSMultipartAtomicCompletionAndAbort(t *testing.T) {
	for _, mode := range []string{"success", "exists", "part-failure", "cancel", "short-source", "long-source", "missing-etag"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			data := bytes.Repeat([]byte{0x45}, int(nativeMultipartPartBytes)+19)
			var mu sync.Mutex
			var published []byte
			parts := map[int][]byte{}
			aborted := false
			completed := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.URL.Path != "/cells/n/c/id/ltx/e1/0009/object.ltx" {
					t.Errorf("wrong key %s", r.URL.Path)
				}
				q := r.URL.Query()
				switch {
				case r.Method == http.MethodPost && q.Has("uploads"):
					fmt.Fprint(w, "<InitiateMultipartUploadResult><UploadId>test-id</UploadId></InitiateMultipartUploadResult>")
				case r.Method == http.MethodPut && q.Get("uploadId") == "test-id":
					if len(published) != 0 {
						t.Error("visible before complete")
					}
					n, _ := strconv.Atoi(q.Get("partNumber"))
					body, err := io.ReadAll(r.Body)
					if err != nil {
						// A sibling failure cancels in-flight request bodies. These
						// modes still assert abort and absence of publication below.
						if mode == "success" || mode == "exists" {
							t.Error(err)
						}
						return
					}
					if int64(len(body)) > nativeMultipartPartBytes {
						t.Error("unbounded part")
					}
					if mode == "part-failure" {
						w.WriteHeader(400)
						fmt.Fprint(w, "<Error><Code>InvalidArgument</Code></Error>")
						return
					}
					w.Header().Set("x-cos-hash-crc64ecma", strconv.FormatUint(crc64.Checksum(body, crc64.MakeTable(crc64.ECMA)), 10))
					parts[n] = body
					if mode != "missing-etag" {
						w.Header().Set("ETag", fmt.Sprintf("part-%d", n))
					}
					if mode == "cancel" {
						cancel()
					}
				case r.Method == http.MethodPost && q.Get("uploadId") == "test-id":
					completed = true
					if r.Header.Get("x-cos-forbid-overwrite") != "true" {
						t.Error("missing atomic no-overwrite")
					}
					var request cos.CompleteMultipartUploadOptions
					if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					if len(request.Parts) != 2 {
						t.Errorf("completion parts=%d", len(request.Parts))
					}
					if mode == "exists" {
						w.WriteHeader(409)
						fmt.Fprint(w, "<Error><Code>FileAlreadyExists</Code></Error>")
						return
					}
					published = append(append([]byte{}, parts[1]...), parts[2]...)
					fmt.Fprint(w, "<CompleteMultipartUploadResult><ETag>final-etag</ETag></CompleteMultipartUploadResult>")
				case r.Method == http.MethodDelete && q.Get("uploadId") == "test-id":
					aborted = true
					w.WriteHeader(204)
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL)
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			u, _ := url.Parse(server.URL)
			c := &COSAppender{client: cos.NewClient(&cos.BaseURL{BucketURL: u}, server.Client())}
			source := data
			if mode == "short-source" {
				source = data[:len(data)-1]
			}
			if mode == "long-source" {
				source = append(append([]byte{}, data...), 1)
			}
			tag, err := c.CreateObjectReader(ctx, "cells/n/c/id/ltx/e1/0009/object.ltx", bytes.NewReader(source), int64(len(data)))
			mu.Lock()
			defer mu.Unlock()
			if mode == "success" {
				if err != nil || tag != "final-etag" || aborted || !completed || !bytes.Equal(published, data) {
					t.Fatalf("result %s %v abort=%v complete=%v", tag, err, aborted, completed)
				}
			} else {
				if err == nil || !aborted || len(published) != 0 {
					t.Fatalf("failed upload published: err=%v abort=%v", err, aborted)
				}
				if mode == "exists" && !errors.Is(err, ErrPrecondition) {
					t.Fatalf("conflict=%v", err)
				}
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel=%v", err)
				}
			}
		})
	}
}

func TestCOSMultipartConcurrencyBound(t *testing.T) {
	// The helper permits tiny fixture parts; production always uses >=32MiB.
	// More than one batch catches an implementation that spawns one worker per
	// part without bounding source read-ahead or outstanding requests.
	const partBytes = 16
	data := bytes.Repeat([]byte{0x27}, partBytes*9)
	var active, peak, arrived atomic.Int32
	gate := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, data[:partBytes]) {
			t.Errorf("part data: %x %v", body, err)
		}
		if arrived.Add(1) == nativeMultipartConcurrency {
			close(gate)
		}
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("ETag", "part-"+r.URL.Query().Get("partNumber"))
		w.Header().Set("x-cos-hash-crc64ecma", strconv.FormatUint(crc64.Checksum(body, crc64.MakeTable(crc64.ECMA)), 10))
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	c := &COSAppender{client: cos.NewClient(&cos.BaseURL{BucketURL: u}, server.Client())}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	parts, err := c.uploadMultipartParts(ctx, "object", "upload", bytes.NewReader(data), int64(len(data)), partBytes)
	if err != nil {
		t.Fatal(err)
	}
	if peak.Load() != nativeMultipartConcurrency || len(parts) != 9 {
		t.Fatalf("peak=%d parts=%d", peak.Load(), len(parts))
	}
	for i, p := range parts {
		if p.PartNumber != i+1 || p.ETag != fmt.Sprintf("part-%d", i+1) {
			t.Fatalf("part[%d]=%+v", i, p)
		}
	}
}

func TestCOSSinglePutDoesNotCloseCallerSpool(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				if string(raw) != "snapshot" {
					t.Errorf("body=%q", raw)
				}
				if conflict {
					w.WriteHeader(409)
					fmt.Fprint(w, "<Error><Code>FileAlreadyExists</Code></Error>")
					return
				}
				w.Header().Set("x-cos-hash-crc64ecma", strconv.FormatUint(crc64.Checksum(raw, crc64.MakeTable(crc64.ECMA)), 10))
				w.Header().Set("ETag", "created")
			}))
			defer server.Close()
			u, _ := url.Parse(server.URL)
			c := &COSAppender{client: cos.NewClient(&cos.BaseURL{BucketURL: u}, server.Client())}
			f, err := os.CreateTemp(t.TempDir(), "snapshot")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err = f.WriteString("snapshot"); err != nil {
				t.Fatal(err)
			}
			if _, err = f.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			_, err = c.CreateObjectReader(t.Context(), "cells/n/c/id/ltx/e1/0000/file.ltx", f, 8)
			if conflict {
				if !errors.Is(err, ErrPrecondition) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				t.Fatalf("SDK closed caller spool: %v", err)
			}
			raw, err := io.ReadAll(f)
			if err != nil || string(raw) != "snapshot" {
				t.Fatalf("retry source=%q err=%v", raw, err)
			}
		})
	}
}

func TestMultipartPartBytes(t *testing.T) {
	threshold := nativeMultipartPartBytes * 10000
	for _, tc := range []struct{ size, want int64 }{
		{1, nativeMultipartPartBytes},
		{threshold - 1, nativeMultipartPartBytes},
		{threshold, nativeMultipartPartBytes},
		{threshold + 1, nativeMultipartPartBytes + 1},
		{threshold + 10000, nativeMultipartPartBytes + 1},
		{threshold + 10001, nativeMultipartPartBytes + 2},
		{60 << 30, 32 << 20},
		{100 << 30, 32 << 20},
		{1<<63 - 1, 922337203685478},
	} {
		t.Run(strconv.FormatInt(tc.size, 10), func(t *testing.T) {
			got := multipartPartBytes(tc.size)
			if got != tc.want {
				t.Fatalf("part bytes=%d want %d", got, tc.want)
			}
			count := 1 + (tc.size-1)/got
			if count > 10000 {
				t.Fatalf("part count=%d exceeds provider limit", count)
			}
		})
	}
}

// Hold a canceled UploadPart inside RoundTrip: handler completion is not proof
// that the SDK worker has returned and released its upload buffer.
func TestCOSMultipartJoinsCanceledTransportBeforeAbort(t *testing.T) {
	second := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	aborted := make(chan struct{}, 1)
	var releaseOnce, secondOnce, canceledOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	response := func(r *http.Request, status int, body string) *http.Response {
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}
	}
	transport := assetRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			return response(r, 200, "<InitiateMultipartUploadResult><UploadId>join</UploadId></InitiateMultipartUploadResult>"), nil
		case r.Method == http.MethodPut && q.Get("partNumber") == "1":
			select {
			case <-second:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			return response(r, 400, "<Error><Code>InvalidArgument</Code></Error>"), nil
		case r.Method == http.MethodPut && q.Get("partNumber") == "2":
			secondOnce.Do(func() { close(second) })
			<-r.Context().Done()
			canceledOnce.Do(func() { close(canceled) })
			<-release
			return nil, r.Context().Err()
		case r.Method == http.MethodDelete:
			aborted <- struct{}{}
			return response(r, 204, ""), nil
		default:
			t.Errorf("unexpected multipart request: %s %s", r.Method, r.URL)
			return response(r, 400, "<Error><Code>InvalidArgument</Code></Error>"), nil
		}
	})
	u, _ := url.Parse("http://multipart.invalid")
	c := &COSAppender{client: cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{Transport: transport})}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	data := bytes.Repeat([]byte{1}, int(nativeMultipartPartBytes)+19)
	done := make(chan error, 1)
	go func() {
		_, err := c.createMultipart(ctx, "cells/n/c/id/ltx/e1/0009/join.ltx", bytes.NewReader(data), int64(len(data)), nil)
		done <- err
	}()
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("sibling was not canceled")
	}
	select {
	case <-aborted:
		t.Fatal("aborted while canceled transport still owns a part")
	case <-done:
		t.Fatal("returned while canceled transport still owns a part")
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("part failure succeeded")
		}
	case <-ctx.Done():
		t.Fatal("upload failed to join")
	}
	select {
	case <-aborted:
	default:
		t.Fatal("failed upload was not aborted")
	}
}
