package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	cos "github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

type blockedAppendSource struct {
	reader           *bytes.Reader
	entered, release chan struct{}
	blocked          bool
	fail             bool
}

func (s *blockedAppendSource) Read(p []byte) (int, error) {
	if !s.blocked && s.reader.Size()-int64(s.reader.Len()) >= 1<<20 {
		s.blocked = true
		close(s.entered)
		<-s.release
		if s.fail {
			return 0, errors.New("injected interrupted body")
		}
	}
	if len(p) > 64<<10 {
		p = p[:64<<10]
	}
	return s.reader.Read(p)
}
func TestCOSAppendPublicationVisibility(t *testing.T) {
	if os.Getenv("CELLHIVE_NATIVE_TEST_PROVIDER") != "cos" {
		t.Skip("COS credentials required")
	}
	c, err := NewCOSAppender(os.Getenv("COS_ENDPOINT"), os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			key := fmt.Sprintf("itest/append-publication-%d", time.Now().UnixNano())
			t.Cleanup(func() { _, _ = c.client.Object.Delete(context.Background(), key) })
			data := bytes.Repeat([]byte("a"), 8<<20)
			src := &blockedAppendSource{reader: bytes.NewReader(data), entered: make(chan struct{}), release: make(chan struct{}), fail: fail}
			done := make(chan error, 1)
			headers := http.Header{}
			headers.Set("x-cos-meta-litestream-timestamp", "2026-11-10T00:00:00Z")
			go func() {
				_, _, err := c.client.Object.Append(ctx, key, 0, src, &cos.ObjectPutOptions{ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{ContentLength: int64(len(data)), XOptionHeader: &headers}})
				done <- err
			}()
			select {
			case <-src.entered:
			case err := <-done:
				t.Fatalf("upload ended early: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			head, headErr := c.client.Object.Head(ctx, key, nil)
			if head != nil {
				t.Logf("during upload length=%s type=%s", head.Header.Get("Content-Length"), head.Header.Get("X-Cos-Object-Type"))
			}
			close(src.release)
			if !errors.Is(nativeCOSError(headErr), ErrNotFound) {
				t.Errorf("incomplete append visible: %v", headErr)
			}
			err := <-done
			if fail {
				if err == nil {
					t.Fatal("failed source upload succeeded")
				}
				head, err = c.client.Object.Head(ctx, key, nil)
				if head != nil {
					t.Logf("after failure length=%s type=%s", head.Header.Get("Content-Length"), head.Header.Get("X-Cos-Object-Type"))
				}
				if !errors.Is(nativeCOSError(err), ErrNotFound) {
					t.Fatalf("partial object after failed append: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			resp, err := c.client.Object.Get(ctx, key, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || !bytes.Equal(got, data) {
				t.Fatal("body mismatch", err)
			}
			if got := resp.Header.Get("x-cos-meta-litestream-timestamp"); got != "2026-11-10T00:00:00Z" {
				t.Fatalf("timestamp=%q", got)
			}
		})
	}
}
