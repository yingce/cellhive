package bucket

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

// Run with the same local source and compile-time part-size variants to measure
// the production multipart path without adding a runtime configuration option.
func TestCOSMultipartThroughput(t *testing.T) {
	source := os.Getenv("CELLHIVE_MULTIPART_BENCH_FILE")
	if source == "" {
		t.Skip("set CELLHIVE_MULTIPART_BENCH_FILE for live benchmark")
	}
	endpoint := os.Getenv("COS_ENDPOINT")
	if endpoint != "https://cellhive-e2e-hk-1376795072.cos.ap-hongkong.myqcloud.com" {
		t.Fatal("dedicated HK bucket required")
	}
	c, err := NewCOSAppender(endpoint, os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%x", h.Sum(nil))
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("cells/multipart-benchmark/__bench__/%d/ltx/e1/0009/0000000000000001-0000000000000001.ltx", time.Now().UnixNano())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := c.client.Object.Delete(ctx, key); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	start := time.Now()
	tag, err := c.createMultipart(ctx, key, f, st.Size(), nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if tag == "" {
		t.Fatal("missing etag")
	}
	response, err := c.client.Object.Get(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.Reset()
	n, err := io.Copy(h, response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if n != st.Size() || fmt.Sprintf("%x", h.Sum(nil)) != want {
		t.Fatal("download SHA256/size mismatch")
	}
	t.Logf("RESULT part_mib=%d bytes=%d seconds=%.6f mib_per_sec=%.3f sha256=%s", nativeMultipartPartBytes>>20, st.Size(), elapsed.Seconds(), float64(st.Size())/(1<<20)/elapsed.Seconds(), want)
}
