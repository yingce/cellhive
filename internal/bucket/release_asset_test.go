package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	cos "github.com/tencentyun/cos-go-sdk-v5"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
)

func TestCOSAssetImmutableCreateLive(t *testing.T) {
	if os.Getenv("CELLHIVE_NATIVE_TEST_PROVIDER") != "cos" {
		t.Skip("set CELLHIVE_NATIVE_TEST_PROVIDER=cos and COS credentials")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	endpoint, name := os.Getenv("COS_ENDPOINT"), os.Getenv("COS_BUCKET")
	service, region, err := COSServiceEndpoint(endpoint, name)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := NewS3Bucket(ctx, S3Options{Endpoint: service, Region: region, Bucket: name, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	appender, err := NewCOSAppender(endpoint, os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("CELLHIVE_COS_CREATE_DIAG") == "1" {
		u, err := url.Parse(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		transport := newBoundedHTTPTransport()
		trace := assetRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			resp, err := transport.RoundTrip(r)
			if resp != nil {
				t.Logf("wire method=%s forbid=%q status=%d etag=%q request_id=%q", r.Method, r.Header.Get("x-cos-forbid-overwrite"), resp.StatusCode, resp.Header.Get("ETag"), resp.Header.Get("X-Cos-Request-Id"))
			}
			return resp, err
		})
		auth := &cos.AuthorizationTransport{SecretID: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), Transport: trace}
		appender.client = cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{Transport: auth})
	}
	b := NewProviderBucket(raw, appender)
	prefix := fmt.Sprintf("assets/release-probe/%d/manifest/", time.Now().UnixNano())
	for _, value := range [][]byte{nil, []byte("original")} {
		key := prefix + fmt.Sprint(len(value))
		t.Cleanup(func() {
			if err := b.Delete(context.Background(), key); err != nil {
				t.Error("cleanup failed")
			}
		})
		if _, err := b.ConditionalCreate(ctx, key, value); err != nil {
			t.Fatal(err)
		}
		for _, retry := range [][]byte{value, []byte("changed")} {
			if _, err := b.ConditionalCreate(ctx, key, retry); !errors.Is(err, ErrPrecondition) {
				t.Fatalf("duplicate: %v", err)
			}
		}
		got, _, err := b.Get(ctx, key)
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("immutable content mismatch: %v", err)
		}
	}
	for _, empty := range []bool{false, true} {
		key := prefix + fmt.Sprintf("concurrent-%v", empty)
		t.Cleanup(func() {
			if err := b.Delete(context.Background(), key); err != nil {
				t.Error("cleanup failed")
			}
		})
		var wg sync.WaitGroup
		var mu sync.Mutex
		var winner []byte
		var winners int
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				value := []byte(fmt.Sprint(i))
				if empty {
					value = nil
				}
				etag, err := b.ConditionalCreate(ctx, key, value)
				t.Logf("concurrent create writer=%d etag=%q err=%v", i, etag, err)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					winners++
					winner = value
				} else if !errors.Is(err, ErrPrecondition) {
					t.Errorf("concurrent error: %v", err)
				}
			}(i)
		}
		wg.Wait()
		if winners != 1 {
			t.Fatalf("winners=%d", winners)
		}
		got, _, err := b.Get(ctx, key)
		if err != nil || !bytes.Equal(got, winner) {
			t.Fatalf("winner content mismatch: %v", err)
		}
		assertCOSAssetReads(t, b, key, winner)
	}
}

func assertCOSAssetReads(t *testing.T, b *ProviderBucket, key string, want []byte) {
	t.Helper()
	ctx := t.Context()
	size, tag, err := b.Stat(ctx, key)
	if err != nil || size != int64(len(want)) {
		t.Fatalf("stat size=%d err=%v", size, err)
	}
	reader, streamSize, streamTag, err := b.OpenReader(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || streamSize != size || streamTag != tag || !bytes.Equal(got, want) {
		t.Fatalf("stream mismatch: %v", err)
	}
	if len(want) > 0 {
		got, _, err = b.RangedGet(ctx, key, 0, 1)
		if err != nil || !bytes.Equal(got, want[:1]) {
			t.Fatalf("range mismatch: %v", err)
		}
	}
	tail, total, _, err := b.TailGet(ctx, key, 1)
	start := max(0, len(want)-1)
	if err != nil || total != size || !bytes.Equal(tail, want[start:]) {
		t.Fatalf("tail mismatch: %v", err)
	}
	if _, err := b.PresignGet(ctx, key, time.Minute); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("raw presign permitted: %v", err)
	}
	if _, err := b.Put(ctx, key, []byte("overwrite")); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("put permitted: %v", err)
	}
	if _, err := b.CAS(ctx, key, []byte("overwrite"), tag); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("CAS permitted: %v", err)
	}
	items, err := b.ListSizes(ctx, key)
	if err != nil || len(items) != 1 || items[0].Size != size {
		t.Fatalf("list sizes=%v err=%v", items, err)
	}
	items, _, err = b.ListPage(ctx, key, "", 10)
	if err != nil || len(items) != 1 || items[0].Size != size {
		t.Fatalf("page sizes=%v err=%v", items, err)
	}
}

func TestCOSAssetUploadLimitLive(t *testing.T) {
	if os.Getenv("CELLHIVE_NATIVE_TEST_PROVIDER") != "cos" {
		t.Skip("requires real COS")
	}
	b := openProviderForFenceTest(t)
	key := fmt.Sprintf("assets/release-probe/%d/limit/file.bin", time.Now().UnixNano())
	t.Cleanup(func() {
		if err := b.Delete(context.Background(), key); err != nil {
			t.Error(err)
		}
	})
	data := bytes.Repeat([]byte{0, 255, 1, 254}, 16<<20) // public upload limit: 64 MiB
	if _, err := b.ConditionalCreate(t.Context(), key, data); err != nil {
		t.Fatal(err)
	}
	got, _, err := b.Get(t.Context(), key)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("64MiB read mismatch: %v", err)
	}
	assertCOSAssetReads(t, b, key, data)
}
