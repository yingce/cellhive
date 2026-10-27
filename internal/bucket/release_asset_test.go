package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	key := prefix + "concurrent"
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
			_, err := b.ConditionalCreate(ctx, key, value)
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
}
