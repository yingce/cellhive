package bucket

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestCOSDataContract complements the production authority/immutable probes:
// ordinary data reads, actual presigned HTTP GET and bounded cursor paging.
func TestCOSDataContract(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_TEST") != "1" {
		t.Skip("set CELLHIVE_COS_TEST=1 and COS_*")
	}
	name := os.Getenv("COS_BUCKET")
	ep, region, err := COSServiceEndpoint(os.Getenv("COS_ENDPOINT"), name)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewS3Bucket(t.Context(), S3Options{Endpoint: ep, Region: region, Bucket: name, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("itest/cos-data-%d/", time.Now().UnixNano())
	keys := []string{"a", "b", "c", "d", "e"}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for _, key := range keys {
			if err := b.Delete(ctx, prefix+key); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}
	})
	for _, key := range keys {
		if _, err := b.Put(t.Context(), prefix+key, []byte("value-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	data, _, err := b.Get(t.Context(), prefix+"a")
	if err != nil || string(data) != "value-a" {
		t.Fatalf("get=%q err=%v", data, err)
	}
	data, _, err = b.RangedGet(t.Context(), prefix+"a", 1, 3)
	if err != nil || string(data) != "alu" {
		t.Fatalf("range=%q err=%v", data, err)
	}
	url, err := b.PresignGet(t.Context(), prefix+"a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	client := http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal("presigned GET transport failed")
	}
	defer resp.Body.Close()
	data, err = io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil || resp.StatusCode != 200 || string(data) != "value-a" {
		t.Fatalf("presigned GET status=%d bytes=%d err=%v", resp.StatusCode, len(data), err)
	}
	var got []string
	cursor := ""
	for page := 0; ; page++ {
		if page >= 5 {
			t.Fatal("pagination did not terminate")
		}
		items, next, err := b.ListPage(t.Context(), prefix, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) > 2 {
			t.Fatal("page exceeds bound")
		}
		for _, it := range items {
			if it.Size != 7 || it.ETag == "" {
				t.Fatalf("invalid item %+v", it)
			}
			got = append(got, strings.TrimPrefix(it.Key, prefix))
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if strings.Join(got, "") != "abcde" {
		t.Fatalf("paged keys=%v", got)
	}
	t.Log("GET, range, signed HTTP GET and 2-item cursor pages verified")
}
