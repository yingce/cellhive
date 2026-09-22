package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestS3BucketIntegration checks the authority bucket contract against a real
// S3-compatible endpoint, including conditional delete for owner fencing.
// Gated by CELLHIVE_S3_TEST_ENDPOINT (e.g. local MinIO/rustfs).
func TestS3BucketIntegration(t *testing.T) {
	ep := os.Getenv("CELLHIVE_S3_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("set CELLHIVE_S3_TEST_ENDPOINT to run S3 integration")
	}
	ctx := context.Background()
	b, err := NewS3Bucket(ctx, S3Options{
		Endpoint: ep, Region: envOr("CELLHIVE_S3_TEST_REGION", "us-east-1"),
		AccessKey: envOr("CELLHIVE_S3_TEST_ACCESS", "minioadmin"),
		SecretKey: envOr("CELLHIVE_S3_TEST_SECRET", "minioadmin"),
		Bucket:    envOr("CELLHIVE_S3_TEST_BUCKET", "cellhive"), PathStyle: os.Getenv("CELLHIVE_S3_TEST_PATH_STYLE") != "false",
	})
	if err != nil {
		t.Fatalf("s3: %v", err)
	}
	key := fmt.Sprintf("itest/%s/%d/obj", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() { _ = b.Delete(context.Background(), key) })

	if _, err := b.ConditionalCreate(ctx, key, []byte("one")); err != nil {
		t.Fatalf("conditional create: %v", err)
	}
	if _, err := b.ConditionalCreate(ctx, key, []byte("two")); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("reject-create err = %v, want ErrPrecondition", err)
	}
	data, etag, err := b.Get(ctx, key)
	if err != nil || string(data) != "one" {
		t.Fatalf("get = %q, %v", data, err)
	}
	if _, err := b.CAS(ctx, key, []byte("three"), etag); err != nil {
		t.Fatalf("cas: %v", err)
	}
	if _, err := b.CAS(ctx, key, []byte("four"), etag); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("reject-stale err = %v, want ErrPrecondition", err)
	}
	got, _, err := b.RangedGet(ctx, key, 1, 3)
	if err != nil || string(got) != "hre" {
		t.Fatalf("ranged = %q, %v (want hre)", got, err)
	}
	if _, err := b.PresignGet(ctx, key, 60_000_000_000); err != nil {
		t.Fatalf("presign: %v", err)
	}
	// A store that ignores If-Match on DeleteObject cannot fence owner release.
	deleteKey := fmt.Sprintf("itest/%s/%d/conditional-delete", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() { _ = b.Delete(context.Background(), deleteKey) })
	deleteETag, err := b.ConditionalCreate(ctx, deleteKey, []byte("owner"))
	if err != nil {
		t.Fatalf("conditional delete setup: %v", err)
	}
	if err := b.ConditionalDelete(ctx, deleteKey, deleteETag+"-stale"); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale conditional delete err = %v, want ErrPrecondition", err)
	}
	if data, _, err := b.Get(ctx, deleteKey); err != nil || string(data) != "owner" {
		t.Fatalf("stale delete changed owner: data=%q err=%v", data, err)
	}
	if err := b.ConditionalDelete(ctx, deleteKey, deleteETag); err != nil {
		t.Fatalf("conditional delete: %v", err)
	}
	if _, _, err := b.Get(ctx, deleteKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conditional delete left owner behind: %v", err)
	}

	// Cursor paging (ADR-145): bounded pages from the S3 list response with
	// sizes, exclusive StartAfter cursor, no per-object GET.
	base := fmt.Sprintf("itest/%s/%d/page/", t.Name(), time.Now().UnixNano())
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if _, err := b.Put(ctx, base+k, []byte(k)); err != nil {
			t.Fatalf("put %s: %v", base+k, err)
		}
	}
	var keys []string
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("ListPage did not terminate")
		}
		items, next, err := b.ListPage(ctx, base, cursor, 2)
		if err != nil {
			t.Fatalf("list page: %v", err)
		}
		if len(items) > 2 {
			t.Fatalf("page returned %d items, want <= 2", len(items))
		}
		for _, it := range items {
			if it.Size != 1 || it.ETag == "" {
				t.Fatalf("item = %+v", it)
			}
			keys = append(keys, strings.TrimPrefix(it.Key, base))
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if strings.Join(keys, "") != "abcde" {
		t.Fatalf("paged keys = %v, want a..e", keys)
	}
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		_ = b.Delete(ctx, base+k)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var _ = bytes.Equal
