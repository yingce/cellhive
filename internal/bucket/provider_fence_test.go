package bucket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// openProviderForFenceTest builds the same provider bucket used in cloud
// conformance tests. It is opt-in (CELLHIVE_NATIVE_TEST_PROVIDER=oss|cos).
func openProviderForFenceTest(t *testing.T) *ProviderBucket {
	t.Helper()
	kind := os.Getenv("CELLHIVE_NATIVE_TEST_PROVIDER")
	if kind == "" {
		t.Skip("requires CELLHIVE_NATIVE_TEST_PROVIDER=oss|cos")
	}
	var endpoint, name, access, secret string
	var appender PositionAppender
	var err error
	switch kind {
	case "oss":
		endpoint, name, access, secret = os.Getenv("OSS_ENDPOINT"), os.Getenv("OSS_BUCKET"), os.Getenv("OSS_ACCESS_KEY_ID"), os.Getenv("OSS_ACCESS_KEY_SECRET")
		appender, err = NewOSSAppender(endpoint, name, access, secret)
	case "cos":
		endpoint, name, access, secret = os.Getenv("COS_ENDPOINT"), os.Getenv("COS_BUCKET"), os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY")
		appender, err = NewCOSAppender(endpoint, access, secret)
	default:
		t.Fatalf("unsupported provider %q", kind)
	}
	if err != nil {
		t.Fatal(err)
	}
	if kind == "oss" && len(endpoint) >= 4 && endpoint[:4] != "http" {
		endpoint = "https://" + endpoint
	}
	region := ""
	if kind == "cos" {
		endpoint, region, err = COSServiceEndpoint(endpoint, name)
		if err != nil {
			t.Fatal(err)
		}
	}
	data, err := NewS3Bucket(context.Background(), S3Options{
		Endpoint: endpoint, Region: region, Bucket: name,
		AccessKey: access, SecretKey: secret, DisableOptionalChecksums: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewProviderBucket(data, appender)
}

// Removed sidecar formats must never be silently recreated by cloud adapters.
func TestProviderResourceFence(t *testing.T) {
	b := openProviderForFenceTest(t)
	for _, name := range []string{"resource-fence.json", "resource-fence.v2"} {
		key := fmt.Sprintf("cells/itest/__kv__/removed%d/%s", time.Now().UnixNano(), name)
		if _, err := b.ConditionalCreate(t.Context(), key, []byte(`{"deleted":true}`)); !errors.Is(err, ErrNotSupported) {
			t.Fatalf("removed %s create=%v", name, err)
		}
		if _, _, err := b.Get(t.Context(), key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("removed %s was published: %v", name, err)
		}
	}
}

// Historical node leases have no current-window object. Listing their logical
// keys must reuse the one physical listing instead of listing once per node.
func TestProviderHistoricalNodeListDoesNotAmplifyLists(t *testing.T) {
	ctx := t.Context()
	b := openProviderForFenceTest(t)
	current := time.Now().UTC().Truncate(time.Minute)
	b.authority = NewSegmentedAuthority(b.appender, b.S3Bucket, AuthorityOptions{
		Segment: time.Minute,
		Now:     func() time.Time { return current },
	})
	prefix := fmt.Sprintf("nodes/itest-list-%d-", time.Now().UnixNano())
	defer func() {
		keys, err := b.S3Bucket.List(ctx, prefix)
		if err != nil {
			t.Errorf("cleanup list: %v", err)
			return
		}
		for _, key := range keys {
			if err := b.S3Bucket.Delete(ctx, key); err != nil {
				t.Errorf("cleanup %s: %v", key, err)
			}
		}
	}()
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("%s%d.json", prefix, i)
		if _, err := b.ConditionalCreate(ctx, key, []byte(`{"expiry_ms":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	current = current.Add(3 * time.Minute)
	before := b.S3Bucket.Stats()["list"]
	keys, err := b.List(ctx, prefix)
	if err != nil || len(keys) != 3 {
		t.Fatalf("logical list = %v, %v", keys, err)
	}
	if delta := b.S3Bucket.Stats()["list"] - before; delta != 1 {
		t.Fatalf("three historical nodes used %d physical Lists, want one", delta)
	}
	if err := b.Delete(ctx, prefix+"1.json"); err != nil {
		t.Fatal(err)
	}
	before = b.S3Bucket.Stats()["list"]
	keys, err = b.List(ctx, prefix)
	if err != nil || len(keys) != 2 || keys[0] != prefix+"0.json" || keys[1] != prefix+"2.json" {
		t.Fatalf("list after tombstone = %v, %v", keys, err)
	}
	if delta := b.S3Bucket.Stats()["list"] - before; delta != 1 {
		t.Fatalf("tombstone list used %d physical Lists, want one", delta)
	}
}
