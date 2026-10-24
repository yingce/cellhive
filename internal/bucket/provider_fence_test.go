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

func TestProviderResourceFence(t *testing.T) {
	ctx := context.Background()
	b := openProviderForFenceTest(t)
	id := time.Now().UnixNano()
	key := fmt.Sprintf("cells/itest/__kv__/fence%d/resource-fence.json", id)
	legacyKey := fmt.Sprintf("cells/itest/__kv__/fencelegacy%d/resource-fence.json", id)
	defer b.Delete(ctx, key)
	defer b.Delete(ctx, legacyKey)

	one := []byte(`{"job_id":"one","deleted":true}`)
	token, err := b.ConditionalCreate(ctx, key, one)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.ConditionalCreate(ctx, key, []byte(`{"job_id":"two"}`)); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("second create = %v, want precondition", err)
	}
	got, gotToken, err := b.Get(ctx, key)
	if err != nil || string(got) != string(one) || gotToken != token {
		t.Fatalf("get = %q token=%q err=%v, want %q token=%q", got, gotToken, err, one, token)
	}

	updated := []byte(`{"job_id":"one","deleted":false}`)
	token2, err := b.CAS(ctx, key, updated, token)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err = b.Get(ctx, key)
	if err != nil || string(got) != string(updated) || token2 == token {
		t.Fatalf("after CAS = %q err=%v token=%q, want %q", got, err, token2, updated)
	}

	// A plain JSON fence written by the earlier hotfix must remain readable and
	// migrate into the append authority on its first CAS.
	legacy := []byte(`{"job_id":"old","deleted":true}`)
	if _, err := b.S3Bucket.Put(ctx, legacyKey, legacy); err != nil {
		t.Fatal(err)
	}
	got, legacyToken, err := b.Get(ctx, legacyKey)
	if err != nil || string(got) != string(legacy) || legacyToken == "" {
		t.Fatalf("legacy get = %q token=%q err=%v", got, legacyToken, err)
	}
	if _, err := b.ConditionalCreate(ctx, legacyKey, []byte(`{"job_id":"new"}`)); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("legacy create = %v, want precondition", err)
	}
	if _, err := b.CAS(ctx, legacyKey, []byte(`{"job_id":"old","deleted":false}`), legacyToken); err != nil {
		t.Fatalf("legacy CAS = %v", err)
	}
	got, _, err = b.Get(ctx, legacyKey)
	if err != nil || string(got) != `{"job_id":"old","deleted":false}` {
		t.Fatalf("legacy after CAS = %q err=%v", got, err)
	}
}

func TestProviderPointResourceFence(t *testing.T) {
	ctx := context.Background()
	b := openProviderForFenceTest(t)
	key := fmt.Sprintf("cells/itest/__kv__/point%d/resource-fence.v2", time.Now().UnixNano())
	defer b.Delete(ctx, key)
	if _, _, err := b.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing point fence = %v", err)
	}
	first := []byte(`{"job_id":"old","deleted":true,"epoch_floor":7}`)
	token, err := b.ConditionalCreate(ctx, key, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.ConditionalCreate(ctx, key, []byte(`{}`)); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("duplicate point fence create = %v", err)
	}
	for i := 0; i < 5; i++ {
		got, tkn, err := b.Get(ctx, key)
		if err != nil || string(got) != string(first) || tkn != token {
			t.Fatalf("read %d: data=%q token=%q err=%v", i, got, tkn, err)
		}
	}
	if _, err := b.CAS(ctx, key, []byte(`{"deleted":false}`), token+"0"); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale CAS = %v", err)
	}
	next, err := b.CAS(ctx, key, []byte(`{"deleted":false}`), token)
	if err != nil || next == token {
		t.Fatalf("CAS token=%q err=%v", next, err)
	}
	if got, _, err := b.Get(ctx, key); err != nil || string(got) != `{"deleted":false}` {
		t.Fatalf("new value=%q err=%v", got, err)
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
