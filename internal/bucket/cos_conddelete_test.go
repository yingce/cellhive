package bucket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestCOSConditionalDelete checks the two delete paths CellHive uses:
//   - authority keys (owner/lease) go through the COS native-append authority;
//   - other keys use the S3 If-Match delete path.
//
// Opt-in: CELLHIVE_COS_TEST=1 plus COS_ENDPOINT/COS_BUCKET/COS_SECRET_*.
func TestCOSConditionalDelete(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_TEST") != "1" {
		t.Skip("set CELLHIVE_COS_TEST=1 and COS_*")
	}
	endpoint, name := os.Getenv("COS_ENDPOINT"), os.Getenv("COS_BUCKET")
	access, secret := os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY")
	if endpoint == "" || name == "" || access == "" || secret == "" {
		t.Fatal("COS endpoint/bucket/credentials required")
	}
	ctx := context.Background()
	appender, err := NewCOSAppender(endpoint, access, secret)
	if err != nil {
		t.Fatal(err)
	}
	ep, region, err := COSServiceEndpoint(endpoint, name)
	if err != nil {
		t.Fatal(err)
	}
	base, err := NewS3Bucket(ctx, S3Options{Endpoint: ep, Region: region, AccessKey: access, SecretKey: secret, Bucket: name, PathStyle: false, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	b := NewProviderBucket(base, appender)
	stamp := time.Now().UnixNano()

	// Authority path: owner/lease fencing must reject a stale token.
	authorityKey := fmt.Sprintf("cells/itest-cos/__test__/%d/owner.json", stamp)
	defer b.Delete(context.Background(), authorityKey)
	token, err := b.ConditionalCreate(ctx, authorityKey, []byte("owner"))
	if err != nil {
		t.Fatalf("authority create: %v", err)
	}
	if err := b.ConditionalDelete(ctx, authorityKey, token+"-stale"); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("authority stale delete err=%v, want ErrPrecondition", err)
	}
	if data, _, err := b.Get(ctx, authorityKey); err != nil || string(data) != "owner" {
		t.Fatalf("authority stale delete changed object: data=%q err=%v", data, err)
	}
	if err := b.ConditionalDelete(ctx, authorityKey, token); err != nil {
		t.Fatalf("authority current delete: %v", err)
	}
	t.Logf("authority conditional delete: stale rejected, current accepted")

	// S3 If-Match path used for non-authority objects.
	s3Key := fmt.Sprintf("itest/%s/%d/conditional-delete", t.Name(), stamp)
	defer base.Delete(context.Background(), s3Key)
	etag, err := base.ConditionalCreate(ctx, s3Key, []byte("owner"))
	if err != nil {
		t.Fatalf("s3 create: %v", err)
	}
	if err := base.ConditionalDelete(ctx, s3Key, etag+"-stale"); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("s3 stale delete err=%v, want ErrPrecondition", err)
	}
	if data, _, err := base.Get(ctx, s3Key); err != nil || string(data) != "owner" {
		t.Fatalf("s3 stale delete changed object: data=%q err=%v", data, err)
	}
	if err := base.ConditionalDelete(ctx, s3Key, etag); err != nil {
		t.Fatalf("s3 current delete: %v", err)
	}
	t.Logf("s3 conditional delete: stale rejected, current accepted")
}
