package replica

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/objectstore"
)

// Opt-in real COS migration test. The same fixed-position appender and
// segmented legacy authority used by cell-agent are exercised together.
func TestResourceFenceMigratesCOSLegacyFormats(t *testing.T) {
	if os.Getenv("CELLHIVE_NATIVE_TEST_PROVIDER") != "cos" {
		t.Skip("requires CELLHIVE_NATIVE_TEST_PROVIDER=cos")
	}
	ctx := context.Background()
	endpoint := os.Getenv("COS_ENDPOINT")
	name := os.Getenv("COS_BUCKET")
	access := os.Getenv("COS_SECRET_ID")
	secret := os.Getenv("COS_SECRET_KEY")
	appender, err := bucket.NewCOSAppender(endpoint, access, secret)
	if err != nil {
		t.Fatal(err)
	}
	service, region, err := bucket.COSServiceEndpoint(endpoint, name)
	if err != nil {
		t.Fatal(err)
	}
	data, err := bucket.NewS3Bucket(ctx, bucket.S3Options{
		Endpoint: service, Region: region, Bucket: name, AccessKey: access,
		SecretKey: secret, DisableOptionalChecksums: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	b := bucket.NewProviderBucket(data, appender)
	objects := objectstore.NewOwned(b, objectstore.PrefixCells, objectstore.OwnerReplica)
	for _, format := range []string{"segmented", "plain"} {
		t.Run(format, func(t *testing.T) {
			sc := cell.Scope{Namespace: fmt.Sprintf("itest-fence-%d", time.Now().UnixNano()), Class: "__kv__", ID: "main"}
			oldKey, newKey := legacyResourceFenceKey(sc), resourceFenceKey(sc)
			t.Cleanup(func() {
				_ = objects.Delete(context.Background(), newKey)
				_ = objects.Delete(context.Background(), oldKey)
			})
			old := []byte(`{"job_id":"old-job","deleted":true,"epoch_floor":7}`)
			if format == "segmented" {
				_, err = objects.ConditionalCreate(ctx, oldKey, old)
			} else {
				_, err = data.Put(ctx, oldKey, old) // simulate pre-ADR-270 plain JSON
			}
			if err != nil {
				t.Fatal(err)
			}
			m := New(b)
			blocked, floor, err := m.ResourceState(ctx, sc)
			if err != nil || !blocked || floor != 7 {
				t.Fatalf("migrated state: blocked=%v floor=%d err=%v", blocked, floor, err)
			}
			if _, err := objects.Get(ctx, newKey); err != nil {
				t.Fatalf("point record missing: %v", err)
			}
			if err := m.AllowResource(ctx, sc, "old-job"); err != nil {
				t.Fatal(err)
			}
			if release, err := New(b).resourceWriteGuard(ctx, sc, 7); release != nil || !errors.Is(err, ErrResourceDeleted) {
				t.Fatalf("old epoch passed migrated fence: release=%v err=%v", release != nil, err)
			}
		})
	}
}
