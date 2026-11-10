package replica

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/objectstore"
)

// Opt-in real COS owner-fence lifecycle and concurrent floor updates.
// No legacy object formats or migration compatibility are exercised.
func TestResourceFenceCOSNativeLifecycle(t *testing.T) {
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
	sc := cell.Scope{Namespace: fmt.Sprintf("itest-fence-%d", time.Now().UnixNano()), Class: "__kv__", ID: "main"}
	t.Cleanup(func() {
		if err := objects.Delete(context.Background(), resourceFenceKey(sc)); err != nil {
			t.Errorf("cleanup owner: %v", err)
		}
	})
	owner := cell.Owner{Node: "node", Session: "session", Epoch: 12}
	raw, err := cell.MarshalOwner(owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = objects.ConditionalCreate(ctx, resourceFenceKey(sc), raw); err != nil {
		t.Fatal(err)
	}
	m := New(b)
	if err := m.BlockResource(ctx, sc, "job"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for floor := uint64(1); floor <= 8; floor++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			failures <- New(b).FenceResource(ctx, sc, "job", floor)
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := m.AllowResource(ctx, sc, "job"); err != nil {
		t.Fatal(err)
	}
	if err := New(b).BlockResource(ctx, sc, "job"); !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("stale cleanup accepted: %v", err)
	}
	blocked, floor, err := New(b).ResourceState(ctx, sc)
	if err != nil || blocked || floor != 8 {
		t.Fatalf("native state: blocked=%v floor=%d err=%v", blocked, floor, err)
	}
	for _, epoch := range []uint64{8, 9} {
		release, err := New(b).resourceWriteGuard(ctx, sc, epoch)
		if epoch == 8 {
			if release != nil || !errors.Is(err, ErrResourceDeleted) {
				t.Fatalf("old epoch passed fence: release=%v err=%v", release != nil, err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			release()
		}
	}
	got, err := objects.Get(ctx, resourceFenceKey(sc))
	if err != nil {
		t.Fatal(err)
	}
	final, err := cell.UnmarshalOwner(got)
	if err != nil || final.Node != owner.Node || final.Session != owner.Session || final.Epoch != owner.Epoch {
		t.Fatalf("owner identity changed: %+v err=%v", final, err)
	}
}

func TestResourceRetiredSweepS3Integration(t *testing.T) {
	ep := os.Getenv("CELLHIVE_S3_TEST_ENDPOINT")
	if ep == "" {
		t.Skip("real S3 opt-in")
	}
	raw, err := bucket.NewS3Bucket(t.Context(), bucket.S3Options{Endpoint: ep, Region: "us-east-1", Bucket: os.Getenv("CELLHIVE_S3_TEST_BUCKET"), AccessKey: os.Getenv("CELLHIVE_S3_TEST_ACCESS"), SecretKey: os.Getenv("CELLHIVE_S3_TEST_SECRET"), PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: fmt.Sprintf("review-retire-%d", time.Now().UnixNano()), Class: "__kv__", ID: "sweep"}
	defer func() {
		keys, err := raw.List(context.Background(), sc.Key()+"/")
		if err != nil {
			t.Error(err)
			return
		}
		for _, k := range keys {
			if err := raw.Delete(context.Background(), k); err != nil {
				t.Error(err)
			}
		}
	}()
	exerciseRetiredSweep(t, New(bucket.NewS3ControlBucket(raw)), sc)
}
