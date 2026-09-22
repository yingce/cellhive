package owner

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

// Run against an isolated OSS bucket with CELLHIVE_NATIVE_TEST_PROVIDER=oss.
func TestOSSOwnerTakeover(t *testing.T) {
	if os.Getenv("CELLHIVE_NATIVE_TEST_PROVIDER") != "oss" {
		t.Skip("requires OSS credentials")
	}
	endpoint := os.Getenv("OSS_ENDPOINT")
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	name, access, secret := os.Getenv("OSS_BUCKET"), os.Getenv("OSS_ACCESS_KEY_ID"), os.Getenv("OSS_ACCESS_KEY_SECRET")
	appender, err := bucket.NewOSSAppender(endpoint, name, access, secret)
	if err != nil {
		t.Fatal(err)
	}
	data, err := bucket.NewS3Bucket(context.Background(), bucket.S3Options{Endpoint: endpoint, Bucket: name, AccessKey: access, SecretKey: secret, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	b := bucket.NewProviderBucket(data, appender)
	sc := cell.Scope{Namespace: "itest", Class: "__kv__", ID: time.Now().UTC().Format("20060102T150405.000000000")}
	first := &Manager{B: b, NodeID: "itest-a", Session: "a", Advertise: "127.0.0.1:7000", Role: cell.RoleCellAgent, OwnerTTL: time.Second}
	second := &Manager{B: b, NodeID: "itest-b", Session: "b", Advertise: "127.0.0.1:7001", Role: cell.RoleCellAgent, OwnerTTL: time.Second}
	ctx := context.Background()
	old, err := first.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = second.Claim(ctx, sc, time.Now()); !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("live owner takeover: %v", err)
	}
	later := time.UnixMilli(old.Expiry).Add(time.Second)
	next, err := second.Claim(ctx, sc, later)
	if err != nil {
		t.Fatal(err)
	}
	if next.Epoch <= old.Epoch || next.Node != second.NodeID {
		t.Fatalf("takeover = %+v, previous = %+v", next, old)
	}
	if err := first.Release(ctx, sc, old.Epoch); !errors.Is(err, ErrEpochMismatch) {
		t.Fatalf("stale owner release: %v", err)
	}
	current, _, err := second.Resolve(ctx, sc)
	if err != nil || current.Epoch != next.Epoch || current.Node != second.NodeID {
		t.Fatalf("winner lost: %+v, %v", current, err)
	}
}
