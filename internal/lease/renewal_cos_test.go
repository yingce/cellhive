package lease

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

// Real provider with deterministic ambiguous-result injection. These tests do
// not claim to reproduce the original network latency; they exercise its unsafe
// completion ordering against native COS append and generation rollover.
func TestCOSRenewalDelayedCommit(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_TEST") != "1" {
		t.Skip("real COS opt-in")
	}
	name := os.Getenv("COS_BUCKET")
	if name != "cellhive-e2e-hk-1376795072" {
		t.Fatal("dedicated bucket required")
	}
	endpoint := os.Getenv("COS_ENDPOINT")
	ep, region, err := bucket.COSServiceEndpoint(endpoint, name)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	base, err := bucket.NewS3Bucket(ctx, bucket.S3Options{Endpoint: ep, Region: region, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), Bucket: name, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	appender, err := bucket.NewCOSAppender(endpoint, os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	for _, initial := range []bool{false, true} {
		for _, rollover := range []bool{false, true} {
			t.Run(fmt.Sprintf("initial=%v/rollover=%v", initial, rollover), func(t *testing.T) {
				clock := time.Unix(1700000000, 0)
				provider := bucket.NewProviderBucket(base, appender, bucket.AuthorityOptions{Now: func() time.Time { return clock }})
				node := fmt.Sprintf("itest-lease-%d", time.Now().UnixNano())
				key := Key(node)
				t.Cleanup(func() {
					clean, done := context.WithTimeout(context.Background(), 30*time.Second)
					defer done()
					// Only this run's known keys, after all synchronous test writes finish.
					for _, k := range []string{key + ".g0", key + ".g1", key} {
						if err := base.Delete(clean, k); err != nil {
							t.Error(err)
						}
					}
				})
				m := NewManager(provider, node, "test-session", "", "", 60*time.Second)
				if !initial {
					if err := m.Publish(ctx, Load{}); err != nil {
						t.Fatal(err)
					}
				}
				fault := &ambiguousLeaseWrite{Bucket: provider, commitOnRetry: true}
				m.B = fault
				for i := 1; i <= 2; i++ {
					if err := m.Publish(ctx, Load{OwnedCells: i}); err == nil {
						t.Fatal("expected injected timeout")
					}
				}
				if rollover {
					clock = clock.Add(time.Hour)
				}
				if err := m.Publish(ctx, Load{OwnedCells: 99}); err != nil {
					t.Fatal(err)
				}
				if fault.changed || m.Fenced() || m.pending != nil {
					t.Fatal("lost exact pending operation")
				}
				data, _, err := provider.Get(ctx, key)
				if err != nil || !bytes.Equal(data, fault.data) {
					t.Fatal("different confirmed data", err)
				}
				m.B = provider
				if err := m.Publish(ctx, Load{OwnedCells: 100}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
