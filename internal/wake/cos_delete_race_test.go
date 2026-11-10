package wake

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

func TestCOSWakeStaleDeletePreservesRepublication(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_TEST") != "1" {
		t.Skip("real COS opt-in")
	}
	name := os.Getenv("COS_BUCKET")
	if name != "cellhive-e2e-hk-1376795072" {
		t.Fatal("dedicated COS test bucket required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	ep, region, err := bucket.COSServiceEndpoint(os.Getenv("COS_ENDPOINT"), name)
	if err != nil {
		t.Fatal(err)
	}
	base, err := bucket.NewS3Bucket(ctx, bucket.S3Options{Endpoint: ep, Region: region, Bucket: name, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	appendClient, err := bucket.NewCOSAppender(os.Getenv("COS_ENDPOINT"), os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	provider := bucket.NewProviderBucket(base, appendClient)
	for _, target := range []string{"pointer", "due"} {
		t.Run(target, func(t *testing.T) {
			scope := fmt.Sprintf("itest-wake-%d/__kv__/%s", time.Now().UnixNano(), target)
			due := time.Now().Add(-time.Second).UnixMilli()
			fresh := New(provider)
			// Only these unique test keys are removed, after both writers have joined.
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				for _, k := range []string{ptrKey(scope), dueKey(due, scope)} {
					if err := provider.Delete(cleanup, k); err != nil {
						t.Error(err)
					}
				}
			}()
			if err := fresh.Put(ctx, scope, due, "", ""); err != nil {
				t.Fatal(err)
			}
			key := ptrKey(scope)
			if target == "due" {
				key = dueKey(due, scope)
			}
			paused := &pausedWakeDelete{Bucket: provider, key: key, entered: make(chan struct{}), release: make(chan struct{})}
			result := make(chan error, 1)
			go func() { result <- New(paused).Delete(ctx, scope) }()
			select {
			case <-paused.entered:
			case err := <-result:
				t.Fatalf("delete before pause: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := fresh.Put(ctx, scope, due, "", ""); err != nil {
				close(paused.release)
				<-result
				t.Fatal(err)
			}
			expected := map[string]string{}
			for _, k := range []string{ptrKey(scope), dueKey(due, scope)} {
				raw, _, err := provider.Get(ctx, k)
				if err != nil {
					close(paused.release)
					<-result
					t.Fatal(err)
				}
				expected[k] = string(raw)
			}
			close(paused.release)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			for key, want := range expected {
				raw, _, err := provider.Get(ctx, key)
				if err != nil || string(raw) != want {
					t.Fatalf("successor lost for %s: %v", target, err)
				}
			}
			t.Logf("real COS stale %s deletion rejected; identical timer republication preserved", target)
		})
	}
}
