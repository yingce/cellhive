package bucket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Verify the exact provider path used by own.json, not the append lease path.
func TestCOSOwnerConditions(t *testing.T) {
	if os.Getenv("CELLHIVE_COS_TEST") != "1" {
		t.Skip("real COS opt-in")
	}
	name := os.Getenv("COS_BUCKET")
	if name != "cellhive-e2e-hk-1376795072" {
		t.Fatal("dedicated bucket required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	endpoint := os.Getenv("COS_ENDPOINT")
	ep, region, err := COSServiceEndpoint(endpoint, name)
	if err != nil {
		t.Fatal(err)
	}
	base, err := NewS3Bucket(ctx, S3Options{Endpoint: ep, Region: region, AccessKey: os.Getenv("COS_SECRET_ID"), SecretKey: os.Getenv("COS_SECRET_KEY"), Bucket: name, DisableOptionalChecksums: true})
	if err != nil {
		t.Fatal(err)
	}
	appender, err := NewCOSAppender(endpoint, os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	b := NewProviderBucket(base, appender)
	t.Run("startup", func(t *testing.T) {
		if err := Diagnose(ctx, b); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("concurrent-claim", func(t *testing.T) {
		key := fmt.Sprintf("cells/itest-cos/__owner_probe__/%d-race/own.json", time.Now().UnixNano())
		t.Cleanup(func() {
			if err := b.Delete(context.Background(), key); err != nil {
				t.Error(err)
			}
		})
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, err := b.ConditionalCreate(ctx, key, []byte(fmt.Sprintf("node-%d", i)))
				if err == nil {
					wins.Add(1)
				} else if !errors.Is(err, ErrPrecondition) {
					t.Errorf("claim: %v", err)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("claim winners=%d want1", wins.Load())
		}
	})
	for _, kind := range []string{"create", "stale-cas", "valid-cas"} {
		t.Run(kind, func(t *testing.T) {
			key := fmt.Sprintf("cells/itest-cos/__owner_probe__/%d-%s/own.json", time.Now().UnixNano(), kind)
			t.Cleanup(func() {
				if err := b.Delete(context.Background(), key); err != nil {
					t.Error(err)
				}
			})
			token, err := b.ConditionalCreate(ctx, key, []byte("first"))
			if err != nil {
				t.Fatal(err)
			}
			var writeErr error
			if kind == "create" {
				_, writeErr = b.ConditionalCreate(ctx, key, []byte("second"))
			} else if kind == "stale-cas" {
				_, writeErr = b.CAS(ctx, key, []byte("second"), token+"-stale")
			} else {
				_, writeErr = b.CAS(ctx, key, []byte("second"), token)
			}
			want := "first"
			if kind == "valid-cas" {
				want = "second"
				if writeErr != nil {
					t.Error(writeErr)
				}
			} else if !errors.Is(writeErr, ErrPrecondition) {
				t.Errorf("unsafe conditional write: err=%v", writeErr)
			}
			data, _, err := b.Get(ctx, key)
			if err != nil || string(data) != want {
				t.Errorf("data=%q want=%q err=%v", data, want, err)
			}
		})
	}
}
