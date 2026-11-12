package bucket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// TestS3ConcurrentCASIntegration tests the provider's actual conditional PUT
// semantics, both directly and through the production control-record adapter.
func TestS3ConcurrentCASIntegration(t *testing.T) {
	endpoint := os.Getenv("CELLHIVE_S3_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("real S3 opt-in")
	}
	raw, err := NewS3Bucket(t.Context(), S3Options{Endpoint: endpoint, Region: envOr("CELLHIVE_S3_TEST_REGION", "us-east-1"), Bucket: envOr("CELLHIVE_S3_TEST_BUCKET", "cellhive"), AccessKey: envOr("CELLHIVE_S3_TEST_ACCESS", "minioadmin"), SecretKey: envOr("CELLHIVE_S3_TEST_SECRET", "minioadmin"), PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"raw", "control"} {
		t.Run(mode, func(t *testing.T) {
			var b Bucket = raw
			if mode == "control" {
				b = NewS3ControlBucket(raw)
			}
			key := fmt.Sprintf("nodes/cas-itest-%d-%s.json", time.Now().UnixNano(), mode)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := raw.Delete(ctx, key); err != nil {
					t.Error(err)
				}
			})
			for round := 0; round < 21; round++ {
				var token string
				if round > 0 {
					_, token, err = b.Get(t.Context(), key)
					if err != nil {
						t.Fatal(err)
					}
				}
				type result struct {
					value []byte
					err   error
				}
				results := make(chan result, 32)
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := 0; i < 32; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						<-start
						value := []byte(fmt.Sprintf("round=%d writer=%d", round, i))
						var e error
						if round == 0 {
							_, e = b.ConditionalCreate(t.Context(), key, value)
						} else {
							_, e = b.CAS(t.Context(), key, value, token)
						}
						results <- result{value, e}
					}(i)
				}
				close(start)
				wg.Wait()
				close(results)
				wins := 0
				var winner []byte
				for r := range results {
					if r.err == nil {
						wins++
						winner = r.value
					} else if !errors.Is(r.err, ErrPrecondition) {
						t.Fatalf("round %d unexpected error: %v", round, r.err)
					}
				}
				if wins != 1 {
					t.Fatalf("round %d winners=%d, want 1", round, wins)
				}
				actual, _, e := b.Get(t.Context(), key)
				if e != nil || !bytes.Equal(actual, winner) {
					t.Fatalf("round %d winner readback mismatch: %v", round, e)
				}
				if round > 0 {
					if _, e := b.CAS(t.Context(), key, []byte("stale"), token); !errors.Is(e, ErrPrecondition) {
						t.Fatalf("stale CAS accepted: %v", e)
					}
				}
			}
			t.Log("32 concurrent creators: exactly 1 winner; 20 rounds x 32 same-ETag writers: exactly 1 winner per round; readback and stale-token rejection verified")
		})
	}
}
