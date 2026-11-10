package bucket

import (
	"context"
	"strings"
	"testing"
)

type unsafeOwnerBucket struct {
	Bucket
	staleCAS bool
}

func (b unsafeOwnerBucket) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	if ownKey(key) && !b.staleCAS {
		return b.Bucket.Put(ctx, key, data)
	}
	return b.Bucket.ConditionalCreate(ctx, key, data)
}
func (b unsafeOwnerBucket) CAS(ctx context.Context, key string, data []byte, token string) (string, error) {
	if ownKey(key) && b.staleCAS {
		return b.Bucket.Put(ctx, key, data)
	}
	return b.Bucket.CAS(ctx, key, data, token)
}
func TestDiagnoseRejectsUnsafeOwnerConditions(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "cas"}[stale], func(t *testing.T) {
			b, err := NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			err = Diagnose(t.Context(), unsafeOwnerBucket{Bucket: b, staleCAS: stale})
			if err == nil || !strings.Contains(err.Error(), "owner") {
				t.Fatalf("unsafe owner semantics accepted: %v", err)
			}
		})
	}
}
