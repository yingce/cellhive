package artifacts

import (
	"context"
	"errors"
	"testing"

	"cellhive/internal/bucket"
)

type sealReviewBucket struct {
	bucket.Bucket
	writeErr error
}

func (b sealReviewBucket) ConditionalCreate(context.Context, string, []byte) (string, error) {
	return "", b.writeErr
}

func TestSealReviewUploadReservationOutcome(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(map[bool]string{false: "precondition-no-write", true: "unknown-write"}[uncertain], func(t *testing.T) {
			s := newStore(t)
			if _, err := s.PutAssetAt(t.Context(), "ns", "w", "t", "a", []byte("old")); err != nil {
				t.Fatal(err)
			}
			writeErr := bucket.ErrPrecondition
			if uncertain {
				writeErr = errors.New("unknown write outcome")
			}
			s.B = sealReviewBucket{Bucket: s.B, writeErr: writeErr}
			calls := 0
			s.Reserve = func(context.Context, string, string, string, string) (Finish, error) {
				return func(_ context.Context, _ string, _ int64, success bool) error {
					calls++
					if success {
						t.Fatal("conflicting upload marked successful")
					}
					return nil
				}, nil
			}
			if _, err := s.PutAssetAt(t.Context(), "ns", "w", "t", "a", []byte("new")); err == nil {
				t.Fatal("upload unexpectedly succeeded")
			}
			want := 1
			if uncertain {
				want = 0
			}
			if calls != want {
				t.Fatalf("finish calls=%d want %d", calls, want)
			}
		})
	}
}
