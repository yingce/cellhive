package control

import (
	"context"
	"testing"

	"cellhive/internal/artifacts"
	"cellhive/internal/bucket"
)

func TestArtifactConflictReleasesControlReservation(t *testing.T) {
	s := newStore(t, false)
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a := artifacts.New(b)
	a.Reserve = func(ctx context.Context, kind, id, path, mode string) (artifacts.Finish, error) {
		op, err := s.BeginArtifact(ctx, kind, id, path, mode, "writer")
		return func(ctx context.Context, sha string, size int64, success bool) error {
			return s.FinishArtifact(ctx, op, sha, size, success)
		}, err
	}
	if _, err := a.PutAssetAt(t.Context(), "ns", "w", "token", "a", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.PutAssetAt(t.Context(), "ns", "w", "token", "a", []byte("new")); err == nil {
		t.Fatal("conflict accepted")
	}
	if _, err := a.PutAssetAt(t.Context(), "ns", "w", "token", "a", []byte("old")); err != nil {
		t.Fatalf("conflict leaked reservation: %v", err)
	}
	if _, ok, err := s.ClaimArtifactGC(t.Context(), "asset", "ns/w/token", "gc", 0); err != nil || !ok {
		t.Fatalf("GC after conflict: %v %v", ok, err)
	}
}
