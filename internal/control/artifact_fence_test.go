package control

import (
	"context"
	"errors"
	"testing"
)

func TestArtifactClaimExcludesUploadAndLateDeploy(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetGCMark(ctx, "bundle", "sha1", 0); err != nil {
		t.Fatal(err)
	}
	up, err := s.BeginArtifact(ctx, "bundle", "sha1", "", "upload", "writer")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.ClaimArtifactGC(ctx, "bundle", "sha1", "gc", 0); err != nil || ok {
		t.Fatalf("GC during upload: %v %v", ok, err)
	}
	if err := s.FinishArtifact(ctx, up, "sha1", 5, true); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := s.ClaimArtifactGC(ctx, "bundle", "sha1", "gc", 0)
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	for _, stage := range []bool{false, true} {
		if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha1", StageOnly: stage}, "test"); !errors.Is(err, ErrArtifactUnavailable) {
			t.Fatalf("publish during delete: %v", err)
		}
	}
	if _, err := s.BeginArtifact(ctx, "bundle", "sha1", "", "upload", "writer"); !errors.Is(err, ErrArtifactUnavailable) {
		t.Fatalf("same SHA upload during delete: %v", err)
	}
	if err := s.FinishArtifactGC(ctx, claim, nil); err != nil {
		t.Fatal(err)
	}
	// A caller that completed its blob precheck before GC must still fail here.
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha1"}, "test"); !errors.Is(err, ErrArtifactUnavailable) {
		t.Fatalf("late deploy after deletion: %v", err)
	}
	up, err = s.BeginArtifact(ctx, "bundle", "sha1", "", "upload", "writer")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FinishArtifact(ctx, up, "sha1", 5, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha1"}, "test"); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactUnknownAndPurgeOverlap(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	up, err := s.BeginArtifact(ctx, "asset", "acme/web/token", "a.txt", "upload", "writer")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingPurges(ctx, 1)
	if err != nil || len(jobs) != 1 {
		t.Fatal(err)
	}
	if _, err := s.ClaimPurge(ctx, jobs[0], "purger"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("purge overlaps upload: %v", err)
	}
	if err := s.FinishArtifact(ctx, up, "hash", 1, true); err != nil {
		t.Fatal(err)
	}
	j, err := s.ClaimPurge(ctx, jobs[0], "purger")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginArtifact(ctx, "asset", "acme/web/token", "a.txt", "upload", "writer"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("upload overlaps purge: %v", err)
	}
	if err := s.CompletePurge(ctx, j); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "code", AssetsSHA: "token"}, "test"); !errors.Is(err, ErrArtifactUnavailable) {
		t.Fatalf("purged token published: %v", err)
	}
	up, err = s.BeginArtifact(ctx, "asset", "acme/web/token", "a.txt", "upload", "writer")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FinishArtifact(ctx, up, "hash", 1, true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "code", AssetsSHA: "token"}, "test"); !errors.Is(err, ErrArtifactUnavailable) {
		t.Fatalf("single put revived token: %v", err)
	}
}
