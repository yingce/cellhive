package control

import (
	"context"
	"testing"
	"time"
)

// TestDeleteLockBlocksDeploy covers ADR-107: a worker delete lock serializes
// delete against deploy, and expires so a crashed delete cannot block forever.
func TestDeleteLockBlocksDeploy(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("app: %v", err)
	}
	held, err := s.AcquireDeleteLock(ctx, "acme", "w", "admin", time.Minute)
	if err != nil || held {
		t.Fatalf("acquire = %v, %v; want acquired", held, err)
	}
	held2, err := s.AcquireDeleteLock(ctx, "acme", "w", "admin2", time.Minute)
	if err != nil || !held2 {
		t.Fatalf("second acquire = %v, %v; want held", held2, err)
	}
	if _, err := s.Deploy(ctx, "acme", "w", DeploySpec{BundleSHA: "sha"}, "test"); err == nil {
		t.Fatalf("deploy succeeded while a delete lock is held")
	}
	if err := s.ReleaseDeleteLock(ctx, "acme", "w"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "w", DeploySpec{BundleSHA: "sha"}, "test"); err != nil {
		t.Fatalf("deploy after release: %v", err)
	}

	// An expired lock does not block.
	if _, err := s.AcquireDeleteLock(ctx, "acme", "w2", "admin", -time.Second); err != nil {
		t.Fatalf("acquire expired: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "w2", DeploySpec{BundleSHA: "sha"}, "test"); err != nil {
		t.Fatalf("deploy with an expired lock: %v", err)
	}
}

// TestVersionRecordsSessionPolicy verifies the deploy records the session policy
// for audit/rollout (ADR-107).
func TestVersionRecordsSessionPolicy(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("app: %v", err)
	}
	v, err := s.Deploy(ctx, "acme", "w", DeploySpec{BundleSHA: "sha", SessionPolicy: "preserve"}, "test")
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if v.SessionPolicy != "preserve" {
		t.Fatalf("session policy = %q, want preserve", v.SessionPolicy)
	}
}
