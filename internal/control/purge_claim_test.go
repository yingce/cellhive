package control

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPurgeClaimIdentityAndCompletion(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingPurges(ctx, 1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%v err=%v", jobs, err)
	}
	j, err := s.ClaimPurge(ctx, jobs[0], "holder-a")
	if err != nil {
		t.Fatal(err)
	}
	if j.JobID == "" || j.OpID == "" || j.Holder != "holder-a" {
		t.Fatalf("missing identity: %+v", j)
	}
	other := New(s.cs, nil)
	if _, err := other.ClaimPurge(ctx, jobs[0], "holder-b"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("second claim: %v", err)
	}
	wrong := j
	wrong.Holder = "holder-b"
	if err := other.CompletePurge(ctx, wrong); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("foreign completion: %v", err)
	}
	wrong = j
	wrong.OpID = "old-operation"
	if err := other.CompletePurge(ctx, wrong); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("foreign operation completion: %v", err)
	}
	if _, err := other.CreateApp(ctx, "acme", "test"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("recreate: %v", err)
	}
	if err := s.CompletePurge(ctx, j); err != nil {
		t.Fatal(err)
	}
	if _, err := other.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := other.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.CompletePurge(ctx, j); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("stale completion: %v", err)
	}
}

func TestPurgeClaimRejectsCancelledCandidate(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingPurges(ctx, 1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%v err=%v", jobs, err)
	}
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimPurge(ctx, jobs[0], "holder"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("stale snapshot claim: %v", err)
	}
}

func TestPurgeClaimUncertainCannotCompleteOrExpire(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingPurges(ctx, 1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%v err=%v", jobs, err)
	}
	j, err := s.ClaimPurge(ctx, jobs[0], "holder-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.HoldPurge(ctx, j, context.DeadlineExceeded); err != nil {
		t.Fatal(err)
	}
	other := New(s.cs, nil)
	other.now = func() time.Time { return time.Now().Add(365 * 24 * time.Hour) }
	if jobs, err := other.PendingPurges(ctx, 10); err != nil || len(jobs) != 0 {
		t.Fatalf("uncertain runnable: %v %v", jobs, err)
	}
	if _, err := other.ClaimPurge(ctx, j, "holder-a"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("same holder reclaim: %v", err)
	}
	if err := s.CompletePurge(ctx, j); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("uncertain completion: %v", err)
	}
	if err := s.PurgeAppRows(ctx, "acme"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("legacy rows bypass: %v", err)
	}
	if _, err := other.CreateApp(ctx, "acme", "test"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("uncertain recreation: %v", err)
	}
}

func TestPurgeClaimWorkerBlocksNamespaceAndCompletes(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha1"}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteWorker(ctx, "acme", "web", "test"); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingPurges(ctx, 1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%v err=%v", jobs, err)
	}
	j, err := s.ClaimPurge(ctx, jobs[0], "holder")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "acme", "test"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("overlapping app delete: %v", err)
	}
	if err := s.PurgeWorkerRows(ctx, "acme", "web"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("legacy rows bypass: %v", err)
	}
	if err := s.CompletePurge(ctx, j); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Deploy(ctx, "acme", "web", DeploySpec{BundleSHA: "sha2"}, "test"); err != nil {
		t.Fatal(err)
	}
}

func TestPurgeClaimMigratesLegacyPending(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	c, err := s.cell(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range []string{"job_id", "op_id", "holder"} {
		if _, err := c.DB.ExecContext(ctx, `ALTER TABLE purges DROP COLUMN `+col); err != nil {
			t.Fatal(err)
		}
	}
	other := New(s.cs, nil)
	jobs, err := other.PendingPurges(ctx, 1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("legacy jobs=%v err=%v", jobs, err)
	}
	if jobs[0].JobID != "" {
		t.Fatal("fixture not legacy")
	}
	j, err := other.ClaimPurge(ctx, jobs[0], "new-process")
	if err != nil {
		t.Fatal(err)
	}
	if j.JobID == "" || j.OpID == "" {
		t.Fatalf("legacy claim missing identity: %+v", j)
	}
	if err := other.CompletePurge(ctx, j); err != nil {
		t.Fatal(err)
	}
}
