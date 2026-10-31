package control

import (
	"cellhive/internal/cell"
	"context"
	"errors"
	"testing"
	"time"
)

func TestResourceDeletionBlocksRecreationUntilCleanup(t *testing.T) {
	s := newStore(t, false)
	ctx := t.Context()
	if _, err := s.CreateResource(ctx, "acme", "kv", "CACHE", "acme/__kv__/physical", "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteResource(ctx, "acme", "kv", "CACHE", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateResource(ctx, "acme", "kv", "CACHE", "acme/__kv__/physical", "ops"); err == nil {
		t.Fatal("recreated resource while cleanup is pending")
	}
}

func TestResourceCleanupSnapshotRetryAndStaleLease(t *testing.T) {
	s := newStore(t, false)
	ctx := t.Context()
	now := time.Unix(100000, 0)
	s.now = func() time.Time { return now }
	if _, err := s.CreateResource(ctx, "acme", "kv", "CACHE", "acme/__kv__/physical", "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteResource(ctx, "acme", "kv", "CACHE", "ops"); err != nil {
		t.Fatal(err)
	}
	j, err := s.ResourceCleanup(ctx, "acme", "kv", "CACHE")
	if err != nil || j.DataScope != "acme/__kv__/physical" || j.JobID == "" {
		t.Fatalf("scope snapshot: %+v %v", j, err)
	}
	if err := s.DeleteResource(ctx, "acme", "kv", "CACHE", "ops"); err != nil {
		t.Fatal(err)
	}
	same, _ := s.ResourceCleanup(ctx, "acme", "kv", "CACHE")
	if same.JobID != j.JobID {
		t.Fatal("repeated delete replaced job")
	}
	claimed, ok, err := s.ClaimResourceCleanup(ctx, j)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if _, ok, err := s.ClaimResourceCleanup(ctx, j); err != nil || ok {
		t.Fatalf("double claim: %v %v", ok, err)
	}
	if err := s.FinishResourceCleanup(ctx, claimed, errors.New("bucket down")); err != nil {
		t.Fatal(err)
	}
	failed, err := s.ResourceCleanup(ctx, "acme", "kv", "CACHE")
	if err != nil || failed.State != "failed" || failed.Attempts != 1 || failed.LastError != "bucket down" {
		t.Fatalf("failure lost: %+v %v", failed, err)
	}
	jobs, err := s.PendingResourceCleanups(ctx, 20)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("retry ignores backoff: %+v %v", jobs, err)
	}
	if err := s.RetryResourceCleanup(ctx, "acme", "kv", "CACHE"); err != nil {
		t.Fatal(err)
	}
	fresh, ok, err := s.ClaimResourceCleanup(ctx, j)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if err := s.FinishResourceCleanup(ctx, claimed, nil); !errors.Is(err, ErrCleanupStale) {
		t.Fatalf("stale lease completed cleanup: %v", err)
	}
	if err := s.FinishResourceCleanup(ctx, fresh, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateResource(ctx, "acme", "kv", "CACHE", "acme/__kv__/physical", "ops"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := s.DeleteResource(ctx, "acme", "kv", "CACHE", "ops"); err != nil {
		t.Fatal(err)
	}
	newest, _ := s.ResourceCleanup(ctx, "acme", "kv", "CACHE")
	if newest.JobID == j.JobID {
		t.Fatal("new incarnation reused old job")
	}
	if err := s.FinishResourceCleanup(ctx, fresh, nil); !errors.Is(err, ErrCleanupStale) {
		t.Fatal("old job can complete new incarnation")
	}
}

func TestDeleteScopeSnapshotSerializesAliasAndScopeUpdates(t *testing.T) {
	for _, update := range []string{"alias", "scope"} {
		t.Run(update, func(t *testing.T) {
			s := newStore(t, false)
			ctx := t.Context()
			physical := "acme/__kv__/physical"
			if _, err := s.CreateResource(ctx, "acme", "kv", "A", physical, "ops"); err != nil {
				t.Fatal(err)
			}
			snapshot := make(chan struct{})
			release := make(chan struct{})
			s.ResourceGeneration = func(context.Context, cell.Scope) (string, error) { close(snapshot); <-release; return "", nil }
			deletion := make(chan error, 1)
			go func() { deletion <- s.DeleteResource(ctx, "acme", "kv", "A", "ops") }()
			<-snapshot
			changed := make(chan error, 1)
			go func() {
				name, scope := "B", physical
				if update == "scope" {
					name = "A"
					scope = "acme/__kv__/new"
				}
				_, err := s.CreateResource(ctx, "acme", "kv", name, scope, "ops")
				changed <- err
			}()
			close(release)
			if err := <-deletion; err != nil {
				t.Fatal(err)
			}
			if err := <-changed; !errors.Is(err, ErrCleanupPending) {
				t.Fatalf("concurrent %s escaped transaction: %v", update, err)
			}
			j, err := s.ResourceCleanup(ctx, "acme", "kv", "A")
			if err != nil || j.DataScope != physical {
				t.Fatalf("wrong scope snapshot: %+v %v", j, err)
			}
		})
	}
}

func TestPruneCompletedResourceCleanups(t *testing.T) {
	s := newStore(t, false)
	ctx := t.Context()
	now := time.Unix(200000, 0)
	s.now = func() time.Time { return now }
	if _, err := s.CreateResource(ctx, "acme", "kv", "DONE", "acme/__kv__/done", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateResource(ctx, "acme", "kv", "PENDING", "acme/__kv__/pending", "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteResource(ctx, "acme", "kv", "DONE", "ops"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteResource(ctx, "acme", "kv", "PENDING", "ops"); err != nil {
		t.Fatal(err)
	}
	job, err := s.ResourceCleanup(ctx, "acme", "kv", "DONE")
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := s.ClaimResourceCleanup(ctx, job)
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := s.FinishResourceCleanup(ctx, claimed, nil); err != nil {
		t.Fatal(err)
	}
	n, err := s.PruneCompletedResourceCleanups(ctx, now.Add(time.Hour).UnixMilli(), 10)
	if err != nil || n != 1 {
		t.Fatalf("pruned = %d, %v; want 1", n, err)
	}
	if _, err := s.ResourceCleanup(ctx, "acme", "kv", "DONE"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("completed job survived prune: %v", err)
	}
	if _, err := s.ResourceCleanup(ctx, "acme", "kv", "PENDING"); err != nil {
		t.Fatalf("pending job was pruned: %v", err)
	}
}
