package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestValidateResourceCleanupLeaseIdentity(t *testing.T) {
	for _, generation := range []string{"generation-1", ""} {
		t.Run("generation="+generation, func(t *testing.T) {
			s := newStore(t, false)
			ctx := t.Context()
			now := time.Unix(100000, 0)
			s.now = func() time.Time { return now }
			s.ResourceGeneration = func(context.Context, cell.Scope) (string, error) { return generation, nil }
			if _, err := s.CreateResource(ctx, "acme", "kv", "CACHE", "acme/__kv__/physical", "ops"); err != nil {
				t.Fatal(err)
			}
			if err := s.DeleteResource(ctx, "acme", "kv", "CACHE", "ops"); err != nil {
				t.Fatal(err)
			}
			pending, err := s.ResourceCleanup(ctx, "acme", "kv", "CACHE")
			if err != nil {
				t.Fatal(err)
			}
			claimed, ok, err := s.ClaimResourceCleanup(ctx, pending)
			if err != nil || !ok {
				t.Fatalf("claim: ok=%v err=%v", ok, err)
			}
			if claimed.SourceGeneration != generation {
				t.Fatalf("source generation = %q, want %q", claimed.SourceGeneration, generation)
			}
			if err := s.ValidateResourceCleanupLease(ctx, claimed); err != nil {
				t.Fatalf("valid durable lease: %v", err)
			}
			for _, tc := range []struct {
				name   string
				change func(*ResourceCleanup)
			}{
				{"scope", func(j *ResourceCleanup) { j.DataScope = "acme/__kv__/other" }},
				{"noncanonical scope", func(j *ResourceCleanup) { j.DataScope = "physical" }},
				{"generation", func(j *ResourceCleanup) { j.SourceGeneration = "other-generation" }},
				{"job", func(j *ResourceCleanup) { j.JobID += "-other" }},
				{"token", func(j *ResourceCleanup) { j.LeaseToken += "-other" }},
				{"empty token", func(j *ResourceCleanup) { j.LeaseToken = "" }},
				{"shorter deadline", func(j *ResourceCleanup) { j.LeaseUntilMs-- }},
				{"longer deadline", func(j *ResourceCleanup) { j.LeaseUntilMs++ }},
				{"missing deadline", func(j *ResourceCleanup) { j.LeaseUntilMs = 0 }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					wrong := claimed
					tc.change(&wrong)
					if err := s.ValidateResourceCleanupLease(ctx, wrong); !errors.Is(err, ErrCleanupStale) {
						t.Fatalf("mismatched lease: %v", err)
					}
				})
			}
			// Expiration is inclusive, even when the token still matches the row.
			now = time.UnixMilli(claimed.LeaseUntilMs)
			if err := s.ValidateResourceCleanupLease(ctx, claimed); !errors.Is(err, ErrCleanupStale) {
				t.Fatalf("expired lease: %v", err)
			}
			fresh, ok, err := s.ClaimResourceCleanup(ctx, pending)
			if err != nil || !ok {
				t.Fatalf("reclaim: ok=%v err=%v", ok, err)
			}
			if err := s.ValidateResourceCleanupLease(ctx, fresh); err != nil {
				t.Fatalf("reclaimed lease: %v", err)
			}
			if err := s.ValidateResourceCleanupLease(ctx, claimed); !errors.Is(err, ErrCleanupStale) {
				t.Fatalf("predecessor lease accepted: %v", err)
			}
			if err := s.ContinueResourceCleanup(ctx, fresh); err != nil {
				t.Fatal(err)
			}
			if err := s.ValidateResourceCleanupLease(ctx, fresh); !errors.Is(err, ErrCleanupStale) {
				t.Fatalf("released lease accepted: %v", err)
			}
		})
	}
}

func TestValidateResourceCleanupLeaseRejectsEmptyDurableToken(t *testing.T) {
	s := newStore(t, false)
	ctx := t.Context()
	now := time.Unix(100000, 0)
	s.now = func() time.Time { return now }
	c, err := s.cell(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A malformed running row must not authenticate an omitted token merely
	// because the empty incoming token equals the durable value.
	_, err = c.DB.ExecContext(ctx, `INSERT INTO resource_cleanups(ns,kind,name,job_id,data_scope,source_generation,dead_letters,state,requested_ms,updated_ms,lease_token,lease_until_ms) VALUES('acme','kv','CACHE','job','acme/__kv__/physical','','[]','running',?,?, '',?)`, now.UnixMilli(), now.UnixMilli(), now.Add(time.Minute).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.ResourceCleanup(ctx, "acme", "kv", "CACHE")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ValidateResourceCleanupLease(ctx, job); !errors.Is(err, ErrCleanupStale) {
		t.Fatalf("empty durable token accepted: %v", err)
	}
}
