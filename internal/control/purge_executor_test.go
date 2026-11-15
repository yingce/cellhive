package control

import (
	"context"
	"errors"
	"testing"
)

func TestPurgeExecutorProofFailureDoesNotDelete(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	proofErr := errors.New("remote proof unavailable")
	calls, commits := 0, 0
	e := &PurgeExecutor{Store: s, Holder: "executor", Hook: func(context.Context, string, string) (bool, error) { calls++; return true, nil }, Commit: func(ctx context.Context, write func() error) error {
		commits++
		if err := write(); err != nil {
			return err
		}
		if commits == 2 {
			return proofErr
		}
		return nil
	}}
	if err := e.Pass(ctx); !errors.Is(err, proofErr) {
		t.Fatalf("proof result: %v", err)
	}
	if calls != 0 {
		t.Fatalf("physical deletion before proof: %d", calls)
	}
	if _, err := s.CreateApp(ctx, "acme", "test"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("unconfirmed claim lost: %v", err)
	}
	if err := e.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("unconfirmed claim resumed: %d", calls)
	}
}

func TestPurgeExecutorKeepsIdentityAcrossPartialPasses(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	commit := func(_ context.Context, write func() error) error { return write() }
	calls := 0
	e := &PurgeExecutor{Store: s, Holder: "a", Commit: commit, Hook: func(context.Context, string, string) (bool, error) { calls++; return calls == 2, nil }}
	if err := e.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	other := &PurgeExecutor{Store: New(s.cs, nil), Holder: "b", Commit: commit, Hook: func(context.Context, string, string) (bool, error) {
		t.Error("other executor took running job")
		return true, nil
	}}
	if err := other.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateApp(ctx, "acme", "test"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("partial pass released fence: %v", err)
	}
	if err := e.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
}

func TestPurgeExecutorUnknownOutcomeIsNotRetried(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	e := &PurgeExecutor{Store: s, Holder: "a", Commit: func(_ context.Context, write func() error) error { return write() }, Hook: func(context.Context, string, string) (bool, error) { calls++; return false, context.DeadlineExceeded }}
	if err := e.Pass(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unknown outcome: %v", err)
	}
	if err := e.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("unknown delete retried: %d", calls)
	}
	if _, err := s.CreateApp(ctx, "acme", "test"); !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("unknown fence released: %v", err)
	}
}
