package owner

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

func TestResourceRetirementSerializesClaimsAndReleases(t *testing.T) {
	m, b := newManager(t, t.TempDir(), "local")
	sc := cell.Scope{Namespace: "retire", Class: "__kv__", ID: "a"}
	expected, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	started, resume, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- m.WithResourceRetirement(t.Context(), sc, "job", expected, func(ctx context.Context, got cell.Owner) error {
			close(started)
			<-resume
			return replica.New(b).RetireOwnedResource(ctx, sc, "job", got, func(context.Context) error { return nil })
		})
	}()
	<-started
	for _, op := range []string{"claim", "cleanup-claim", "release", "purge", "retire"} {
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		switch op {
		case "claim":
			_, err = m.Claim(ctx, sc, time.Now())
		case "cleanup-claim":
			_, err = m.ClaimForResourceCleanup(ctx, sc, time.Now(), "job", &expected, func(context.Context) error { return nil }, nil)
		case "release":
			err = m.Release(ctx, sc, expected.Epoch)
		case "purge":
			err = m.ReleaseForPurge(ctx, sc, expected.Epoch)
		case "retire":
			err = m.WithResourceRetirement(ctx, sc, "job", expected, func(context.Context, cell.Owner) error { return errors.New("unexpected callback") })
		}
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s bypassed transition: %v", op, err)
		}
	}
	if check, ok := m.ReadFence(sc); ok {
		check()
		t.Error("read evidence granted during retirement")
	}
	other := sc
	other.ID = "unrelated"
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := m.Claim(ctx, other, time.Now()); err != nil {
		t.Errorf("unrelated scope blocked: %v", err)
	}
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := m.Claim(t.Context(), sc, time.Now()); !errors.Is(err, ErrRetired) {
		t.Fatalf("retired resource admitted: %v", err)
	}
	if err := m.ReleaseForPurge(t.Context(), sc, expected.Epoch); err != nil {
		t.Fatal(err)
	}
}

func TestResourceRetirementRejectsChangedIdentity(t *testing.T) {
	for _, mode := range []string{"node", "session", "epoch", "job", "floor", "retired", "expired", "cleared-job"} {
		t.Run(mode, func(t *testing.T) {
			m, b := newManager(t, t.TempDir(), "local")
			sc := cell.Scope{Namespace: "retire", Class: "__queue__", ID: mode}
			expected, err := m.Claim(t.Context(), sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			current := expected
			switch mode {
			case "node":
				current.Node = "successor"
			case "session":
				current.Session = "successor"
			case "epoch":
				current.Epoch++
			case "job":
				current.RetirementJob = "successor"
			case "floor":
				current.RetiredEpoch++
			case "retired":
				current.Retired = true
			case "expired":
				current.Expiry = time.Now().Add(-time.Second).UnixMilli()
			case "cleared-job":
				current.RetirementJob = "job"
				expected = current
			}
			raw, _ := cell.MarshalOwner(current)
			if _, err := b.Put(t.Context(), Key(sc), raw); err != nil {
				t.Fatal(err)
			}
			called := false
			err = m.WithResourceRetirement(t.Context(), sc, "job", expected, func(context.Context, cell.Owner) error { called = true; return nil })
			if err == nil || called {
				t.Fatalf("stale source accepted: called=%v err=%v", called, err)
			}
		})
	}
}

func TestResourceRetirementFailureInvalidatesAndUnlocks(t *testing.T) {
	for _, mode := range []string{"failure", "cancel", "successor-after-callback", "source-after-callback"} {
		t.Run(mode, func(t *testing.T) {
			m, b := newManager(t, t.TempDir(), "local")
			sc := cell.Scope{Namespace: "retire", Class: "__kv__", ID: mode}
			expected, err := m.Claim(t.Context(), sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			before, ok := m.ReadFence(sc)
			if !ok {
				t.Fatal("missing initial fence")
			}
			defer before()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failure := errors.New("ambiguous publication")
			err = m.WithResourceRetirement(ctx, sc, "job", expected, func(context.Context, cell.Owner) error {
				if mode == "cancel" {
					cancel()
					return nil
				}
				if mode == "successor-after-callback" || mode == "source-after-callback" {
					next := expected
					if mode == "successor-after-callback" {
						next.Epoch++
					} else {
						next.RetirementJob = "recreated"
					}
					raw, _ := cell.MarshalOwner(next)
					_, err := b.Put(t.Context(), Key(sc), raw)
					return err
				}
				return failure
			})
			if err == nil {
				t.Fatal("failed callback accepted")
			}
			if before() {
				t.Fatal("old ownership evidence survived transition")
			}
			check, ok := m.ReadFence(sc)
			if !ok {
				t.Fatal("transition marker leaked")
			}
			check()
			// No retirement was published, so a fresh claim can execute after failure.
			claimCtx, claimCancel := context.WithTimeout(t.Context(), time.Second)
			defer claimCancel()
			if _, err := m.Claim(claimCtx, sc, time.Now()); err != nil {
				t.Fatalf("transition lock leaked: %v", err)
			}
		})
	}
}

type retirementRenewBucket struct {
	bucket.Bucket
	beforeCAS func() error
}

func (b *retirementRenewBucket) CAS(ctx context.Context, key string, data []byte, token string) (string, error) {
	if b.beforeCAS != nil {
		fn := b.beforeCAS
		b.beforeCAS = nil
		if err := fn(); err != nil {
			return "", err
		}
	}
	return b.Bucket.CAS(ctx, key, data, token)
}
func TestResourceRetirementRenewalConflict(t *testing.T) {
	m, b := newManager(t, t.TempDir(), "local")
	sc := cell.Scope{Namespace: "retire", Class: "__kv__", ID: "renew"}
	expected, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	hooked := &retirementRenewBucket{Bucket: b}
	renewedExpiry := int64(0)
	hooked.beforeCAS = func() error {
		renewed, err := m.Renew(t.Context(), sc, expected.Epoch, time.Now().Add(time.Second))
		renewedExpiry = renewed.Expiry
		return err
	}
	err = m.WithResourceRetirement(t.Context(), sc, "job", expected, func(ctx context.Context, got cell.Owner) error {
		return replica.New(hooked).RetireOwnedResource(ctx, sc, "job", got, func(context.Context) error { return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := m.Resolve(t.Context(), sc)
	if err != nil || !got.Retired || got.RetirementJob != "job" || got.Expiry != renewedExpiry {
		t.Fatalf("renewal lost retirement or lease: %+v %v", got, err)
	}
}

func TestResourceRetirementWaitsForReleaseHook(t *testing.T) {
	m, _ := newManager(t, t.TempDir(), "local")
	sc := cell.Scope{Namespace: "retire", Class: "__kv__", ID: "release-first"}
	expected, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	started, resume, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	m.BeforeRelease = func(ctx context.Context, scope cell.Scope, epoch uint64) error {
		if _, _, err := m.Resolve(ctx, scope); err != nil {
			return err
		}
		close(started)
		<-resume
		return nil
	}
	go func() { done <- m.Release(t.Context(), sc, expected.Epoch) }()
	<-started
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	err = m.WithResourceRetirement(ctx, sc, "job", expected, func(context.Context, cell.Owner) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("retirement bypassed release hook: %v", err)
	}
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := m.WithResourceRetirement(t.Context(), sc, "job", expected, func(context.Context, cell.Owner) error { return nil }); !errors.Is(err, ErrUnowned) {
		t.Fatalf("released owner accepted: %v", err)
	}
}

func TestResourceRetirementRetriesLostAcknowledgment(t *testing.T) {
	m, b := newManager(t, t.TempDir(), "local")
	sc := cell.Scope{Namespace: "retire", Class: "__kv__", ID: "lost-ack"}
	expected, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	for i := 0; i < 2; i++ {
		err = m.WithResourceRetirement(t.Context(), sc, "job", expected, func(ctx context.Context, _ cell.Owner) error {
			return rep.RetireOwnedResource(ctx, sc, "job", expected, func(context.Context) error { return nil })
		})
		if err != nil {
			t.Fatalf("publication attempt%d: %v", i, err)
		}
	}
	if err := rep.AllowResource(t.Context(), sc, "job"); err != nil {
		t.Fatal(err)
	}
	called := false
	err = m.WithResourceRetirement(t.Context(), sc, "job", expected, func(context.Context, cell.Owner) error { called = true; return nil })
	if err == nil || called {
		t.Fatalf("recreated resource reblocked: called=%v err=%v", called, err)
	}
}
