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

func releasedRetirementFixture(t *testing.T) (*Manager, bucket.Bucket, cell.Scope, cell.Owner) {
	t.Helper()
	m, b := newManager(t, t.TempDir(), "local")
	sc := cell.Scope{Namespace: "released", Class: "__kv__", ID: "resource"}
	previous, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := replica.New(b).RetireOwnedResource(t.Context(), sc, "job", previous, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	return m, b, sc, previous
}

func TestReleasedResourceRetirementProof(t *testing.T) {
	for _, mode := range []string{"released", "local-owner", "foreign-live", "recreated", "wrong-job", "missing", "epoch", "floor", "foreign-previous", "missing-node", "missing-session", "zero-epoch", "nil-callback"} {
		t.Run(mode, func(t *testing.T) {
			m, b, sc, previous := releasedRetirementFixture(t)
			if mode != "local-owner" {
				if err := m.ReleaseForPurge(t.Context(), sc, previous.Epoch); err != nil {
					t.Fatal(err)
				}
			}
			current, _, err := m.Resolve(t.Context(), sc)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "foreign-live":
				current.Node = "foreign"
				current.Session = "foreign-session"
				current.Expiry = time.Now().Add(time.Minute).UnixMilli()
			case "recreated":
				current.Retired = false
			case "wrong-job":
				current.RetirementJob = "other"
			case "epoch":
				current.Epoch = previous.Epoch - 1
			case "floor":
				current.RetiredEpoch = previous.Epoch - 1
			case "foreign-previous":
				previous.Node = "foreign"
			case "missing-node":
				previous.Node = ""
			case "missing-session":
				previous.Session = ""
			case "zero-epoch":
				previous.Epoch = 0
			}
			raw, err := cell.MarshalOwner(current)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.Put(t.Context(), Key(sc), raw); err != nil {
				t.Fatal(err)
			}
			if mode == "missing" {
				if err := b.Delete(t.Context(), Key(sc)); err != nil {
					t.Fatal(err)
				}
			}
			called := false
			fn := func(context.Context) error { called = true; return nil }
			if mode == "nil-callback" {
				fn = nil
			}
			err = m.WithReleasedResourceRetirement(t.Context(), sc, "job", previous, fn)
			if mode == "released" {
				if err != nil || !called {
					t.Fatalf("released proof: called=%v err=%v", called, err)
				}
			} else if err == nil || called {
				t.Fatalf("invalid proof accepted: called=%v err=%v", called, err)
			}
		})
	}
}

func TestReleasedResourceRetirementSerializesLocalClaims(t *testing.T) {
	m, _, sc, previous := releasedRetirementFixture(t)
	if err := m.ReleaseForPurge(t.Context(), sc, previous.Epoch); err != nil {
		t.Fatal(err)
	}
	current, _, err := m.Resolve(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	old, ok := m.ReadFence(sc)
	if !ok {
		t.Fatal("missing initial read fence")
	}
	defer old()
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- m.WithReleasedResourceRetirement(t.Context(), sc, "job", previous, func(context.Context) error { close(entered); <-resume; return nil })
	}()
	defer func() {
		close(resume)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	<-entered
	if old() {
		t.Error("old read evidence survived transition")
	}
	if fence, ok := m.ReadFence(sc); ok {
		fence()
		t.Error("read fence granted inside transition")
	}
	for _, op := range []string{"ordinary", "cleanup"} {
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		if op == "ordinary" {
			_, err = m.Claim(ctx, sc, time.Now())
		} else {
			_, err = m.ClaimForResourceCleanup(ctx, sc, time.Now(), "job", &current, func(context.Context) error { return nil }, nil)
		}
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s crossed boundary: %v", op, err)
		}
	}
	other := sc
	other.ID = "unrelated"
	if _, err := m.Claim(t.Context(), other, time.Now()); err != nil {
		t.Error(err)
	}
}

func TestReleasedResourceRetirementRechecksAfterCallback(t *testing.T) {
	for _, mode := range []string{"node", "session", "epoch", "recreated", "job", "floor", "missing", "callback-error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			m, b, sc, previous := releasedRetirementFixture(t)
			if err := m.ReleaseForPurge(t.Context(), sc, previous.Epoch); err != nil {
				t.Fatal(err)
			}
			current, _, err := m.Resolve(t.Context(), sc)
			if err != nil {
				t.Fatal(err)
			}
			old, ok := m.ReadFence(sc)
			if !ok {
				t.Fatal("missing fence")
			}
			defer old()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			err = m.WithReleasedResourceRetirement(ctx, sc, "job", previous, func(context.Context) error {
				switch mode {
				case "node":
					current.Node = "remote"
				case "session":
					current.Session = "remote"
				case "epoch":
					current.Epoch++
				case "recreated":
					current.Retired = false
				case "job":
					current.RetirementJob = "other"
				case "floor":
					current.RetiredEpoch++
				case "missing":
					return b.Delete(t.Context(), Key(sc))
				case "callback-error":
					return errors.New("injected callback failure")
				case "cancel":
					cancel()
					return nil
				}
				raw, err := cell.MarshalOwner(current)
				if err != nil {
					return err
				}
				_, err = b.Put(t.Context(), Key(sc), raw)
				return err
			})
			if err == nil {
				t.Fatal("changed released proof accepted")
			}
			if old() {
				t.Fatal("old evidence survived failure")
			}
			if fence, ok := m.ReadFence(sc); !ok {
				t.Fatal("transition marker leaked")
			} else {
				fence()
			}
		})
	}
}

type releaseLostACKBucket struct {
	bucket.Bucket
	fail bool
}

func (b *releaseLostACKBucket) CAS(ctx context.Context, key string, data []byte, token string) (string, error) {
	next, err := b.Bucket.CAS(ctx, key, data, token)
	if err == nil && b.fail {
		b.fail = false
		return "", errors.New("release committed but response lost")
	}
	return next, err
}
func TestReleasedResourceRetirementProvesLostReleaseACK(t *testing.T) {
	m, b, sc, previous := releasedRetirementFixture(t)
	m.B = &releaseLostACKBucket{Bucket: b, fail: true}
	if err := m.ReleaseForPurge(t.Context(), sc, previous.Epoch); err == nil {
		t.Fatal("expected lost release response")
	}
	// Retry the proof from fresh own.json, not the ambiguous release response.
	calls := 0
	for i := 0; i < 2; i++ {
		if err := m.WithReleasedResourceRetirement(t.Context(), sc, "job", previous, func(context.Context) error { calls++; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("callbacks=%d", calls)
	}
	if _, err := m.Claim(t.Context(), sc, time.Now()); !errors.Is(err, ErrRetired) {
		t.Fatalf("retired scope resurrected: %v", err)
	}
}
