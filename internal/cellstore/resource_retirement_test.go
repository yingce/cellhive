package cellstore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"cellhive/internal/cell"
)

func retirementFixture(t *testing.T) (*Store, cell.Scope, ResourceRetirementIdentity) {
	t.Helper()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "cache"}, ResourceRetirementIdentity{JobID: "job", Node: "node", Session: "session", Epoch: 1, OwnerRevision: 2}
}

func waitRetirementGate(t *testing.T, s *Store, sc cell.Scope) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.waitLocked(t.Context(), func() bool { _, exists := s.retiring[sc.String()]; return !exists }); err != nil {
		t.Fatal(err)
	}
}

func TestResourceRetirementAdmittedCellCanFinish(t *testing.T) {
	s, sc, id := retirementFixture(t)
	s.Pin = func(cell.Scope) bool { return true }
	end, err := s.BeginRequestContext(t.Context(), sc.String())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.BeginResourceRetirement(t.Context(), sc, id) }()
	waitRetirementGate(t, s, sc)
	if late, err := s.BeginRequestContext(t.Context(), sc.String()); late != nil || !errors.Is(err, cell.ErrResourceDeleted) {
		t.Fatalf("late admission: %v", err)
	}
	other, err := s.BeginRequestContext(t.Context(), "acme/__kv__/other")
	if err != nil {
		t.Fatal(err)
	}
	other()
	// This open and write occur after the barrier, under the original admission.
	c, err := s.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(t.Context(), "k", []byte("v"), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("drained while busy: %v", err)
	default:
	}
	end()
	end() // releases are once-only
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// observingContext reports that waitLocked has installed its cancellation wakeup.
type observingContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *observingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestResourceRetirementEvictingWaiterRejected(t *testing.T) {
	s, sc, id := retirementFixture(t)
	key := sc.String()
	s.mu.Lock()
	s.evicting[key] = true
	s.mu.Unlock()
	waiter := make(chan error, 1)
	waiting := &observingContext{Context: t.Context(), waiting: make(chan struct{})}
	go func() {
		end, err := s.BeginRequestContext(waiting, key)
		if end != nil {
			end()
		}
		waiter <- err
	}()
	<-waiting.waiting
	retire := make(chan error, 1)
	go func() { retire <- s.BeginResourceRetirement(t.Context(), sc, id) }()
	waitRetirementGate(t, s, sc)
	// Even with eviction still held, the waiter must reject retirement promptly.
	if err := <-waiter; !errors.Is(err, cell.ErrResourceDeleted) {
		t.Fatalf("waiter: %v", err)
	}
	s.mu.Lock()
	delete(s.evicting, key)
	s.cond.Broadcast()
	s.mu.Unlock()
	if err := <-retire; err != nil {
		t.Fatal(err)
	}
}

func TestResourceRetirementCancelledDrainRetainsIdentity(t *testing.T) {
	for _, opening := range []bool{false, true} {
		t.Run(map[bool]string{false: "busy", true: "opening"}[opening], func(t *testing.T) {
			s, sc, id := retirementFixture(t)
			var end func()
			if opening {
				s.mu.Lock()
				s.opening = map[string]bool{sc.String(): true}
				s.mu.Unlock()
			} else {
				var err error
				end, err = s.BeginRequestContext(t.Context(), sc.String())
				if err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- s.BeginResourceRetirement(ctx, sc, id) }()
			waitRetirementGate(t, s, sc)
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel: %v", err)
			}
			for _, field := range []string{"job", "generation", "node", "session", "epoch", "revision"} {
				wrong := id
				switch field {
				case "job":
					wrong.JobID = "other"
				case "generation":
					wrong.Generation = "other"
				case "node":
					wrong.Node = "other"
				case "session":
					wrong.Session = "other"
				case "epoch":
					wrong.Epoch++
				case "revision":
					wrong.OwnerRevision++
				}
				if err := s.BeginResourceRetirement(t.Context(), sc, wrong); !errors.Is(err, ErrRetirementIdentityMismatch) {
					t.Fatalf("%s: %v", field, err)
				}
			}
			if opening {
				s.mu.Lock()
				delete(s.opening, sc.String())
				s.cond.Broadcast()
				s.mu.Unlock()
			} else {
				end()
			}
			if err := s.BeginResourceRetirement(t.Context(), sc, id); err != nil {
				t.Fatal(err)
			}
			if err := s.Forget(t.Context(), sc); err != nil {
				t.Fatal(err)
			}
			if end, err := s.BeginRequestContext(t.Context(), sc.String()); end != nil || !errors.Is(err, cell.ErrResourceDeleted) {
				t.Fatalf("Forget reopened: %v", err)
			}
			count, age := s.ResourceRetirementStats()
			if count != 1 || age <= 0 {
				t.Fatalf("stats: %d %s", count, age)
			}
		})
	}
}

func TestResourceRetirementDuplicateReleaseDoesNotDrainOtherRequest(t *testing.T) {
	s, sc, id := retirementFixture(t)
	first, err := s.BeginRequestContext(t.Context(), sc.String())
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.BeginRequestContext(t.Context(), sc.String())
	if err != nil {
		t.Fatal(err)
	}
	defer second()
	first()
	first()
	s.mu.Lock()
	remaining := s.busy[sc.String()]
	s.mu.Unlock()
	if remaining != 1 {
		t.Fatalf("second admission lost: busy=%d", remaining)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.BeginResourceRetirement(ctx, sc, id) }()
	waitRetirementGate(t, s, sc)
	// Cancellation must observe the remaining second request, rather than a false
	// successful drain caused by decrementing the first request twice.
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("other request lost its pin: %v", err)
	}
	second()
	if err := s.BeginResourceRetirement(t.Context(), sc, id); err != nil {
		t.Fatal(err)
	}
}

func TestRetirementScopePaginationIsBoundedAndOrdered(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, name := range []string{"z", "b", "a", "d", "c"} {
		sc := cell.Scope{Namespace: "page", Class: "__kv__", ID: name}
		id := ResourceRetirementIdentity{JobID: "pending", Node: "node", Session: "session", Epoch: 1}
		if err := s.BeginResourceRetirement(t.Context(), sc, id); err != nil {
			t.Fatal(err)
		}
	}
	after := ""
	var names []string
	for {
		page := s.ResourceRetirementScopes(after, 2)
		if len(page) > 2 {
			t.Fatal("page bound exceeded")
		}
		if len(page) == 0 {
			break
		}
		for _, sc := range page {
			if sc.String() <= after {
				t.Fatal("pagination did not advance")
			}
			names = append(names, sc.ID)
			after = sc.String()
		}
	}
	want := []string{"a", "b", "c", "d", "z"}
	if len(names) != len(want) {
		t.Fatalf("lost gates: %v", names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("unordered gates: %v", names)
		}
	}
}
