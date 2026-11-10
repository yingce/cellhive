package cellcapture

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

// A cancelled commit deliberately cannot exit until released, modelling a
// transport whose cancellation acknowledgement/cleanup is still in progress.
type quiescenceCommitter struct {
	entered   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (c *quiescenceCommitter) Commit(ctx context.Context, _ cell.Scope, _ uint64, _ []byte) error {
	c.once.Do(func() { close(c.entered) })
	<-ctx.Done()
	close(c.cancelled)
	<-c.release
	return ctx.Err()
}

func awaitQuiescenceSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("capture lifecycle did not reach expected state")
	}
}

func TestCaptureQuiescenceRetainsBlockedState(t *testing.T) {
	for _, mode := range []string{"drop", "stop", "epoch", "watch-owner"} {
		t.Run(mode, func(t *testing.T) {
			cs, err := cellstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer cs.Close()
			scope := cell.Scope{Namespace: "quiescence", Class: "__kv__", ID: mode}
			var epoch atomic.Uint64
			epoch.Store(1)
			var owned atomic.Bool
			owned.Store(true)
			commit := &quiescenceCommitter{entered: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(commit.release) }) }
			defer release()
			checkpoint := time.Hour
			if mode == "watch-owner" {
				checkpoint = time.Millisecond
			}
			m := &Manager{Store: cs, Committer: commit, Checkpoint: checkpoint, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return owned.Load(), epoch.Load(), nil }}
			defer m.Stop()
			if _, err := m.Ensure(t.Context(), scope); err != nil {
				t.Fatal(err)
			}
			m.mu.Lock()
			st := m.caps[scope.String()]
			m.mu.Unlock()
			c, err := cs.Cell(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Put(t.Context(), "key", []byte("value"), nil); err != nil {
				t.Fatal(err)
			}
			st.cap.Notify()
			awaitQuiescenceSignal(t, commit.entered)
			switch mode {
			case "drop":
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if err := m.Drop(ctx, scope); !errors.Is(err, context.Canceled) {
					t.Fatalf("blocked drop = %v", err)
				}
			case "stop":
				m.Stop()
			case "epoch":
				// Stop the watcher from winning this particular transition: the new
				// epoch is checked by Ensure even when checkpointing is infrequent.
				epoch.Store(2)
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
				_, err := m.Ensure(ctx, scope)
				cancel()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("epoch replacement = %v", err)
				}
			case "watch-owner":
				owned.Store(false)
			}
			awaitQuiescenceSignal(t, commit.cancelled)
			// Every cancellation path must remain visible to repeated Drop calls.
			for i := 0; i < 2; i++ {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if err := m.Drop(ctx, scope); !errors.Is(err, context.Canceled) {
					t.Fatalf("retry falsely proved quiescence: %v", err)
				}
			}
			if mode != "watch-owner" {
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
				_, err := m.Ensure(ctx, scope)
				cancel()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("replacement overlapped stopped capture: %v", err)
				}
			}
			m.mu.Lock()
			current := m.caps[scope.String()]
			m.mu.Unlock()
			if current != st {
				t.Fatal("unfinished state lost its slot")
			}
			release()
			awaitQuiescenceSignal(t, st.stopped)
			if err := m.Drop(t.Context(), scope); err != nil {
				t.Fatal(err)
			}
			m.mu.Lock()
			remaining := len(m.caps)
			_, hint := m.hints[scope.String()]
			m.mu.Unlock()
			if remaining != 0 {
				t.Fatal("completed stopped state retained")
			}
			if mode != "drop" && hint {
				t.Fatal("non-drop cancellation published resume hint")
			}
		})
	}
}

func TestCaptureQuiescenceCancelsInitializationAndReadyWait(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	scope := cell.Scope{Namespace: "quiescence", Class: "__kv__", ID: "initializer"}
	c, err := cs.Cell(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(t.Context(), "seed", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	commit := &quiescenceCommitter{entered: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(commit.release) }) }
	defer release()
	m := &Manager{Store: cs, Committer: commit, AutoSnapshot: true, Checkpoint: time.Hour, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }}
	defer m.Stop()
	result := make(chan error, 1)
	go func() { _, err := m.Ensure(t.Context(), scope); result <- err }()
	awaitQuiescenceSignal(t, commit.entered)
	m.mu.Lock()
	st := m.caps[scope.String()]
	m.mu.Unlock()
	// The initializer is blocked, but a second Ensure must obey its own deadline.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	_, err = m.Ensure(ctx, scope)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ready wait ignored cancellation: %v", err)
	}
	ctx, cancel = context.WithCancel(t.Context())
	cancel()
	if err := m.Drop(ctx, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("initializer drop = %v", err)
	}
	awaitQuiescenceSignal(t, commit.cancelled)
	ctx, cancel = context.WithCancel(t.Context())
	cancel()
	if err := m.Drop(ctx, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("initializer retry falsely succeeded: %v", err)
	}
	release()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled initializer = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("initializer stranded ready/stopped channels")
	}
	awaitQuiescenceSignal(t, st.stopped)
	awaitQuiescenceSignal(t, st.ready)
	if err := m.Drop(t.Context(), scope); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	remaining := len(m.caps)
	_, hint := m.hints[scope.String()]
	m.mu.Unlock()
	if remaining != 0 || hint {
		t.Fatal("failed initialization retained state or resume hint")
	}
}

func TestCaptureQuiescenceDoesNotRepublishInvalidatedHint(t *testing.T) {
	for _, mode := range []string{"forget-hint", "stop", "epoch"} {
		t.Run(mode, func(t *testing.T) {
			cs, err := cellstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer cs.Close()
			scope := cell.Scope{Namespace: "quiescence", Class: "__kv__", ID: "hint-" + mode}
			c, err := cs.Cell(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Put(t.Context(), "seed", []byte("value"), nil); err != nil {
				t.Fatal(err)
			}
			block := &quiescenceCommitter{entered: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(block.release) }) }
			defer release()
			var epoch atomic.Uint64
			epoch.Store(1)
			var calls atomic.Int32
			m := &Manager{Store: cs, AutoSnapshot: true, Checkpoint: time.Hour,
				Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, epoch.Load(), nil },
				Committer: committerFunc(func(ctx context.Context, s cell.Scope, e uint64, b []byte) error {
					if calls.Add(1) == 1 {
						return nil
					}
					return block.Commit(ctx, s, e, b)
				}),
			}
			defer m.Stop()
			if _, err := m.Ensure(t.Context(), scope); err != nil {
				t.Fatal(err)
			}
			m.mu.Lock()
			st := m.caps[scope.String()]
			m.mu.Unlock()
			if st.cap.Stats().DurableTxID == 0 {
				t.Fatal("test requires a reusable durable hint")
			}
			if err := c.Put(t.Context(), "next", []byte("value"), nil); err != nil {
				t.Fatal(err)
			}
			st.cap.Notify()
			awaitQuiescenceSignal(t, block.entered)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := m.Drop(ctx, scope); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			awaitQuiescenceSignal(t, block.cancelled)
			switch mode {
			case "forget-hint":
				m.ForgetHint(scope)
			case "stop":
				m.Stop()
			case "epoch":
				epoch.Store(2)
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
				_, err := m.Ensure(ctx, scope)
				cancel()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			}
			release()
			awaitQuiescenceSignal(t, st.stopped)
			m.mu.Lock()
			_, hint := m.hints[scope.String()]
			m.mu.Unlock()
			if hint {
				t.Fatal("late stopped capture republished invalidated resume hint")
			}
		})
	}
}

// An initialization failure must not advertise readiness while waiting for the
// manager lock to publish its stopping state. No Drop pre-marks this state.
func TestCaptureQuiescenceFailedInitializationPublishesStoppingBeforeReady(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	scope := cell.Scope{Namespace: "quiescence", Class: "__kv__", ID: "failed-ready"}
	entered, release := make(chan struct{}), make(chan struct{})
	failure := errors.New("initial snapshot failed")
	m := &Manager{Store: cs, AutoSnapshot: true,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: committerFunc(func(context.Context, cell.Scope, uint64, []byte) error {
			close(entered)
			<-release
			return failure
		}),
	}
	defer m.Stop()
	result := make(chan error, 1)
	go func() { _, err := m.Ensure(t.Context(), scope); result <- err }()
	awaitQuiescenceSignal(t, entered)
	m.mu.Lock()
	st := m.caps[scope.String()]
	close(release)
	// failInit starts the cancelled loop before publishing the stopping state.
	// Hold the manager lock to isolate the publication boundary deterministically.
	select {
	case <-st.cap.Done():
	case <-time.After(5 * time.Second):
		m.mu.Unlock()
		t.Fatal("failed initializer did not start cancelled loop")
	}
	select {
	case <-st.ready:
		m.mu.Unlock()
		t.Fatal("failed initializer published ready before stopping")
	case <-time.After(30 * time.Millisecond):
	}
	m.mu.Unlock()
	select {
	case err := <-result:
		if !errors.Is(err, failure) {
			t.Fatalf("Ensure = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failed initializer did not finish")
	}
	awaitQuiescenceSignal(t, st.stopped)
	m.mu.Lock()
	defer m.mu.Unlock()
	if !st.stopping || m.caps[scope.String()] != nil {
		t.Fatal("failed initializer was not stopped and reclaimed")
	}
}
