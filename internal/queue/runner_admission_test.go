package queue_test

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"cellhive/internal/cellstore"
	"cellhive/internal/queue"
)

func TestRunnerReadAdmissionRejectsBeforeOpeningCell(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	fd := &fakeDispatcher{}
	r := queue.Runner{Store: queue.New(cs), Dispatch: fd, Queues: staticRefs("demo", "q"), AdmitRead: func(context.Context, queue.Ref) (func(), error) { return nil, errors.New("retiring") }}
	n, err := r.Pass(t.Context())
	if err != nil || n != 0 || fd.calls != 0 {
		t.Fatalf("pass=%d %v calls=%d", n, err, fd.calls)
	}
	path, err := cs.Path(queue.Scope("demo", "q"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("denied readiness opened SQLite: %v", err)
	}
}

func TestRunnerSeparatesPreparationReadDispatchAndCommitAdmission(t *testing.T) {
	st := newStore(t)
	if _, err := st.Send(t.Context(), "demo", "q", []byte("one"), "text/plain", 0, ""); err != nil {
		t.Fatal(err)
	}
	var readHeld, dispatchHeld atomic.Bool
	var preparations, validations atomic.Int32
	fd := &fakeDispatcher{}
	r := queue.Runner{Store: st, Dispatch: fd, Queues: staticRefs("demo", "q"),
		Filter: func(context.Context, queue.Ref) (bool, error) {
			preparations.Add(1)
			if readHeld.Load() || dispatchHeld.Load() {
				t.Error("preparation ran under admission")
			}
			return true, nil
		},
		AdmitRead: func(context.Context, queue.Ref) (func(), error) {
			readHeld.Store(true)
			return func() { readHeld.Store(false) }, nil
		},
		AdmitDispatch: func(context.Context, queue.Ref) (func(), error) {
			dispatchHeld.Store(true)
			return func() { dispatchHeld.Store(false) }, nil
		},
		ValidateDispatch: func(context.Context, queue.Ref) (bool, error) {
			validations.Add(1)
			if !dispatchHeld.Load() {
				t.Error("owner validation outside dispatch admission")
			}
			return true, nil
		},
		Commit: func(_ context.Context, _, _ string, fn func() error) error {
			if readHeld.Load() || dispatchHeld.Load() {
				t.Error("nested mutation admission")
			}
			return fn()
		},
	}
	if n, err := r.Pass(t.Context()); err != nil || n != 1 {
		t.Fatalf("pass=%d %v", n, err)
	}
	if preparations.Load() != 2 || validations.Load() != 1 || readHeld.Load() || dispatchHeld.Load() {
		t.Fatalf("lifecycle prepare=%d validate=%d read=%v dispatch=%v", preparations.Load(), validations.Load(), readHeld.Load(), dispatchHeld.Load())
	}
}
