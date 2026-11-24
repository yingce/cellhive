package timer

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

type cronRecorder struct {
	calls []Timer
	fail  bool
}

func (d *cronRecorder) Dispatch(_ context.Context, tm Timer) error {
	d.calls = append(d.calls, tm)
	if d.fail {
		return errors.New("failed")
	}
	return nil
}
func cronPass(t *testing.T, st *Store, d Dispatcher, now int64) {
	t.Helper()
	if _, err := DispatchDue(t.Context(), st, d, now, 100, time.Hour, nil); err != nil {
		t.Fatal(err)
	}
}
func cronSeed(t *testing.T, st *Store) Timer {
	t.Helper()
	tm := New(1000, KindCron, st.cell.Scope.String(), "original-expression")
	if err := st.Upsert(t.Context(), tm); err != nil {
		t.Fatal(err)
	}
	return tm
}
func TestCronMemoryBackoffAndCeiling(t *testing.T) {
	st := newStore(t)
	tm := cronSeed(t, st)
	d := &cronRecorder{fail: true}
	times := []int64{1000, 5000, 13000, 29000, 61000, 125000}
	for i, now := range times {
		if i > 0 {
			cronPass(t, st, d, now-1)
			if len(d.calls) != i {
				t.Fatal("early retry")
			}
		}
		cronPass(t, st, d, now)
		if len(d.calls) != i+1 || d.calls[i].DueAtMs != 1000 || d.calls[i].Occurrence != tm.Occurrence {
			t.Fatalf("attempt %d: %+v", i, d.calls)
		}
		if i < 5 {
			// Poll wrappers share resident memory; repeated materialization must not
			// overwrite the retry deadline or persist the failed expression again.
			var err error
			resident := st.cell
			clock := st.Now
			st, err = NewStore(t.Context(), resident)
			st.Now = clock
			if err != nil {
				t.Fatal(err)
			}
			if err = st.Upsert(t.Context(), tm); err != nil {
				t.Fatal(err)
			}
			var occurrence string
			var attempt int
			if err = st.cell.DB.QueryRow(`SELECT occurrence, attempt FROM timers JOIN timer_retry USING(token)`).Scan(&occurrence, &attempt); err != nil {
				t.Fatal(err)
			}
			if occurrence != "" || attempt != -1 {
				t.Fatalf("persisted retry payload: %q %d", occurrence, attempt)
			}
		}
	}
	cronPass(t, st, d, 1000000)
	if len(d.calls) != 6 || len(st.cronMemory().owed) != 0 {
		t.Fatal("retry survived ceiling")
	}
}
func TestCronRetryLostOnResidentReplacement(t *testing.T) {

	root := t.TempDir()
	cs, err := cellstore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	scope := cell.Scope{Namespace: "demo", Class: "__cron__", ID: "web"}
	c, err := cs.Open(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	st, err := NewStore(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	cronSeed(t, st)
	d := &cronRecorder{fail: true}
	cronPass(t, st, d, 1000)
	if err = cs.Close(); err != nil {
		t.Fatal(err)
	}
	cs, err = cellstore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	c, err = cs.Open(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	st, err = NewStore(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	cronPass(t, st, d, 5000)
	if len(d.calls) != 1 {
		t.Fatal("replayed retry after restart")
	}
	if next, _ := st.NextDue(t.Context()); next != 0 {
		t.Fatal("orphan wake survived")
	}
	// A later normal occurrence still executes.
	if err = st.Upsert(t.Context(), New(60000, KindCron, scope.String(), "next-expression")); err != nil {
		t.Fatal(err)
	}
	d.fail = false
	cronPass(t, st, d, 60000)
	if len(d.calls) != 2 {
		t.Fatal("next occurrence lost")
	}

}
func TestCronNextOccurrenceWins(t *testing.T) {
	for _, cutoff := range []int64{5000, 6000} {
		t.Run(time.UnixMilli(cutoff).String(), func(t *testing.T) {
			st := newStore(t)
			cronSeed(t, st)
			d := &cronRecorder{fail: true}
			st.CronNext = func(context.Context, string, int64) (int64, error) { return cutoff, nil }
			cronPass(t, st, d, 1000)
			// Tie cancels immediately; overdue polling cancels a previously armed retry.
			cronPass(t, st, d, cutoff)
			if len(d.calls) != 1 {
				t.Fatalf("old retry ran past next occurrence: %+v", d.calls)
			}
			if next, err := st.NextDue(t.Context()); err != nil || next != 0 {
				t.Fatalf("deadline %d %v", next, err)
			}
		})
	}
}
func TestCronLegacyRetryIsNotReplayed(t *testing.T) {
	st := newStore(t)
	tm := cronSeed(t, st)
	if _, err := st.cell.DB.Exec(`INSERT INTO timer_retry VALUES(?,3,5000)`, tm.Token); err != nil {
		t.Fatal(err)
	}
	d := &cronRecorder{}
	cronPass(t, st, d, 5000)
	if len(d.calls) != 0 {
		t.Fatal("replayed legacy retry")
	}
	if next, _ := st.NextDue(t.Context()); next != 0 {
		t.Fatal("orphan not consumed")
	}
}

func TestCronRetryDelayStartsAfterFailure(t *testing.T) {
	st := newStore(t)
	cronSeed(t, st)
	clock := time.UnixMilli(1000)
	st.Now = func() time.Time { return clock }
	d := cronDispatchFunc(func(context.Context, Timer) error {
		clock = clock.Add(15 * time.Second)
		return errors.New("slow failure")
	})
	cronPass(t, st, d, 1000)
	if next, err := st.NextDue(t.Context()); err != nil || next != 20000 {
		t.Fatalf("retry must wait 4s after completion: %d %v", next, err)
	}
}

type cronDispatchFunc func(context.Context, Timer) error

func (f cronDispatchFunc) Dispatch(ctx context.Context, tm Timer) error { return f(ctx, tm) }

func TestCronConcurrentPassesDispatchOnce(t *testing.T) {
	st := newStore(t)
	cronSeed(t, st)
	var calls atomic.Int32
	d := cronDispatchFunc(func(context.Context, Timer) error { calls.Add(1); return nil })
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); cronPass(t, st, d, 1000) }()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("concurrent dispatch count=%d", calls.Load())
	}
}

func TestCronOnlyFailedExpressionsRetry(t *testing.T) {
	st := newStore(t)
	for _, expr := range []string{"ok", "failed"} {
		if err := st.Upsert(t.Context(), New(1000, KindCron, st.cell.Scope.String(), expr)); err != nil {
			t.Fatal(err)
		}
	}
	counts := map[string]int{}
	d := cronDispatchFunc(func(_ context.Context, tm Timer) error {
		counts[tm.Occurrence]++
		if tm.Occurrence == "failed" {
			return errors.New("failed")
		}
		return nil
	})
	cronPass(t, st, d, 1000)
	cronPass(t, st, d, 5000)
	if counts["ok"] != 1 || counts["failed"] != 2 {
		t.Fatalf("unexpected retries: %v", counts)
	}
}
