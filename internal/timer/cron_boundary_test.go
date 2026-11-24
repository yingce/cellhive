package timer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCronFailureAfterNextSlotDoesNotRetry(t *testing.T) {
	st := newStore(t)
	if err := st.Upsert(t.Context(), New(60000, KindCron, st.cell.Scope.String(), "* * * * *")); err != nil {
		t.Fatal(err)
	}
	clock := time.UnixMilli(60000)
	st.Now = func() time.Time { return clock }
	st.CronNext = func(_ context.Context, _ string, after int64) (int64, error) { return (after/60000 + 1) * 60000, nil }
	calls := 0
	d := cronDispatchFunc(func(context.Context, Timer) error {
		calls++
		if calls == 1 {
			clock = time.UnixMilli(121000)
		}
		return errors.New("failure")
	})
	cronPass(t, st, d, 60000)
	due, err := st.NextDue(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	clock = time.UnixMilli(125000)
	cronPass(t, st, d, 125000)
	t.Logf("original=60000 next normal slot=120000 first failure=121000 retry=%d calls=%d", due, calls)
	if calls != 1 || due != 0 {
		t.Fatalf("old occurrence retried after its cutoff, calls=%d", calls)
	}
}
