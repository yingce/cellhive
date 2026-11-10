package timer

import (
	"context"
	"errors"
	"testing"
)

func TestRunnerAdmissionCoversWholePass(t *testing.T) {
	for _, mode := range []string{"success", "denied", "open-error"} {
		t.Run(mode, func(t *testing.T) {
			r, _, st := newRunner(t, &recordingDispatcher{})
			const scope = "demo/__timers__/run"
			if err := st.Upsert(t.Context(), New(1, KindDOAlarm, scope, "due")); err != nil {
				t.Fatal(err)
			}
			held := false
			opens, releases, commits := 0, 0, 0
			r.Own = func(context.Context, string) (bool, error) {
				if held {
					t.Error("ownership preparation under admission")
				}
				return true, nil
			}
			r.Admit = func(context.Context, string) (func(), error) {
				if mode == "denied" {
					return nil, errors.New("retiring")
				}
				held = true
				return func() { held = false; releases++ }, nil
			}
			r.Open = func(context.Context, string) (*Store, error) {
				opens++
				if !held {
					t.Error("open outside admission")
				}
				if mode == "open-error" {
					return nil, errors.New("open failed")
				}
				return st, nil
			}
			r.Commit = func(_ context.Context, _ string, fn func() error) error {
				commits++
				if !held {
					t.Error("commit outside admission")
				}
				return fn()
			}
			n, err := r.Pass(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if held {
				t.Fatal("admission leaked")
			}
			if mode == "denied" {
				if opens != 0 || releases != 0 || n != 0 {
					t.Fatalf("denied opens=%d releases=%d n=%d", opens, releases, n)
				}
			} else if opens != 1 || releases != 1 {
				t.Fatalf("opens=%d releases=%d", opens, releases)
			}
			if mode == "success" && (n != 1 || commits == 0) {
				t.Fatalf("dispatch=%d commits=%d", n, commits)
			}
		})
	}
}
