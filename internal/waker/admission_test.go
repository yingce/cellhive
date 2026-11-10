package waker

import (
	"context"
	"errors"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/timer"
)

func TestWakerAdmissionCoversWholePass(t *testing.T) {
	for _, mode := range []string{"success", "denied", "open-error"} {
		t.Run(mode, func(t *testing.T) {
			cs, err := cellstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer cs.Close()
			sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "timer"}
			c, err := cs.Cell(t.Context(), sc)
			if err != nil {
				t.Fatal(err)
			}
			st, err := timer.NewStore(t.Context(), c)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.Upsert(t.Context(), timer.New(1000, timer.KindDOAlarm, sc.String(), "due")); err != nil {
				t.Fatal(err)
			}
			w := newWaker(t, newBucket(t, t.TempDir()), "node", &countingDispatcher{}, sc, st)
			held := false
			opens, releases, commits := 0, 0, 0
			w.Own = func(context.Context, string) (bool, error) {
				if held {
					t.Error("ownership preparation under admission")
				}
				return true, nil
			}
			w.Admit = func(context.Context, string) (func(), error) {
				if mode == "denied" {
					return nil, errors.New("retiring")
				}
				held = true
				return func() { held = false; releases++ }, nil
			}
			w.Open = func(context.Context, string) (*timer.Store, error) {
				opens++
				if !held {
					t.Error("open outside admission")
				}
				if mode == "open-error" {
					return nil, errors.New("open failed")
				}
				return st, nil
			}
			w.Commit = func(_ context.Context, _ string, fn func() error) error {
				commits++
				if !held {
					t.Error("commit outside admission")
				}
				return fn()
			}
			n, _, err := w.Pass(t.Context())
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
