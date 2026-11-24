package control

import (
	"context"
	"testing"
	"time"
)

func TestPurgeNotificationsProjectButReceivedNudgesDoNot(t *testing.T) {
	now := time.Now()
	calls := 0
	s := Store{now: func() time.Time { return now }, purgeWake: make(chan struct{}, 1), SchedulePurge: func(at time.Time) {
		calls++
		if !at.Equal(now) {
			t.Errorf("deadline=%v", at)
		}
	}}
	s.notifyPurge()
	if calls != 1 || len(s.purgeWake) != 1 {
		t.Fatal("mutation did not publish and wake")
	}
	<-s.purgeWake
	s.NudgePurge()
	if calls != 1 || len(s.purgeWake) != 1 {
		t.Fatal("received hint republished or failed to wake")
	}
}

func TestPurgeLoopProjectsIdleAndActiveDeadlines(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "active"}[active], func(t *testing.T) {
			s := newStore(t, false)
			now := time.Now()
			s.now = func() time.Time { return now }
			if active {
				if _, err := s.CreateApp(t.Context(), "demo", "test"); err != nil {
					t.Fatal(err)
				}
				if err := s.DeleteApp(t.Context(), "demo", "test"); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var next time.Time
			s.SchedulePurge = func(at time.Time) { next = at; cancel() }
			executor := &PurgeExecutor{Store: s, Holder: "test", Commit: func(_ context.Context, f func() error) error { return f() }, Hook: func(context.Context, string, string) (bool, error) { return false, nil }}
			s.RunPurgeLoop(ctx, nil, 5*time.Second, executor)
			delay := time.Minute
			if active {
				delay = 5 * time.Second
			}
			if !next.Equal(now.Add(delay)) {
				t.Fatalf("next=%v want=%v", next, now.Add(delay))
			}
		})
	}
}
