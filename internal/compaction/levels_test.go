package compaction

import (
	"testing"
	"time"
)

func TestCelldSchedule(t *testing.T) {
	want := []time.Duration{0, 30 * time.Second, 5 * time.Minute, time.Hour}
	for level, interval := range want {
		got, ok := CelldSchedule(level)
		if !ok || got.Level != level || got.Interval != interval {
			t.Fatalf("level %d = %+v, %v", level, got, ok)
		}
	}
	if _, ok := CelldSchedule(CelldSnapshotLevel); ok {
		t.Fatal("snapshot level must not be a non-snapshot schedule")
	}
}

func TestLevelSchedulesUseIndependentUTCBoundaries(t *testing.T) {
	now := time.Date(2026, 11, 8, 10, 4, 45, 0, time.FixedZone("offset", 8*3600))
	for level, want := range map[int]time.Duration{1: 15 * time.Second, 2: 15 * time.Second, 3: 55*time.Minute + 15*time.Second} {
		schedule, _ := CelldSchedule(level)
		if got := schedule.NextAt(now).Sub(now); got != want {
			t.Fatalf("level%d delay=%s want=%s", level, got, want)
		}
		boundary := schedule.NextAt(now)
		if schedule.NextAt(boundary).Sub(boundary) != schedule.Interval {
			t.Fatalf("level%d repeats past boundary", level)
		}
	}
}
