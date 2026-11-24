package server

import (
	"testing"
	"time"
)

func TestCleanupNotificationsProjectButReceivedNudgesDoNot(t *testing.T) {
	calls := 0
	s := Server{Deps: Deps{ProjectMaintenance: func(kind string, at time.Time) {
		calls++
		if kind != "resource-cleanup" || at.IsZero() {
			t.Errorf("invalid projection %s %v", kind, at)
		}
	}}, resourceCleanupWake: make(chan struct{}, 1)}
	s.notifyResourceCleanup()
	if calls != 1 || len(s.resourceCleanupWake) != 1 {
		t.Fatal("mutation did not publish and wake")
	}
	<-s.resourceCleanupWake
	s.NudgeResourceCleanup()
	if calls != 1 || len(s.resourceCleanupWake) != 1 {
		t.Fatal("received hint republished or failed to wake")
	}
}
