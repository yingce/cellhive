package admission

import (
	"testing"
	"time"
)

func TestLimiterBurstAndRefill(t *testing.T) {
	l := New(10, 3) // 10/s, burst 3
	now := time.Unix(0, 0)
	l.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		if !l.Allow("acme") {
			t.Fatalf("burst allow %d denied", i)
		}
	}
	if l.Allow("acme") {
		t.Fatal("expected shed after burst")
	}
	if l.Shed() != 1 {
		t.Fatalf("shed = %d, want 1", l.Shed())
	}
	// A different namespace has its own bucket.
	if !l.Allow("other") {
		t.Fatal("independent namespace denied")
	}
	// Refill 10/s: after 100ms one token is available.
	now = now.Add(100 * time.Millisecond)
	if !l.Allow("acme") {
		t.Fatal("expected refill token")
	}
}

func TestDisabledLimiter(t *testing.T) {
	l := New(0, 0)
	if l.Enabled() {
		t.Fatal("rate 0 should disable")
	}
	for i := 0; i < 100; i++ {
		if !l.Allow("acme") {
			t.Fatal("disabled limiter denied")
		}
	}
}
