package lease

import (
	"cellhive/internal/bucket"
	"context"
	"errors"
	"testing"
	"time"
)

type discoveryBucket struct {
	bucket.Bucket
	reads int
	fail  bool
}

func (b *discoveryBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.reads++
	if b.fail {
		return nil, "", errors.New("unavailable")
	}
	return b.Bucket.Get(ctx, key)
}
func TestDiscoveryLeaseObservationIsNotTakeoverProof(t *testing.T) {
	fs, _ := bucket.NewFSBucket(t.TempDir())
	b := &discoveryBucket{Bucket: fs}
	m := NewManager(b, "self", "s", "", "", 10*time.Second)
	putKnownTest(t, fs, NodeLease{Node: "peer", Session: "s1", Expiry: time.Now().Add(time.Minute).UnixMilli()})
	if _, ok, err := m.NodeLeaseForDiscovery(t.Context(), "peer"); err != nil || !ok {
		t.Fatal(ok, err)
	}
	first := b.reads
	for i := 0; i < 20; i++ {
		if _, ok, err := m.NodeLeaseForDiscovery(t.Context(), "peer"); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	if b.reads != first {
		t.Fatal("fresh live observation re-read")
	}
	if _, _, err := m.NodeLeaseFresh(t.Context(), "peer"); err != nil {
		t.Fatal(err)
	}
	if b.reads != first+1 {
		t.Fatal("takeover fresh read was cached")
	}
	// Old observations and near-expiry values never suppress a point read.
	m.mu.Lock()
	m.observed["peer"] = time.Now().Add(-6 * time.Second)
	m.mu.Unlock()
	b.fail = true
	if _, _, err := m.NodeLeaseForDiscovery(t.Context(), "peer"); err == nil {
		t.Fatal("stale fallback")
	}
	b.fail = false
	putKnownTest(t, fs, NodeLease{Node: "peer", Session: "s2", Expiry: time.Now().Add(time.Second).UnixMilli()})
	if _, _, err := m.NodeLeaseFresh(t.Context(), "peer"); err != nil {
		t.Fatal(err)
	}
	prior := b.reads
	if _, _, err := m.NodeLeaseForDiscovery(t.Context(), "peer"); err != nil {
		t.Fatal(err)
	}
	if b.reads != prior+1 {
		t.Fatal("near expiry lease reused")
	}
	// A lease that expired after observation must be read again before recovery.
	putKnownTest(t, fs, NodeLease{Node: "peer", Session: "s3", Expiry: time.Now().Add(-time.Second).UnixMilli()})
	if _, _, err := m.NodeLeaseFresh(t.Context(), "peer"); err != nil {
		t.Fatal(err)
	}
	b.fail = true
	if _, _, err := m.NodeLeaseForDiscovery(t.Context(), "peer"); err == nil {
		t.Fatal("cached expiry used as death evidence")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := m.NodeLeaseForDiscovery(ctx, "peer"); err != context.Canceled {
		t.Fatal(err)
	}
}

// Discovery and independent maintenance share the same bounded observation.
// Reusing an observation must neither move its timestamp nor delay Fresh reads.
func TestBackgroundReadersShareBoundedLeaseObservation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		age, remaining time.Duration
		reads          int
	}{
		{"within_half_ttl", 3 * time.Second, 8 * time.Second, 0},
		{"too_old", 6 * time.Second, 8 * time.Second, 1},
		{"near_expiry", time.Millisecond, time.Second, 1},
		{"future_observation", -time.Second, 8 * time.Second, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _ := bucket.NewFSBucket(t.TempDir())
			b := &discoveryBucket{Bucket: fs}
			m := NewManager(b, "self", "s", "", "", 10*time.Second)
			old := NodeLease{Node: "peer", Session: "old", Expiry: time.Now().Add(tc.remaining).UnixMilli()}
			next := old
			next.Session = "new"
			next.Expiry = time.Now().Add(10 * time.Second).UnixMilli()
			putKnownTest(t, fs, next)
			observed := time.Now().Add(-tc.age)
			m.cached = []NodeLease{old}
			m.observed = map[string]time.Time{"peer": observed}
			m.refreshKnown(t.Context())
			if b.reads != tc.reads {
				t.Fatalf("background GETs=%d want %d", b.reads, tc.reads)
			}
			if _, ok, err := m.NodeLeaseForDiscovery(t.Context(), "peer"); err != nil || !ok {
				t.Fatal(ok, err)
			}
			want := tc.reads
			// Future observations cannot be overwritten by an older response:
			// keep forcing authority reads rather than extending stale proof.
			if tc.age < 0 {
				want++
			}
			if b.reads != want {
				t.Fatalf("GETs=%d want %d", b.reads, want)
			}
			if tc.reads == 0 && !m.observed["peer"].Equal(observed) {
				t.Fatal("reuse extended observation")
			}
			fresh, ok, err := m.NodeLeaseFresh(t.Context(), "peer")
			if err != nil || !ok || fresh.Session != "new" || b.reads != want+1 {
				t.Fatalf("fresh=%+v ok=%v err=%v reads=%d", fresh, ok, err, b.reads)
			}
		})
	}
}
