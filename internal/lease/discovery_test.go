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
	m.observed["peer"] = time.Now().Add(-2 * time.Second)
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
