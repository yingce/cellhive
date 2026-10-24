package lease

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

// TestHasShedTarget covers the pressure check (ADR-123): a node only refuses new
// claims when a live peer with placement headroom can take them.
func TestHasShedTarget(t *testing.T) {
	now := time.Now()
	live := func(node string, owned, weight int) NodeLease {
		return NodeLease{Node: node, Expiry: now.Add(time.Minute).UnixMilli(),
			Load: Load{OwnedCells: owned, Weight: weight}}
	}
	// Only self: no target (a single node never sheds).
	if HasShedTarget([]NodeLease{live("self", 100, 8)}, "self", now) {
		t.Fatal("single node reported a shed target")
	}
	// A live peer with headroom (owned < weight): shed.
	if !HasShedTarget([]NodeLease{live("self", 100, 8), live("peer", 2, 8)}, "self", now) {
		t.Fatal("peer with headroom not detected")
	}
	// A peer that is itself pressured, full, or expired: no target.
	pressured := live("peer", 1, 8)
	pressured.Load.Pressured = true
	full := live("peer", 8, 8)
	expired := live("peer", 1, 8)
	expired.Expiry = now.Add(-time.Minute).UnixMilli()
	for _, l := range []NodeLease{pressured, full, expired} {
		if HasShedTarget([]NodeLease{live("self", 100, 8), l}, "self", now) {
			t.Fatalf("unexpected shed target: %+v", l)
		}
	}
	// Weight 0 is treated as 1, so a peer owning 0 cells still has headroom.
	zero := NodeLease{Node: "peer", Expiry: now.Add(time.Minute).UnixMilli(), Load: Load{OwnedCells: 0, Weight: 0}}
	if !HasShedTarget([]NodeLease{zero}, "self", now) {
		t.Fatal("zero-weight peer without cells should have headroom")
	}
}

type probeBucket struct {
	bucket.Bucket
	probe func()
}

func (b *probeBucket) List(ctx context.Context, prefix string) ([]string, error) {
	b.probe()
	return b.Bucket.List(ctx, prefix)
}

// TestSampleCachedDoesNotHoldLockDuringIO is the regression for holding mu across
// the bucket round trip, which serialized every caller behind the slowest one.
func TestSampleCachedDoesNotHoldLockDuringIO(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	m := &Manager{B: base, NodeID: "n1", Session: "s1", TTL: time.Minute}
	var held atomic.Bool
	m.B = &probeBucket{Bucket: base, probe: func() {
		if !m.mu.TryLock() {
			held.Store(true)
			return
		}
		m.mu.Unlock()
	}}
	if got := m.SampleCached(context.Background(), time.Minute); got == nil {
		t.Fatalf("SampleCached returned nil")
	}
	if held.Load() {
		t.Fatalf("SampleCached held mu during bucket I/O")
	}
}

func TestSampleCachedServesStaleWhileRefreshing(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{B: base, NodeID: "n1", Session: "s1", TTL: time.Minute}
	if got := m.SampleCached(context.Background(), time.Millisecond); got == nil {
		t.Fatal("first sample should populate the cache")
	}
	block := make(chan struct{})
	release := make(chan struct{})
	m.B = &probeBucket{Bucket: base, probe: func() {
		close(block)
		<-release
	}}
	time.Sleep(5 * time.Millisecond) // expire the cache
	start := time.Now()
	got := m.SampleCached(context.Background(), time.Millisecond)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("SampleCached blocked on refresh for %s", time.Since(start))
	}
	if got == nil {
		t.Fatal("stale snapshot was not returned")
	}
	select {
	case <-block:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not start")
	}
	close(release)
}

func TestHealthyRequiresPublish(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{B: base, NodeID: "n1", Session: "s1", TTL: time.Second, renewKey: Key("n1")}
	if m.Healthy(time.Now()) {
		t.Fatal("unpublished manager must not claim to be healthy")
	}
	if err := m.Publish(context.Background(), Load{}); err != nil {
		t.Fatal(err)
	}
	if !m.Healthy(time.Now()) {
		t.Fatal("published manager should be healthy")
	}
	if m.Healthy(time.Now().Add(2 * time.Second)) {
		t.Fatal("manager stayed healthy past its lease TTL")
	}
}

func TestNodeLeaseUsesSnapshotAndPointRead(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(base, "n1", "s1", "addr", "", time.Minute)
	if err := m.Publish(context.Background(), Load{}); err != nil {
		t.Fatal(err)
	}
	l, ok, err := m.NodeLease(context.Background(), "n1")
	if err != nil || !ok || l.Session != "s1" {
		t.Fatalf("self lease = %+v ok=%v err=%v", l, ok, err)
	}
	if _, ok, err := m.NodeLease(context.Background(), "missing"); err != nil || ok {
		t.Fatalf("missing lease ok=%v err=%v", ok, err)
	}
}

// A slow PUT must not extend the window in which the node keeps acking: Healthy
// is measured from the Expiry that peers see, not from when the PUT returned.
func TestHealthyMeasuresFromPublishedExpiry(t *testing.T) {
	m := &Manager{TTL: 10 * time.Second}
	// A lease published at T0 expires at T0+10s. Even if its PUT only returned at
	// T0+3s, the node must stop acking before T0+10s-margin (margin = TTL/5).
	t0 := time.Now()
	m.publishedExpiry = t0.Add(10 * time.Second)
	if !m.Healthy(t0.Add(7 * time.Second)) {
		t.Fatal("expected healthy before expiry-margin")
	}
	if m.Healthy(t0.Add(8*time.Second + time.Millisecond)) {
		t.Fatal("must be unhealthy once within margin of the published expiry")
	}
}

func TestPublishNeverMovesExpiryBackwards(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(base, "n1", "s1", "addr", "", 10*time.Second)
	if err := m.Publish(context.Background(), Load{}); err != nil {
		t.Fatal(err)
	}
	first := m.publishedExpiry
	m.TTL = time.Second // a shorter TTL must not shrink what peers already saw
	if err := m.Publish(context.Background(), Load{}); err != nil {
		t.Fatal(err)
	}
	if m.publishedExpiry.Before(first) {
		t.Fatalf("publishedExpiry moved backwards: %v -> %v", first, m.publishedExpiry)
	}
}

func TestLegacyPeersDetection(t *testing.T) {
	now := time.Now()
	live := now.Add(time.Minute).UnixMilli()
	dead := now.Add(-time.Minute).UnixMilli()
	leases := []NodeLease{
		{Node: "self", Expiry: live},
		{Node: "new", Expiry: live, Liveness: NodeLeaseLiveness},
		{Node: "old-live", Expiry: live},
		{Node: "old-fence", Expiry: live, Liveness: "node-lease"},
		{Node: "old-dead", Expiry: dead},
	}
	got := LegacyPeers(leases, "self", now)
	if len(got) != 2 || got[0].Node != "old-live" || got[1].Node != "old-fence" {
		t.Fatalf("legacy peers = %+v, want old owner and fence protocols", got)
	}
	m := &Manager{NodeID: "self", cached: leases}
	if !m.LegacyPeerVisible(now) {
		t.Fatal("legacy peer in snapshot must be visible")
	}
	m.cached = leases[:2]
	if m.LegacyPeerVisible(now) {
		t.Fatal("no legacy peer expected")
	}
}

func TestNodeLeaseFreshBypassesSnapshot(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	other := NewManager(base, "n2", "s2", "addr", "", time.Minute)
	if err := other.Publish(context.Background(), Load{}); err != nil {
		t.Fatal(err)
	}
	m := NewManager(base, "n1", "s1", "addr", "", time.Minute)
	m.cached, m.cachedAt = []NodeLease{}, time.Now() // stale snapshot that lacks n2
	l, ok, err := m.NodeLeaseFresh(context.Background(), "n2")
	if err != nil || !ok || l.Session != "s2" {
		t.Fatalf("fresh read = %+v ok=%v err=%v", l, ok, err)
	}
	if _, ok, err := m.NodeLeaseFresh(context.Background(), "nobody"); err != nil || ok {
		t.Fatalf("missing node ok=%v err=%v", ok, err)
	}
}
