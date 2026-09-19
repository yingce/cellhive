package lease

import (
	"testing"
	"time"
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
