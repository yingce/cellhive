package rebalance

import "testing"

func cands(n, idleN int) []Candidate {
	out := make([]Candidate, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, Candidate{Scope: string(rune('a' + i)), Epoch: 1, Idle: i < idleN})
	}
	return out
}

func TestPlanBelowTargetDoesNothing(t *testing.T) {
	self := Load{Node: "a", Weight: 1, Owned: 5}
	peers := []Load{{Node: "b", Weight: 1, Owned: 5}}
	if got := Plan(self, peers, cands(5, 5), 32); len(got) != 0 {
		t.Fatalf("moved %d, want 0", len(got))
	}
}

func TestPlanMovesToUnderloadedPeerWithHeadroom(t *testing.T) {
	self := Load{Node: "a", Weight: 1, Owned: 10}
	peers := []Load{{Node: "b", Weight: 1, Owned: 0}}
	got := Plan(self, peers, cands(10, 10), 32)
	// total target 5 each; over=5; peer room = 5*0.98 = 4.9 -> 4.
	if len(got) != 4 {
		t.Fatalf("moved %d, want 4 (2%% headroom)", len(got))
	}
	for _, c := range got {
		if !c.Idle {
			t.Fatalf("moved a busy cell %+v", c)
		}
	}
}

func TestPlanSkipsBusyAndNoIdle(t *testing.T) {
	self := Load{Node: "a", Weight: 1, Owned: 10}
	peers := []Load{{Node: "b", Weight: 1, Owned: 0}}
	got := Plan(self, peers, cands(10, 3), 32)
	if len(got) != 3 {
		t.Fatalf("moved %d, want only the 3 idle cells", len(got))
	}
	if got := Plan(self, peers, cands(10, 0), 32); len(got) != 0 {
		t.Fatalf("moved busy cells: %d", len(got))
	}
}

func TestPlanWeightProportional(t *testing.T) {
	self := Load{Node: "a", Weight: 3, Owned: 8}
	peers := []Load{{Node: "b", Weight: 1, Owned: 0}}
	// totalWeight 4, totalOwned 8 -> self target 6, over 2; peer target 2, room 1.96 -> 1.
	if got := Plan(self, peers, cands(8, 8), 32); len(got) != 1 {
		t.Fatalf("moved %d, want 1", len(got))
	}
}

func TestPlanCapsAndNoPeerRoom(t *testing.T) {
	self := Load{Node: "a", Weight: 1, Owned: 100}
	peers := []Load{{Node: "b", Weight: 1, Owned: 0}}
	if got := Plan(self, peers, cands(100, 100), 32); len(got) != 32 {
		t.Fatalf("moved %d, want maxMove 32", len(got))
	}
	// Peer already at its target -> no room -> nothing moves.
	full := []Load{{Node: "b", Weight: 1, Owned: 100}}
	if got := Plan(self, full, cands(100, 100), 32); len(got) != 0 {
		t.Fatalf("moved %d with a full peer, want 0", len(got))
	}
	if got := Plan(self, nil, cands(10, 10), 32); len(got) != 0 {
		t.Fatalf("moved %d with no peers, want 0", len(got))
	}
}

func TestPlanDeterministicOrder(t *testing.T) {
	self := Load{Node: "a", Weight: 1, Owned: 10}
	peers := []Load{{Node: "b", Weight: 1, Owned: 0}}
	unsorted := []Candidate{{Scope: "z", Idle: true}, {Scope: "m", Idle: true}, {Scope: "a", Idle: true}, {Scope: "b", Idle: true}}
	got := Plan(self, peers, unsorted, 2)
	if len(got) != 2 || got[0].Scope != "a" || got[1].Scope != "b" {
		t.Fatalf("order = %+v, want [a b]", got)
	}
}
