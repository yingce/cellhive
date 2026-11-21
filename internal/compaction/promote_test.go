package compaction

import "testing"

func TestShouldPromoteL1ByCountOrBytes(t *testing.T) {
	p := L1PromotePolicy(L1MinObjects, L1MinBytes)
	if ShouldPromote(Cue{Objects: 255, Bytes: L1MinBytes - 1}, p) {
		t.Fatal("255 objects under 64MiB must wait")
	}
	if !ShouldPromote(Cue{Objects: L1MinObjects}, p) {
		t.Fatal("256 objects must promote")
	}
	if !ShouldPromote(Cue{Objects: 1, Bytes: L1MinBytes}, p) {
		t.Fatal("64MiB must promote")
	}
}

func TestShouldPromoteEmptyCue(t *testing.T) {
	if ShouldPromote(Cue{}, L1PromotePolicy(L1MinObjects, L1MinBytes)) {
		t.Fatal("empty cue promoted")
	}
}
