package timer

import "testing"

func TestRegistryProducerUpdateSurvivesRunnerRearm(t *testing.T) {
	r := NewRegistry()
	r.Arm("s", 1000)
	_, gen, ok := r.snapshot("s")
	if !ok {
		t.Fatal("scope missing")
	}
	// A producer commits a timer and makes the scope runnable while the runner
	// is between NextDue and its re-arm.
	r.Add("s")
	if due, _, _ := r.snapshot("s"); due != 0 {
		t.Fatalf("Add(0) due = %d, want immediate", due)
	}
	// The runner's stale generation must not overwrite the producer's Add.
	r.setIfUnchanged("s", gen, 2000)
	if due, _, _ := r.snapshot("s"); due != 0 {
		t.Fatalf("stale setIfUnchanged overwrote Add(0): due=%d", due)
	}
	// A current-generation re-arm is allowed.
	_, cur, _ := r.snapshot("s")
	r.setIfUnchanged("s", cur, 3000)
	if due, _, _ := r.snapshot("s"); due != 3000 {
		t.Fatalf("current setIfUnchanged due = %d, want 3000", due)
	}
}

func TestRegistryRemoveIfPreservesConcurrentAdd(t *testing.T) {
	r := NewRegistry()
	r.Arm("s", 1000)
	_, gen, _ := r.snapshot("s")
	r.Add("s")
	r.removeIf("s", gen)
	if _, _, ok := r.snapshot("s"); !ok {
		t.Fatal("stale removeIf dropped a scope re-added by a producer")
	}
	_, cur, _ := r.snapshot("s")
	r.removeIf("s", cur)
	if _, _, ok := r.snapshot("s"); ok {
		t.Fatal("removeIf with current generation did not remove")
	}
}
