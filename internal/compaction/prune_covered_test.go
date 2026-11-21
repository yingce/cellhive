package compaction

import (
	"path"
	"strings"
	"testing"
)

func TestPruneCoveredBelowL9RemovesDominatedLowerLevels(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "prune-covered"
	appendNativeAt(t, m, s, 1, 9, 1, 10, 1, 1, 2)
	appendNativeAt(t, m, s, 1, 0, 1, 3, 2, 1)   // covered by L9
	appendNativeAt(t, m, s, 1, 1, 4, 8, 3, 1)   // covered
	appendNativeAt(t, m, s, 1, 2, 9, 10, 4, 1)  // covered (max==L9.max)
	appendNativeAt(t, m, s, 1, 0, 11, 11, 5, 1) // NOT covered

	n, err := PruneCoveredBelowL9(t.Context(), m, s, 1, Options{})
	if err != nil || n != 0 {
		t.Fatalf("without IdleDeleteOK pruned=%d err=%v, want 0", n, err)
	}
	n, err = PruneCoveredBelowL9(t.Context(), m, s, 1, Options{IdleDeleteOK: true})
	if err != nil || n != 3 {
		t.Fatalf("pruned=%d err=%v, want 3", n, err)
	}
	for _, level := range []uint16{0, 1, 2} {
		keys, err := m.ListLevelFrom(t.Context(), s, 1, level, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			_, max, ok := l9Span(key)
			if !ok {
				t.Fatalf("bad key %s", key)
			}
			if max <= 10 {
				t.Fatalf("covered object survived at L%d: %s", level, key)
			}
		}
	}
	l0, err := m.ListLevelFrom(t.Context(), s, 1, 0, 0)
	if err != nil || len(l0) != 1 || !strings.Contains(path.Base(l0[0]), "000000000000000b") {
		t.Fatalf("tail L0 missing: %v %v", l0, err)
	}
}

func TestPruneIdleEpochRunsL9AndCovered(t *testing.T) {
	m, s := nativeManager(t)
	s.ID = "prune-idle"
	appendNativeAt(t, m, s, 1, 9, 1, 5, 1, 1, 2)
	appendNativeAt(t, m, s, 1, 9, 1, 3, 2, 1, 2)
	appendNativeAt(t, m, s, 1, 0, 1, 2, 3, 1)

	l9n, covn, err := PruneIdleEpoch(t.Context(), m, s, 1, Options{})
	if err != nil || l9n != 0 || covn != 0 {
		t.Fatalf("without IdleDeleteOK l9=%d covered=%d err=%v", l9n, covn, err)
	}
	l9n, covn, err = PruneIdleEpoch(t.Context(), m, s, 1, Options{IdleDeleteOK: true})
	if err != nil || l9n != 1 || covn != 1 {
		t.Fatalf("l9=%d covered=%d err=%v", l9n, covn, err)
	}
}
