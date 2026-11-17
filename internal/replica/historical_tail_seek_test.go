package replica

import (
	"fmt"
	"sort"
	"testing"

	"cellhive/internal/cell"
)

// A predecessor with a proven high-level prefix must not enumerate its
// 100000 covered L0 objects just to recover the final 21 transactions.
func TestHistoricalEpochSeeksUncompactedTail(t *testing.T) {
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "seek-tail"}
	b := &seekInventoryBucket{}
	for i := 1; i <= 100021; i++ {
		b.keys = append(b.keys, fmt.Sprintf("%s0000/%016x-%016x.ltx", EpochPrefix(sc, 1), i, i))
	}
	b.keys = append(b.keys,
		fmt.Sprintf("%s0001/%016x-%016x.ltx", EpochPrefix(sc, 1), 1, 100000),
		fmt.Sprintf("%s0000/%016x-%016x.ltx", EpochPrefix(sc, 2), 100022, 100022))
	sort.Strings(b.keys)
	plan, err := New(b).nativeRestorePlan(t.Context(), sc, 2)
	if err != nil || len(plan) != 23 || plan[0].min != 1 || plan[len(plan)-1].max != 100022 {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	for i := 1; i < len(plan); i++ {
		if plan[i].min != plan[i-1].max+1 {
			t.Fatalf("gap at %d: %+v", i, plan)
		}
	}
	if b.listed > 30 {
		t.Fatalf("covered inventory scanned: pages=%d keys=%d", b.calls, b.listed)
	}
}
