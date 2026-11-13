package bucket

import (
	"testing"
	"time"
)

func TestAuthorityReclaimMetrics(t *testing.T) {
	resetAuthorityStatsForTest()
	mem := newSegmentMem()
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := reclaimTestAuthority(mem, clock)
	key := "kv/metrics.json"
	tok, err := auth.ConditionalCreate(t.Context(), key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if _, err := auth.CAS(t.Context(), key, []byte("new"), tok); err != nil {
		t.Fatal(err)
	}
	stats := AuthorityStats()
	if stats["retired_deleted"] == 0 {
		t.Fatalf("retired deletion not counted: %+v", stats)
	}
	if stats["retired_delete_failed"] != 0 {
		t.Fatalf("unexpected deletion failure: %+v", stats)
	}
}
