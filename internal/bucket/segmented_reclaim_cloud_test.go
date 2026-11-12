package bucket

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"
)

func testCloudAuthorityReclaim(t *testing.T, base *S3Bucket, raw PositionAppender) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	key := fmt.Sprintf("cells/itest-authority/__reclaim__/%d/own.json", time.Now().UnixNano())
	t.Cleanup(func() {
		clean, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		for _, k := range []string{key + ".g0", key + ".g1", key + ".g2", key} {
			if err := base.Delete(clean, k); err != nil {
				t.Error(err)
			}
		}
	})
	clock := &testClock{t: time.Unix(1700000000, 0)}
	opts := AuthorityOptions{ReclaimRetired: true, Segment: time.Minute, Now: clock.Now}
	writer := NewSegmentedAuthority(raw, base, opts)
	reader := NewSegmentedAuthority(raw, base, opts)
	tok, err := writer.ConditionalCreate(ctx, key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	genesis, _, err := base.Get(ctx, key+".g0")
	if err != nil {
		t.Fatal(err)
	}
	old, err := writer.latest(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.Get(ctx, key); err != nil {
		t.Fatal(err)
	}
	// Interleave cold/warm samples on the same stable record. These timings
	// describe this test host -> cloud endpoint, not production KV latency.
	var coldTimes, warmTimes []time.Duration
	for i := 0; i < 10; i++ {
		cold := NewSegmentedAuthority(raw, base, opts)
		for j, client := range []*SegmentedAuthority{cold, reader} {
			before := base.Stats()["get"]
			start := time.Now()
			got, _, err := client.Get(ctx, key)
			elapsed := time.Since(start)
			if err != nil || string(got) != "old" {
				t.Fatalf("sample value %q %v", got, err)
			}
			want := uint64(3)
			if j == 1 {
				want = 2
				warmTimes = append(warmTimes, elapsed)
			} else {
				coldTimes = append(coldTimes, elapsed)
			}
			if calls := base.Stats()["get"] - before; calls != want {
				t.Fatalf("cold/warm=%d get calls=%d want=%d", j, calls, want)
			}
		}
	}
	sort.Slice(coldTimes, func(i, j int) bool { return coldTimes[i] < coldTimes[j] })
	sort.Slice(warmTimes, func(i, j int) bool { return warmTimes[i] < warmTimes[j] })
	t.Logf("10 interleaved samples: cold 3 GET p50=%s; warm 2 GET p50=%s", coldTimes[4], warmTimes[4])
	clock.Advance(time.Minute)
	tok, err = writer.CAS(ctx, key, []byte("new"), tok)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := base.Get(ctx, key+".g0"); !errors.Is(err, ErrNotFound) {
		t.Fatal("predecessor not physically removed", err)
	}
	// Actual native append recreates a deleted predecessor, unlike stub mode.
	if err := raw.Append(ctx, key+".g0", 0, genesis); err != nil {
		t.Fatal(err)
	}
	delayed := NewSegmentedAuthority(raw, base, AuthorityOptions{ReclaimRetired: true, Segment: time.Minute, Now: func() time.Time { return time.Unix(1700000000, 0) }})
	if _, err := delayed.mutate(ctx, old, []byte("stale"), false); !errors.Is(err, ErrPrecondition) {
		t.Fatal("late write acknowledged", err)
	}
	got, current, err := reader.Get(ctx, key)
	if err != nil || string(got) != "new" || current != tok {
		t.Fatalf("resurrected read %q %v", got, err)
	}
	n, cursor, err := writer.CollectRetiredPage(ctx, key, "", 1)
	if err != nil || n != 1 || cursor == "" {
		t.Fatalf("collect first page %d %q %v", n, cursor, err)
	}
	// COS may return IsTruncated on an exactly full final page; follow its
	// cursor to the empty page instead of assuming g1 ends the listing.
	for page := 0; cursor != "" && page < 3; page++ {
		n, next, err := writer.CollectRetiredPage(ctx, key, cursor, 1)
		if err != nil || n != 0 || (next != "" && next <= cursor) {
			t.Fatalf("collect current page %d %q %v", n, next, err)
		}
		cursor = next
	}
	if cursor != "" {
		t.Fatal("collector did not finish cloud listing")
	}
	if err := writer.ConditionalDelete(ctx, key, tok); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatal("tombstone lost", err)
	}
	clock.Advance(time.Minute)
	if _, err := writer.ConditionalCreate(ctx, key, []byte("recreated-current")); err != nil {
		t.Fatal(err)
	}
	fresh := NewSegmentedAuthority(raw, base, opts)
	if got, _, err := fresh.Get(ctx, key); err != nil || string(got) != "recreated-current" {
		t.Fatalf("cold current %q %v", got, err)
	}
	if _, err := fresh.CAS(ctx, key, []byte("obsolete"), tok); !errors.Is(err, ErrPrecondition) {
		t.Fatal("old token accepted", err)
	}
	t.Log("retirement, late native append, stale ACK rejection, GC, tombstone and cold reopen passed")
}
