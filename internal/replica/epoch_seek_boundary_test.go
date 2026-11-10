package replica

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

// seekInventoryBucket models provider lexicographic pagination without creating
// 100000 local files. Keys describe real native ranges; planning needs no body.
type seekInventoryBucket struct {
	bucket.Bucket
	keys          []string
	calls, listed int
}

func (b *seekInventoryBucket) Get(context.Context, string) ([]byte, string, error) {
	return nil, "", bucket.ErrNotFound
}
func (b *seekInventoryBucket) CommonPrefixes(_ context.Context, prefix, delimiter string) ([]string, error) {
	set := map[string]bool{}
	for _, key := range b.keys {
		if rest, ok := strings.CutPrefix(key, prefix); ok {
			if i := strings.Index(rest, delimiter); i >= 0 {
				set[prefix+rest[:i+len(delimiter)]] = true
			}
		}
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out, nil
}
func (b *seekInventoryBucket) ListPage(_ context.Context, prefix, after string, limit int) ([]bucket.ObjectInfo, string, error) {
	b.calls++
	at := sort.Search(len(b.keys), func(i int) bool { return b.keys[i] >= prefix && b.keys[i] > after })
	out := make([]bucket.ObjectInfo, 0, limit)
	for at < len(b.keys) && strings.HasPrefix(b.keys[at], prefix) && len(out) < limit {
		out = append(out, bucket.ObjectInfo{Key: b.keys[at]})
		at++
	}
	b.listed += len(out)
	next := ""
	if at < len(b.keys) && strings.HasPrefix(b.keys[at], prefix) {
		next = out[len(out)-1].Key
	}
	return out, next, nil
}

// This records the exhaustive baseline rather than asserting that an unsafe
// minimum-TXID seek is an optimization. Covered starts do not prove covered ends.
func TestRestoreSeekInventoryBaseline100000(t *testing.T) {
	for _, tail := range []int{0, 3} {
		t.Run(fmt.Sprint(tail), func(t *testing.T) {
			sc := cell.Scope{Namespace: "n", Class: "c", ID: "inventory"}
			b := &seekInventoryBucket{}
			for i := 1; i <= 100000+tail; i++ {
				b.keys = append(b.keys, fmt.Sprintf("%s0000/%016x-%016x.ltx", EpochPrefix(sc, 1), i, i))
			}
			b.keys = append(b.keys, fmt.Sprintf("%s0009/%016x-%016x.ltx", EpochPrefix(sc, 1), 1, 100000))
			sort.Strings(b.keys)
			plan, err := New(b).nativeRestorePlan(t.Context(), sc, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan) != tail+1 || plan[len(plan)-1].max != uint64(100000+tail) {
				t.Fatalf("plan=%+v", plan)
			}
			t.Logf("covered=100000 tail=%d provider_pages=%d listed_keys=%d", tail, b.calls, b.listed)
		})
	}
}

// A lexicographic predecessor search alone is insufficient: a long crossing
// range can precede arbitrarily many short, fully covered ranges. Exercise the
// real publisher and planner so this cannot silently become a singleton-L0 rule.
func TestRestoreSeekCannotDiscardEarlierCrossingRange(t *testing.T) {
	for _, far := range []bool{false, true} {
		t.Run(fmt.Sprint(far), func(t *testing.T) {
			m := newManager(t)
			sc := cell.Scope{Namespace: "n", Class: "c", ID: "crossing"}
			putNativeEpochFile(t, m, sc, 1, 9, 1, 90000, false)
			start := uint64(89900)
			if far {
				start = 2
			}
			crossing, _, err := m.AppendLevel(t.Context(), sc, 1, 0, nativeBlob(t, start, 90100, 7))
			if err != nil {
				t.Fatal(err)
			}
			putNativeEpochFile(t, m, sc, 1, 0, 89999, 89999, false)
			putNativeEpochFile(t, m, sc, 1, 0, 90000, 90000, false)
			plan, err := m.nativeRestorePlan(t.Context(), sc, 1)
			if err != nil || len(plan) != 2 || plan[1].key != crossing {
				t.Fatalf("crossing plan=%+v error=%v", plan, err)
			}
			after, err := m.ListLevelFrom(t.Context(), sc, 1, 0, 90001)
			if err != nil || len(after) != 0 {
				t.Fatalf("seek=%v error=%v", after, err)
			}
		})
	}
}

// Exhaustively check all subsets of short intervals against an independent
// fixed-point coverage oracle. Non-monotone ends, overlapping ranges, no tail,
// and gaps all occur; any discovery optimization must preserve these outcomes.
func TestRestoreRangeCoverageExhaustiveOracle(t *testing.T) {
	var universe []epochFile
	for lo := uint64(1); lo <= 5; lo++ {
		for hi := lo; hi <= 5; hi++ {
			universe = append(universe, epochFile{key: fmt.Sprintf("%d-%d", lo, hi), min: lo, max: hi})
		}
	}
	for mask := 0; mask < 1<<len(universe); mask++ {
		files := []epochFile{{key: "snapshot", level: 9, min: 1, max: 3}}
		want, maxEnd := uint64(3), uint64(3)
		for i, f := range universe {
			if mask&(1<<i) != 0 {
				files = append(files, f)
				maxEnd = max(maxEnd, f.max)
			}
		}
		for {
			next := want
			for _, f := range files {
				if f.min <= want+1 {
					next = max(next, f.max)
				}
			}
			if next == want {
				break
			}
			want = next
		}
		plan, err := planEpochFiles(files)
		if want != maxEnd {
			if err == nil {
				t.Fatalf("mask=%x accepted gap", mask)
			}
			continue
		}
		if err != nil || len(plan) == 0 || plan[len(plan)-1].max != want {
			t.Fatalf("mask=%x plan=%+v err=%v want=%d", mask, plan, err, want)
		}
	}
}

func TestHistoricalEpochSkips100000CoveredL0(t *testing.T) {
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "historical"}
	b := &seekInventoryBucket{}
	for i := 1; i <= 100000; i++ {
		b.keys = append(b.keys, fmt.Sprintf("%s0000/%016x-%016x.ltx", EpochPrefix(sc, 1), i, i))
	}
	b.keys = append(b.keys, fmt.Sprintf("%s0009/%016x-%016x.ltx", EpochPrefix(sc, 1), 1, 100000), fmt.Sprintf("%s0000/%016x-%016x.ltx", EpochPrefix(sc, 2), 100001, 100001))
	sort.Strings(b.keys)
	plan, err := New(b).nativeRestorePlan(t.Context(), sc, 2)
	if err != nil || len(plan) != 2 || plan[1].max != 100001 {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	if b.calls != 2 || b.listed != 2 {
		t.Fatalf("covered historical inventory scanned: pages=%d keys=%d", b.calls, b.listed)
	}
}

func TestHistoricalEpochDiscoveryRetainsCrossingTailAndGapFill(t *testing.T) {
	for _, kind := range []string{"crossing", "gap", "beyond-cut"} {
		t.Run(kind, func(t *testing.T) {
			sc := cell.Scope{Namespace: "n", Class: "c", ID: kind}
			b := &seekInventoryBucket{}
			add := func(epoch uint64, level int, lo, hi uint64) {
				b.keys = append(b.keys, fmt.Sprintf("%s%04x/%016x-%016x.ltx", EpochPrefix(sc, epoch), level, lo, hi))
			}
			add(1, 9, 1, 90)
			add(2, 0, 101, 101)
			switch kind {
			case "crossing":
				add(1, 0, 2, 100)
				add(1, 0, 89, 89)
			case "gap":
				add(1, 3, 95, 100)
				add(1, 0, 91, 94)
			case "beyond-cut":
				add(1, 3, 91, 110)
				add(1, 0, 91, 100)
			}
			sort.Strings(b.keys)
			m := New(b)
			plan, err := m.nativeRestorePlan(t.Context(), sc, 2)
			if err != nil || len(plan) < 3 || plan[len(plan)-1].max != 101 {
				t.Fatalf("plan=%+v err=%v", plan, err)
			}
			if b.listed != len(b.keys) {
				t.Fatalf("unproven lower level skipped: listed=%d keys=%d", b.listed, len(b.keys))
			}
		})
	}
}

func TestHistoricalEpochMalformedSnapshotCannotAuthorizeSkip(t *testing.T) {
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "malformed"}
	b := &seekInventoryBucket{keys: []string{
		fmt.Sprintf("%s0009/%016x-%016x.ltx", EpochPrefix(sc, 1), 2, 100),
		fmt.Sprintf("%s0000/%016x-%016x.ltx", EpochPrefix(sc, 1), 1, 100),
		fmt.Sprintf("%s0000/%016x-%016x.ltx", EpochPrefix(sc, 2), 101, 101),
	}}
	sort.Strings(b.keys)
	if plan, err := New(b).nativeRestorePlan(t.Context(), sc, 2); err == nil {
		t.Fatalf("invalid L9 accepted: %+v", plan)
	}
	if b.listed != 3 {
		t.Fatal("malformed high-level snapshot authorized skip")
	}
}
