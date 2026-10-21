package hotset

import (
	"reflect"
	"testing"
	"time"
)

func stats(n int64) Stats { return Stats{Requests: n} }

func TestSelectFillsTopK(t *testing.T) {
	now := time.Now()
	d := Select(Policy{Max: 2, Window: time.Minute}, map[string]Stats{
		"a": stats(100), "b": stats(50), "c": stats(10),
	}, nil, now)
	if !reflect.DeepEqual(d.Static, []string{"a", "b"}) || !d.Render {
		t.Fatalf("decision = %+v", d)
	}
}

func TestToleranceKeepsSet(t *testing.T) {
	now := time.Now()
	prev := []string{"a", "b", "c", "d"}
	// e slightly above d but overlapping 3/4 with slack 1: noise.
	d := Select(Policy{Max: 4, Window: time.Minute}, map[string]Stats{
		"a": stats(100), "b": stats(90), "c": stats(80), "d": stats(70), "e": stats(71),
	}, prev, now)
	if d.Render || !reflect.DeepEqual(d.Static, prev) {
		t.Fatalf("tolerance decision = %+v", d)
	}
}

func TestDominantChallengerSwaps(t *testing.T) {
	now := time.Now()
	prev := []string{"a", "b"}
	d := Select(Policy{Max: 2, Window: time.Minute, MinQPS: 0}, map[string]Stats{
		"a": stats(10), "b": stats(20), "c": stats(100),
	}, prev, now)
	if !d.Render || !reflect.DeepEqual(d.Static, []string{"b", "c"}) {
		t.Fatalf("swap decision = %+v", d)
	}
}

func TestSwapRatioBoundsChanges(t *testing.T) {
	now := time.Now()
	prev := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	statsMap := map[string]Stats{"a": stats(10), "b": stats(10), "c": stats(10), "d": stats(10), "e": stats(10), "f": stats(10), "g": stats(10), "h": stats(10), "i": stats(10), "j": stats(10)}
	for k := 0; k < 5; k++ {
		statsMap[string(rune('k'+k))] = stats(1000)
	}
	d := Select(Policy{Max: 10, Window: time.Minute, SwapRatio: 0.1}, statsMap, prev, now)
	if len(d.Add) != 1 || len(d.Remove) != 1 {
		t.Fatalf("swap ratio decision = %+v", d)
	}
}

func TestDeployStabilityAndFloor(t *testing.T) {
	now := time.Now()
	d := Select(Policy{Max: 2, Window: time.Minute, MinQPS: 1, DeployStability: 5 * time.Minute}, map[string]Stats{
		"fresh": {Requests: 1000, LastDeploy: now.Add(-time.Minute)},
		"idle":  stats(0),
		"hot":   stats(100),
	}, nil, now)
	if !reflect.DeepEqual(d.Static, []string{"hot"}) {
		t.Fatalf("stability/floor decision = %+v", d)
	}
}

func TestStaticDisabled(t *testing.T) {
	d := Select(Policy{Max: 0}, nil, []string{"a", "b"}, time.Now())
	if !d.Render || len(d.Remove) != 2 || len(d.Static) != 0 {
		t.Fatalf("disabled decision = %+v", d)
	}
}
