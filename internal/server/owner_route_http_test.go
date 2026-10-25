package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/control"
	"cellhive/internal/lease"
	"cellhive/internal/owner"
)

type routeHTTPBucket struct {
	bucket.Bucket
	owners atomic.Int64
	nodes  atomic.Int64
}

func (b *routeHTTPBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasSuffix(key, "/owner.json") {
		b.owners.Add(1)
		select {
		case <-time.After(15 * time.Millisecond):
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	if strings.HasPrefix(key, "nodes/") {
		b.nodes.Add(1)
	}
	return b.Bucket.Get(ctx, key)
}

// Real loopback HTTP, two independent SQLite stores, shared filesystem bucket.
// The 15ms injected owner GET cost isolates routing from COS variability; this
// is not a production COS/fleet throughput benchmark.
func TestOwnerRouteHTTP(t *testing.T) {
	dir := t.TempDir()
	a, oa, ha := newControlForwardServer(t, dir, "a")
	b, ob, hb := newControlForwardServer(t, dir, "b")
	oa.Advertise = ha.URL
	ob.Advertise = hb.URL
	counts := make([]*routeHTTPBucket, 0, 2)
	for _, s := range []*Server{a, b} {
		s.Cfg.BindingCacheTTL = 30 * time.Second
		lm := lease.NewManager(s.Bucket, s.Owner.NodeID, s.Owner.Session, "", "", 30*time.Second)
		if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
			t.Fatal(err)
		}
		s.Lease = lm
		s.Owner.LeaseView = lm
		counted := &routeHTTPBucket{Bucket: s.Bucket}
		counts = append(counts, counted)
		s.Owner.B = counted
		lm.B = counted
		if err := registerTestBinding(s, "route-http", "kv", "KV", "main"); err != nil {
			t.Fatal(err)
		}
		s.Capture = &cellcapture.Manager{Store: s.Store, Committer: replicaCommitter{rep: s.Replica}, AutoSnapshot: true,
			Owner: func(ctx context.Context, sc cell.Scope) (bool, uint64, error) {
				o, _, err := s.Owner.ResolveRoute(ctx, sc)
				if err != nil {
					return false, 0, err
				}
				live, err := s.Owner.LiveRoute(ctx, sc, o, time.Now())
				return live && o.Node == s.Owner.NodeID && o.Session == s.Owner.Session, o.Epoch, err
			}}
		t.Cleanup(func() { s.Capture.Stop(); s.WaitBackground(); _ = s.Store.Close() })
	}
	sc := cell.Scope{Namespace: "route-http", Class: "__kv__", ID: "main"}
	for _, scope := range []cell.Scope{control.Scope(), sc} {
		if _, err := oa.Claim(t.Context(), scope, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	token := mint(t, a, "route-http", "kv", "KV")
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	call := func(base, method, value string) time.Duration {
		path := "/v1/kv/get?ns=route-http&key=k"
		if method == http.MethodPost {
			path = "/v1/kv/put?ns=route-http&key=k"
		}
		req, err := http.NewRequestWithContext(t.Context(), method, base+path, strings.NewReader(value))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("x-cellhive-scope-token", token)
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		elapsed := time.Since(start)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("%s %s = %d %s %v", base, method, resp.StatusCode, data, err)
		}
		if method == http.MethodGet && string(data) != value {
			t.Fatalf("read %q want %q", data, value)
		}
		return elapsed
	}
	for _, base := range []string{ha.URL, hb.URL} {
		call(base, http.MethodPost, "warm")
		call(base, http.MethodGet, "warm")
	}
	before := []int64{counts[0].owners.Load(), counts[1].owners.Load()}
	nodes := []int64{counts[0].nodes.Load(), counts[1].nodes.Load()}
	samples := map[string][]time.Duration{}
	for round := 0; round < 3; round++ {
		time.Sleep(1100 * time.Millisecond)
		for i, base := range []string{ha.URL, hb.URL} {
			label := []string{"owner", "forwarded"}[i]
			value := fmt.Sprintf("%d/%d", round, i)
			for _, method := range []string{http.MethodPost, http.MethodGet} {
				d := call(base, method, value)
				samples[label+" "+method] = append(samples[label+" "+method], d)
			}
		}
	}
	for name, ds := range samples {
		t.Logf("spaced %s durations=%v", name, ds)
	}
	for i, c := range counts {
		if c.owners.Load() != before[i] || c.nodes.Load() != nodes[i] {
			t.Errorf("node %d warm lookups owner +%d node +%d; want zero", i, c.owners.Load()-before[i], c.nodes.Load()-nodes[i])
		}
	}
	for _, label := range []string{"owner", "forwarded"} {
		base := ha.URL
		if label == "forwarded" {
			base = hb.URL
		}
		for _, method := range []string{http.MethodPost, http.MethodGet} {
			ds := make([]time.Duration, 200)
			start := time.Now()
			for i := range ds {
				ds[i] = call(base, method, "2/1")
			}
			elapsed := time.Since(start)
			sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
			t.Logf("%s %s n=200 p50=%s p99=%s sequential_rps=%.1f", label, method, ds[99], ds[197], 200/elapsed.Seconds())
		}
	}
	// Explicit release must invalidate the former owner's route immediately.
	o, _, err := oa.Resolve(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := oa.Release(t.Context(), sc, o.Epoch); err != nil {
		t.Fatal(err)
	}
	if _, _, err := oa.ResolveRoute(t.Context(), sc); err != owner.ErrUnowned {
		t.Fatalf("released owner route: %v", err)
	}
	// B takes ownership; A still has old SQLite state but must route to B.
	a.Capture.Stop()
	next, err := ob.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if next.Epoch <= o.Epoch {
		t.Fatal("handoff did not advance epoch")
	}
	call(hb.URL, http.MethodPost, "after-handoff")
	call(ha.URL, http.MethodGet, "after-handoff")

}
