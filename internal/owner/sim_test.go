package owner

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

// fakeBucket is an in-memory object store with etag CAS semantics and a
// failure-injection hook, used to drive the owner fencing simulation.
type fakeBucket struct {
	mu      sync.Mutex
	data    map[string][]byte
	etag    map[string]string
	seq     int
	failCAS func() bool
}

func newFakeBucket() *fakeBucket {
	return &fakeBucket{data: map[string][]byte{}, etag: map[string]string{}}
}

func (b *fakeBucket) nextEtag() string { b.seq++; return fmt.Sprintf("e%d", b.seq) }

func (b *fakeBucket) Get(_ context.Context, key string) ([]byte, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.data[key]
	if !ok {
		return nil, "", bucket.ErrNotFound
	}
	return append([]byte(nil), d...), b.etag[key], nil
}

func (b *fakeBucket) RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	d, e, err := b.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	if off >= int64(len(d)) {
		return nil, e, nil
	}
	end := off + length
	if end > int64(len(d)) {
		end = int64(len(d))
	}
	return d[off:end], e, nil
}

func (b *fakeBucket) Put(_ context.Context, key string, data []byte) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data[key] = append([]byte(nil), data...)
	b.etag[key] = b.nextEtag()
	return b.etag[key], nil
}

func (b *fakeBucket) ConditionalCreate(_ context.Context, key string, data []byte) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.data[key]; ok {
		return "", bucket.ErrPrecondition
	}
	b.data[key] = append([]byte(nil), data...)
	b.etag[key] = b.nextEtag()
	return b.etag[key], nil
}

func (b *fakeBucket) CAS(_ context.Context, key string, data []byte, expect string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failCAS != nil && b.failCAS() {
		return "", bucket.ErrPrecondition // a lost race
	}
	cur, ok := b.data[key]
	if !ok {
		return "", bucket.ErrNotFound
	}
	if b.etag[key] != expect {
		return "", bucket.ErrPrecondition
	}
	_ = cur
	b.data[key] = append([]byte(nil), data...)
	b.etag[key] = b.nextEtag()
	return b.etag[key], nil
}

func (b *fakeBucket) ConditionalDelete(_ context.Context, key, expect string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	cur, ok := b.data[key]
	if !ok {
		return bucket.ErrPrecondition
	}
	if b.etag[key] != expect {
		return bucket.ErrPrecondition
	}
	delete(b.data, key)
	delete(b.etag, key)
	_ = cur
	return nil
}

func (b *fakeBucket) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.data, key)
	delete(b.etag, key)
	return nil
}

func (b *fakeBucket) List(_ context.Context, prefix string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for k := range b.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (b *fakeBucket) PresignGet(context.Context, string, time.Duration) (string, error) {
	return "", nil
}

// TestOwnerFencingSimulation is a deterministic simulation of the coordination
// core: several nodes claim/renew/release scopes against a lossy CAS store with
// per-node clock skew. Each seed replays exactly. It asserts the fencing
// invariants: a claim only succeeds once the prior lease is expired at the
// claimer's clock, epochs never regress, and a superseded owner can neither
// renew nor release (the fence holds).
func TestOwnerFencingSimulation(t *testing.T) {
	const seeds = 2000
	const steps = 300
	scopes := []cell.Scope{
		{Namespace: "a", Class: "__kv__", ID: "s1"},
		{Namespace: "a", Class: "__do__", ID: "s2"},
		{Namespace: "b", Class: "__kv__", ID: "s3"},
	}
	for seed := int64(0); seed < seeds; seed++ {
		if err := runSeed(seed, steps, scopes); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}

type simNode struct {
	mgr   *Manager
	skew  time.Duration
	owned map[string]uint64 // scope -> epoch (this node believes it owns)
}

func runSeed(seed int64, steps int, scopes []cell.Scope) error {
	rng := rand.New(rand.NewSource(seed))
	b := newFakeBucket()
	// Inject a lost CAS ~5% of the time so claim races are exercised.
	b.failCAS = func() bool { return rng.Intn(100) < 5 }

	nodes := make([]*simNode, 3)
	for i := range nodes {
		ttl := time.Duration(3+rng.Intn(6)) * time.Second
		nodes[i] = &simNode{
			mgr:   &Manager{B: b, NodeID: fmt.Sprintf("n%d", i), Session: fmt.Sprintf("s%d", i), Advertise: "addr", Role: cell.RoleCellAgent, OwnerTTL: ttl},
			skew:  time.Duration(rng.Intn(1001)-500) * time.Millisecond,
			owned: map[string]uint64{},
		}
	}

	type rec struct {
		node  string
		epoch uint64
		exp   int64
	}
	cur := map[string]rec{} // authoritative record as last written
	lastEpoch := map[string]uint64{}
	nowOf := func(n *simNode, wall time.Time) time.Time { return wall.Add(n.skew) }

	wall := time.Unix(1700000000, 0)
	ctx := context.Background()
	for step := 0; step < steps; step++ {
		wall = wall.Add(time.Duration(rng.Intn(1500)) * time.Millisecond)
		n := nodes[rng.Intn(len(nodes))]
		sc := scopes[rng.Intn(len(scopes))]
		key := sc.String()
		now := nowOf(n, wall)

		switch rng.Intn(4) {
		case 0: // claim
			o, err := n.mgr.Claim(ctx, sc, now)
			if err == nil {
				prev, had := cur[key]
				// Safety: a claim only wins after the prior lease expired.
				if had && prev.exp > now.UnixMilli() {
					return fmt.Errorf("claimed a live lease: prev exp=%d now=%d", prev.exp, now.UnixMilli())
				}
				if o.Epoch <= lastEpoch[key] && lastEpoch[key] != 0 {
					return fmt.Errorf("epoch regressed: got %d last %d", o.Epoch, lastEpoch[key])
				}
				lastEpoch[key] = o.Epoch
				cur[key] = rec{node: o.Node, epoch: o.Epoch, exp: o.Expiry}
				n.owned[key] = o.Epoch
			} else if !errors.Is(err, ErrOwnerLive) && !errors.Is(err, bucket.ErrPrecondition) && !errors.Is(err, ErrUnowned) && !errors.Is(err, ErrGenContention) {
				return fmt.Errorf("claim err: %w", err)
			}
		case 1: // renew
			ep, ok := n.owned[key]
			if !ok {
				continue
			}
			o, err := n.mgr.Renew(ctx, sc, ep, now)
			c, had := cur[key]
			fenced := had && (c.node != n.mgr.NodeID || c.epoch != ep)
			if err == nil {
				if fenced {
					return fmt.Errorf("superseded owner renewed: record=%+v renewer=%s/%d", c, n.mgr.NodeID, ep)
				}
				cur[key] = rec{node: o.Node, epoch: o.Epoch, exp: o.Expiry}
			} else if !errors.Is(err, ErrEpochMismatch) && !errors.Is(err, ErrUnowned) && !errors.Is(err, bucket.ErrPrecondition) && !errors.Is(err, bucket.ErrNotFound) {
				return fmt.Errorf("renew err: %w", err)
			}
		case 2: // release
			ep, ok := n.owned[key]
			if !ok {
				continue
			}
			err := n.mgr.Release(ctx, sc, ep)
			c, had := cur[key]
			fenced := had && (c.node != n.mgr.NodeID || c.epoch != ep)
			if err == nil {
				if fenced {
					return fmt.Errorf("superseded owner released: record=%+v releaser=%s/%d", c, n.mgr.NodeID, ep)
				}
				delete(cur, key)
				delete(n.owned, key)
			} else if !errors.Is(err, ErrEpochMismatch) && !errors.Is(err, ErrUnowned) && !errors.Is(err, bucket.ErrPrecondition) && !errors.Is(err, bucket.ErrNotFound) {
				return fmt.Errorf("release err: %w", err)
			}
		case 3: // resolve: epochs must never regress
			o, _, err := n.mgr.Resolve(ctx, sc)
			if err == nil {
				if o.Epoch < lastEpoch[key] {
					return fmt.Errorf("resolve regressed epoch: got %d last %d", o.Epoch, lastEpoch[key])
				}
			} else if !errors.Is(err, ErrUnowned) {
				return fmt.Errorf("resolve err: %w", err)
			}
		}
	}
	return nil
}
