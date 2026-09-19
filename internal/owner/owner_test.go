package owner

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

func newManager(t *testing.T, dir, node string) (*Manager, bucket.Bucket) {
	t.Helper()
	b, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	return &Manager{
		B:         b,
		NodeID:    node,
		Session:   node + "-s1",
		Advertise: node + ":7000",
		Role:      cell.RoleCellAgent,
		OwnerTTL:  5 * time.Second,
	}, b
}

func TestResolveCached(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, _ := newManager(t, dir, "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "cached"}
	if _, err := m.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatalf("claim: %v", err)
	}

	o1, e1, err := m.ResolveCached(ctx, sc, time.Minute)
	if err != nil {
		t.Fatalf("resolve cached: %v", err)
	}
	// Second call within TTL returns the cached record (same etag).
	o2, e2, err := m.ResolveCached(ctx, sc, time.Minute)
	if err != nil || e1 != e2 || o2.Epoch != o1.Epoch {
		t.Fatalf("cache miss/mismatch: %v %s %s", err, e1, e2)
	}

	// ttl=0 bypasses the cache and re-reads the record.
	o3, _, err := m.ResolveCached(ctx, sc, 0)
	if err != nil || o3.Epoch != o1.Epoch {
		t.Fatalf("bypass cache: %v", err)
	}
}

func TestClaimRenewRelease(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, _ := newManager(t, dir, "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	now := time.Now()

	// Cold claim.
	o, err := m.Claim(ctx, sc, now)
	if err != nil {
		t.Fatalf("claim cold: %v", err)
	}
	if o.Epoch != 1 || o.Node != "node-1" {
		t.Fatalf("unexpected owner: %+v", o)
	}

	// Renew with correct epoch.
	if _, err := m.Renew(ctx, sc, o.Epoch, now); err != nil {
		t.Fatalf("renew: %v", err)
	}

	// Renew with wrong epoch -> mismatch.
	if _, err := m.Renew(ctx, sc, o.Epoch+1, now); !errors.Is(err, ErrEpochMismatch) {
		t.Fatalf("renew wrong epoch: got %v, want ErrEpochMismatch", err)
	}

	// Release then re-claim: the generation counter is a fence, so epochs are
	// monotonic and are never reused after release (ADR-078). Reusing epoch 1
	// would collide with the released epoch's replicated lineage.
	if err := m.Release(ctx, sc, o.Epoch); err != nil {
		t.Fatalf("release: %v", err)
	}
	o2, err := m.Claim(ctx, sc, now)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if o2.Epoch != o.Epoch+1 {
		t.Fatalf("reclaim epoch = %d, want %d (no epoch reuse)", o2.Epoch, o.Epoch+1)
	}
}

func TestLiveOwnerBlocksOtherNode(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m1, _ := newManager(t, dir, "node-1")
	m2, _ := newManager(t, dir, "node-2")
	sc := cell.Scope{Namespace: "demo", Class: "__d1__", ID: "db"}
	now := time.Now()

	if _, err := m1.Claim(ctx, sc, now); err != nil {
		t.Fatalf("node-1 claim: %v", err)
	}
	_, err := m2.Claim(ctx, sc, now)
	if !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("node-2 claim against live owner: got %v, want ErrOwnerLive", err)
	}
}

func TestExpiredOwnerTakeoverAdvancesEpoch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m1, _ := newManager(t, dir, "node-1")
	m2, _ := newManager(t, dir, "node-2")
	sc := cell.Scope{Namespace: "demo", Class: "__queue__", ID: "jobs"}

	o1, err := m1.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatalf("node-1 claim: %v", err)
	}
	// Simulate expiry.
	later := time.UnixMilli(o1.Expiry).Add(time.Second)
	o2, err := m2.Claim(ctx, sc, later)
	if err != nil {
		t.Fatalf("node-2 takeover: %v", err)
	}
	if o2.Node != "node-2" || o2.Epoch != o1.Epoch+1 {
		t.Fatalf("takeover owner = %+v, want node-2 epoch %d", o2, o1.Epoch+1)
	}
}

func TestScopeParse(t *testing.T) {
	if _, err := cell.ParseScope("demo/__kv__/main"); err != nil {
		t.Fatalf("valid scope rejected: %v", err)
	}
	if _, err := cell.ParseScope("bad"); err == nil {
		t.Fatalf("invalid scope accepted")
	}
	if _, err := cell.ParseScope("a/b/c/d"); err == nil {
		t.Fatalf("over-long scope accepted")
	}
}

func TestOwnedScopesTracksClaimAndRelease(t *testing.T) {
	ctx := context.Background()
	m, _ := newManager(t, t.TempDir(), "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "owned"}
	o, err := m.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	owned := m.OwnedScopes()
	if len(owned) != 1 || owned[0].Scope.String() != sc.String() || owned[0].Epoch != o.Epoch {
		t.Fatalf("owned after claim = %+v", owned)
	}
	if _, err := m.Renew(ctx, sc, o.Epoch, time.Now()); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if got := m.OwnedScopes(); len(got) != 1 {
		t.Fatalf("owned after renew = %+v", got)
	}
	if err := m.Release(ctx, sc, o.Epoch); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := m.OwnedScopes(); len(got) != 0 {
		t.Fatalf("owned after release = %+v", got)
	}
}

func TestClaimAsGenerationFence(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	m := &Manager{B: b, NodeID: "cell-node", Session: "sess", Advertise: "http://cell:7001", OwnerTTL: time.Minute}
	scope := cell.Scope{Namespace: "demo", Class: "__do__", ID: "w~C~shard0"}
	now := time.Unix(1_700_000_000, 0)

	o1, err := m.ClaimAs(ctx, scope, "do-1", "http://do-1:8788", cell.RoleCellAgent, 30*time.Second, now)
	if err != nil || o1.Epoch != 1 || o1.Node != "do-1" {
		t.Fatalf("claim do-1 = %+v, %v", o1, err)
	}
	// A different node while the lease is live -> owner_live.
	if _, err := m.ClaimAs(ctx, scope, "do-2", "", cell.RoleCellAgent, 30*time.Second, now); !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("claim do-2 = %v, want ErrOwnerLive", err)
	}
	// Renew with the wrong epoch is rejected; the right epoch works.
	if _, err := m.RenewAs(ctx, scope, "do-1", 99, 30*time.Second, now); !errors.Is(err, ErrEpochMismatch) {
		t.Fatalf("renew wrong epoch = %v", err)
	}
	if _, err := m.RenewAs(ctx, scope, "do-1", 1, 30*time.Second, now); err != nil {
		t.Fatalf("renew = %v", err)
	}
	// Release, then a fresh claim must get a *higher* epoch (generation never reused).
	if err := m.ReleaseAs(ctx, scope, "do-1", 1); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := m.ReleaseAs(ctx, scope, "do-1", 1); err == nil {
		t.Fatalf("second release should fail (already released)")
	}
	o2, err := m.ClaimAs(ctx, scope, "do-2", "http://do-2:8788", cell.RoleCellAgent, 30*time.Second, now)
	if err != nil || o2.Epoch != 2 {
		t.Fatalf("reclaim = %+v, %v; want epoch 2 (monotonic)", o2, err)
	}
	// An expired lease can be taken over (with a higher epoch).
	o3, err := m.ClaimAs(ctx, scope, "do-3", "", cell.RoleCellAgent, 30*time.Second, now.Add(2*time.Minute))
	if err != nil || o3.Epoch != 3 || o3.Node != "do-3" {
		t.Fatalf("expired steal = %+v, %v", o3, err)
	}
}

func TestClaimStatsTakeoverAndEpoch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, b := newManager(t, dir, "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "stats"}

	// Fresh claim: one epoch bump, no takeover.
	if _, err := m.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatalf("claim: %v", err)
	}
	st := m.ClaimStats()
	if st["epoch_bumps"] != 1 || st["takeover_success"] != 0 {
		t.Fatalf("fresh stats = %v", st)
	}

	// A foreign live lease blocks a claim.
	other, _ := newManager(t, dir, "node-2")
	if _, err := other.Claim(ctx, sc, time.Now()); !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("foreign claim = %v, want ErrOwnerLive", err)
	}
	if got := other.ClaimStats()["takeover_blocked"]; got != 1 {
		t.Fatalf("takeover_blocked = %d, want 1", got)
	}

	// An expired foreign owner is a takeover.
	expired := cell.Owner{Node: "node-9", Epoch: 5, Expiry: time.Now().Add(-time.Hour).UnixMilli()}
	data, _ := cell.MarshalOwner(expired)
	if _, err := b.Put(ctx, Key(sc), data); err != nil {
		t.Fatalf("put expired: %v", err)
	}
	m.Invalidate(sc)
	if _, err := m.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatalf("takeover claim: %v", err)
	}
	st = m.ClaimStats()
	if st["takeover_success"] != 1 || st["epoch_bumps"] != 2 {
		t.Fatalf("takeover stats = %v", st)
	}
}

func TestClaimAsStats(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, _ := newManager(t, dir, "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__do__", ID: "w~C~shard0"}
	if _, err := m.ClaimAs(ctx, sc, "node-1", "n1:7000", cell.RoleDORuntime, 5*time.Second, time.Now()); err != nil {
		t.Fatalf("claimAs: %v", err)
	}
	if got := m.ClaimStats()["epoch_bumps"]; got != 1 {
		t.Fatalf("claimAs epoch_bumps = %d, want 1", got)
	}
}
