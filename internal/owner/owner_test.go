package owner

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/lease"
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

func TestTakeoverWaitsForPredecessorRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	first, _ := newManager(t, dir, "node-1")
	second, _ := newManager(t, dir, "node-2")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "gated"}
	old, err := first.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	gateErr := errors.New("predecessor recovery incomplete")
	second.BeforeTakeover = func(_ context.Context, got cell.Owner) error {
		if got.Node != old.Node || got.Session != old.Session || got.Epoch != old.Epoch {
			t.Fatalf("predecessor = %+v, want %+v", got, old)
		}
		return gateErr
	}
	if _, err := second.Claim(ctx, sc, time.UnixMilli(old.Expiry).Add(time.Second)); !errors.Is(err, gateErr) {
		t.Fatalf("takeover error = %v, want recovery error", err)
	}
	got, _, err := first.Resolve(ctx, sc)
	if err != nil || got.Node != old.Node || got.Epoch != old.Epoch {
		t.Fatalf("owner changed before recovery: %+v, %v", got, err)
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

func TestNextEpochRejectsCorruptGeneration(t *testing.T) {
	ctx := context.Background()
	m, _ := newManager(t, t.TempDir(), "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "corrupt"}
	if _, err := m.B.Put(ctx, genKey(sc), []byte("not-a-number")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := m.nextEpoch(ctx, sc); !errors.Is(err, ErrGenCorrupt) {
		t.Fatalf("nextEpoch = %v, want ErrGenCorrupt", err)
	}
	// The fence must fail closed rather than reuse epoch 1.
	if _, err := m.ClaimAs(ctx, sc, "do-1", "", cell.RoleCellAgent, time.Minute, time.Now()); !errors.Is(err, ErrGenCorrupt) {
		t.Fatalf("ClaimAs = %v, want ErrGenCorrupt", err)
	}
}

func TestOwnerRecordCarriesAdminAddress(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, _ := newManager(t, dir, "node-1")
	m.AdminAdvertise = "10.0.0.7:8082"
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "admin-address"}
	o, err := m.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if o.AdminAddress != m.AdminAdvertise {
		t.Fatalf("claim admin address = %q, want %q", o.AdminAddress, m.AdminAdvertise)
	}
	cur, etag, err := m.Resolve(ctx, sc)
	if err != nil || cur.AdminAddress != m.AdminAdvertise {
		t.Fatalf("resolved admin address = %q, err=%v", cur.AdminAddress, err)
	}
	// A lease written before this field existed must acquire it on renewal.
	cur.AdminAddress = ""
	data, err := cell.MarshalOwner(cur)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.B.CAS(ctx, Key(sc), data, etag); err != nil {
		t.Fatal(err)
	}
	m.Invalidate(sc)
	if _, err := m.Renew(ctx, sc, cur.Epoch, time.Now()); err != nil {
		t.Fatal(err)
	}
	after, _, err := m.Resolve(ctx, sc)
	if err != nil || after.AdminAddress != m.AdminAdvertise {
		t.Fatalf("renewed admin address = %q, err=%v", after.AdminAddress, err)
	}
}

type fakeLeaseView map[string]lease.NodeLease

func (f fakeLeaseView) NodeLease(_ context.Context, node string) (lease.NodeLease, bool, error) {
	l, ok := f[node]
	return l, ok, nil
}

func TestNodeLeaseOwnership(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m1, _ := newManager(t, dir, "node-1")
	m2, _ := newManager(t, dir, "node-2")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "liveness"}
	now := time.Now()
	lv := fakeLeaseView{}
	m1.LeaseView = lv
	m2.LeaseView = lv
	o1, err := m1.Claim(ctx, sc, now)
	if err != nil {
		t.Fatal(err)
	}
	// Same session re-entry keeps the epoch and does not need a live lease.
	m1.LeaseView = fakeLeaseView{}
	o2, err := m1.Claim(ctx, sc, now.Add(time.Hour))
	if err != nil || o2.Epoch != o1.Epoch {
		t.Fatalf("same-session claim epoch=%d err=%v, want %d", o2.Epoch, err, o1.Epoch)
	}
	lv["node-1"] = lease.NodeLease{Node: "node-1", Session: o1.Session, Expiry: now.Add(time.Minute).UnixMilli()}
	if _, err := m2.Claim(ctx, sc, now); !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("foreign claim with live node lease: %v, want ErrOwnerLive", err)
	}
	lv["node-1"] = lease.NodeLease{Node: "node-1", Session: o1.Session, Expiry: now.Add(-time.Second).UnixMilli()}
	o3, err := m2.Claim(ctx, sc, now)
	if err != nil || o3.Node != "node-2" || o3.Epoch <= o1.Epoch {
		t.Fatalf("expired node lease takeover: %+v err=%v", o3, err)
	}
}

func TestLiveFailsClosedWithoutLeaseView(t *testing.T) {
	o := cell.Owner{Node: "n", Session: "s", Liveness: "node-lease", Expiry: time.Now().Add(-time.Hour).UnixMilli()}
	m := &Manager{}
	if live, err := m.Live(context.Background(), o, time.Now()); err != nil || live {
		t.Fatalf("node-lease record without view = %v,%v; want false", live, err)
	}
}

func TestSameNodeNewSessionTakesOverImmediately(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m1, _ := newManager(t, dir, "node-1")
	m2, _ := newManager(t, dir, "node-1")
	m2.Session = "node-1-s2"
	lv := fakeLeaseView{}
	m1.LeaseView, m2.LeaseView = lv, lv
	sc := cell.Scope{Namespace: "demo", Class: "__do__", ID: "restart"}
	now := time.Now()
	o1, err := m1.Claim(ctx, sc, now)
	if err != nil {
		t.Fatal(err)
	}
	// The old process's lease is still live, but it belongs to the predecessor
	// session; the new process may take over without waiting for TTL.
	// The restarted process publishes its new-session lease before claiming; the
	// old owner record now has a session mismatch and is immediately dead.
	lv["node-1"] = lease.NodeLease{Node: "node-1", Session: "node-1-s2", Expiry: now.Add(time.Hour).UnixMilli()}
	recovered := false
	m2.BeforeTakeover = func(context.Context, cell.Owner) error { recovered = true; return nil }
	o2, err := m2.Claim(ctx, sc, now)
	if err != nil || !recovered || o2.Epoch != o1.Epoch+1 || o2.Session != "node-1-s2" {
		t.Fatalf("restart takeover = %+v recovered=%v err=%v", o2, recovered, err)
	}
}

// freshLeaseView answers from a stale snapshot but has the true state behind
// NodeLeaseFresh, like lease.Manager with a failing background refresh.
type freshLeaseView struct {
	stale fakeLeaseView
	fresh fakeLeaseView
	errs  bool
}

func (f freshLeaseView) NodeLease(ctx context.Context, node string) (lease.NodeLease, bool, error) {
	return f.stale.NodeLease(ctx, node)
}

func (f freshLeaseView) NodeLeaseFresh(ctx context.Context, node string) (lease.NodeLease, bool, error) {
	if f.errs {
		return lease.NodeLease{}, false, errors.New("bucket unavailable")
	}
	return f.fresh.NodeLease(ctx, node)
}

func TestLiveConfirmsStaleNegativeLease(t *testing.T) {
	now := time.Now()
	o := cell.Owner{Node: "node-1", Session: "current", Liveness: "node-lease"}
	alive := lease.NodeLease{Node: o.Node, Session: o.Session, Expiry: now.Add(time.Minute).UnixMilli()}
	m := &Manager{LeaseView: freshLeaseView{
		stale: fakeLeaseView{o.Node: {Node: o.Node, Session: o.Session, Expiry: now.Add(-time.Second).UnixMilli()}},
		fresh: fakeLeaseView{o.Node: alive},
	}}
	if live, err := m.Live(context.Background(), o, now); err != nil || !live {
		t.Fatalf("expired cached lease hid live owner: live=%t err=%v", live, err)
	}
	m.LeaseView = freshLeaseView{stale: fakeLeaseView{o.Node: {Node: o.Node, Session: "previous", Expiry: alive.Expiry}}, fresh: fakeLeaseView{o.Node: alive}}
	if live, err := m.Live(context.Background(), o, now); err != nil || !live {
		t.Fatalf("cached previous session hid live owner: live=%t err=%v", live, err)
	}
	m.LeaseView = freshLeaseView{stale: fakeLeaseView{}, errs: true}
	if live, err := m.Live(context.Background(), o, now); err == nil || live {
		t.Fatalf("unreadable fresh lease: live=%t err=%v", live, err)
	}
	m.LeaseView = freshLeaseView{stale: fakeLeaseView{}, fresh: fakeLeaseView{o.Node: {Node: o.Node, Session: o.Session, Expiry: now.Add(-time.Second).UnixMilli()}}}
	if live, err := m.Live(context.Background(), o, now); err != nil || live {
		t.Fatalf("genuinely expired lease: live=%t err=%v", live, err)
	}
}

// A stale snapshot saying "owner dead" must not be enough to take a cell over:
// the takeover decision is confirmed with a fresh point read.
func TestTakeoverConfirmsDeadVerdictWithFreshRead(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m1, _ := newManager(t, dir, "node-1")
	m2, _ := newManager(t, dir, "node-2")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "stale"}
	now := time.Now()
	o1, err := m1.Claim(ctx, sc, now)
	if err != nil {
		t.Fatal(err)
	}
	alive := lease.NodeLease{Node: "node-1", Session: o1.Session, Expiry: now.Add(time.Minute).UnixMilli()}
	m2.LeaseView = freshLeaseView{
		stale: fakeLeaseView{}, // snapshot has never seen node-1
		fresh: fakeLeaseView{"node-1": alive},
	}
	if _, err := m2.Claim(ctx, sc, now); !errors.Is(err, ErrOwnerLive) {
		t.Fatalf("claim from stale 'dead' snapshot = %v, want ErrOwnerLive", err)
	}
	// If the fresh read itself fails we cannot prove the owner dead: no takeover.
	m2.LeaseView = freshLeaseView{stale: fakeLeaseView{}, errs: true}
	if _, err := m2.Claim(ctx, sc, now); err == nil || errors.Is(err, ErrOwnerLive) {
		t.Fatalf("claim with unreadable lease = %v, want a lease read error", err)
	}
	// A genuinely dead owner is still taken over.
	m2.LeaseView = freshLeaseView{
		stale: fakeLeaseView{},
		fresh: fakeLeaseView{"node-1": {Node: "node-1", Session: o1.Session, Expiry: now.Add(-time.Second).UnixMilli()}},
	}
	o2, err := m2.Claim(ctx, sc, now)
	if err != nil || o2.Epoch <= o1.Epoch || o2.Node != "node-2" {
		t.Fatalf("dead owner takeover = %+v err=%v", o2, err)
	}
}
