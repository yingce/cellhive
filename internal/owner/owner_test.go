package owner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/lease"
)

type delayedOwnerGet struct {
	bucket.Bucket
	mu      sync.Mutex
	block   bool
	started chan struct{}
	release chan struct{}
}

func (b *delayedOwnerGet) Get(ctx context.Context, key string) ([]byte, string, error) {
	data, etag, err := b.Bucket.Get(ctx, key)
	b.mu.Lock()
	block := b.block
	if block {
		b.block = false
	}
	b.mu.Unlock()
	if block {
		close(b.started)
		<-b.release
	}
	return data, etag, err
}

func TestInvalidateRejectsInFlightCachedOwnerRefresh(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "refresh-race"}
	if _, err := m.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	b := &delayedOwnerGet{Bucket: raw, block: true, started: make(chan struct{}), release: make(chan struct{})}
	m.B = b
	done := make(chan error, 1)
	go func() { _, _, err := m.ResolveCached(t.Context(), sc, 0); done <- err }()
	<-b.started
	m.Invalidate(sc)
	close(b.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	_, cached := m.cache[sc.String()]
	m.mu.Unlock()
	if cached {
		t.Fatal("owner result read before invalidation repopulated the cache")
	}
}

func TestOwnLeaseLivenessUsesPublishedSelfFence(t *testing.T) {
	m, raw := newManager(t, t.TempDir(), "node-1")
	lm := lease.NewManager(raw, m.NodeID, m.Session, "", "", 5*time.Second)
	m.LeaseView = lm
	if err := lm.Publish(t.Context(), lease.Load{}); err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "local-liveness"}
	o, err := m.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// A self owner must not read nodes/<self>.json on the request path.
	lm.B = failingOwnerLeaseBucket{raw}
	if live, err := m.Live(t.Context(), o, time.Now()); err != nil || !live {
		t.Fatalf("published local lease = %v, %v", live, err)
	}
	if live, err := m.Live(t.Context(), o, time.Now().Add(5*time.Second)); err != nil || live {
		t.Fatalf("expired local lease = %v, %v", live, err)
	}
}

type failingOwnerLeaseBucket struct{ bucket.Bucket }

func (b failingOwnerLeaseBucket) Get(context.Context, string) ([]byte, string, error) {
	return nil, "", errors.New("unexpected bucket GET")
}

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
	if _, err := m.nextEpoch(ctx, sc, 0); !errors.Is(err, ErrGenCorrupt) {
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

func TestNextEpochKeepsFloorWhenGenMissing(t *testing.T) {
	ctx := context.Background()
	m, raw := newManager(t, t.TempDir(), "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "legacy-epoch"}
	o := m.newOwner(42, time.Now())
	data, err := cell.MarshalOwner(o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Put(ctx, Key(sc), data); err != nil {
		t.Fatal(err)
	}
	got, err := m.nextEpoch(ctx, sc, o.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	if got != 43 {
		t.Fatalf("epoch = %d, want 43 (must exceed the existing owner epoch)", got)
	}
	gen, _, err := raw.Get(ctx, genKey(sc))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(gen)) != "43" {
		t.Fatalf("owner-gen = %q, want 43", gen)
	}
}

type touchBucket struct {
	bucket.Bucket
	mu    sync.Mutex
	calls int
}

func (b *touchBucket) Touch(context.Context, string) error {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	return nil
}

func TestRenewTouchesNodeLeaseSegmentAtMostPerInterval(t *testing.T) {
	ctx := context.Background()
	m, raw := newManager(t, t.TempDir(), "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__node__", ID: "node-1"}
	now := time.Unix(1_700_000_000, 0)
	o, err := m.Claim(ctx, sc, now)
	if err != nil {
		t.Fatal(err)
	}
	tb := &touchBucket{Bucket: raw}
	m.B = tb
	if _, err := m.Renew(ctx, sc, o.Epoch, now); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Renew(ctx, sc, o.Epoch, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Renew(ctx, sc, o.Epoch, now.Add(31*time.Minute)); err != nil {
		t.Fatal(err)
	}
	tb.mu.Lock()
	calls := tb.calls
	tb.mu.Unlock()
	if calls != 2 {
		t.Fatalf("touch calls = %d, want 2", calls)
	}
}

func TestReleaseRunsBeforeReleaseHook(t *testing.T) {
	ctx := context.Background()
	m, _ := newManager(t, t.TempDir(), "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "pre-release"}
	o, err := m.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	var gotScope string
	var gotEpoch uint64
	m.BeforeRelease = func(_ context.Context, s cell.Scope, epoch uint64) error {
		gotScope, gotEpoch = s.String(), epoch
		return nil
	}
	if err := m.Release(ctx, sc, o.Epoch); err != nil {
		t.Fatalf("release: %v", err)
	}
	if gotScope != sc.String() || gotEpoch != o.Epoch {
		t.Fatalf("hook got scope=%q epoch=%d, want %q/%d", gotScope, gotEpoch, sc.String(), o.Epoch)
	}
	if got := m.OwnedScopes(); len(got) != 0 {
		t.Fatalf("owned after release = %+v", got)
	}
}

func TestReleaseForPurgeSkipsBeforeReleaseHook(t *testing.T) {
	ctx := context.Background()
	m, _ := newManager(t, t.TempDir(), "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "purge"}
	o, err := m.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	called := false
	m.BeforeRelease = func(context.Context, cell.Scope, uint64) error {
		called = true
		return errors.New("must not run for purge")
	}
	if err := m.ReleaseForPurge(ctx, sc, o.Epoch); err != nil {
		t.Fatalf("release for purge: %v", err)
	}
	if called {
		t.Fatal("BeforeRelease ran for a purge release")
	}
	retired, _, err := m.Resolve(ctx, sc)
	if err != nil {
		t.Fatalf("retired lineage missing: %v", err)
	}
	if retired.Node != "" || retired.Session != "" || !retired.Retired || retired.Epoch != o.Epoch || retired.RetiredEpoch != o.Epoch {
		t.Fatalf("purge release lost owner lineage: %+v", retired)
	}
}

func TestReleaseBeforeReleaseErrorKeepsOwnership(t *testing.T) {
	ctx := context.Background()
	m, _ := newManager(t, t.TempDir(), "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "pre-release-error"}
	o, err := m.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	m.BeforeRelease = func(context.Context, cell.Scope, uint64) error {
		return errors.New("compaction failed")
	}
	if err := m.Release(ctx, sc, o.Epoch); err == nil {
		t.Fatal("release succeeded despite BeforeRelease error")
	}
	if got := m.OwnedScopes(); len(got) != 1 || got[0].Scope.String() != sc.String() {
		t.Fatalf("ownership not retained after hook error: %+v", got)
	}
}

func TestClaimAsUsesNodeLeaseSession(t *testing.T) {
	for _, state := range []string{"live", "expired", "restarted"} {
		t.Run(state, func(t *testing.T) {
			m, _ := newManager(t, t.TempDir(), "agent")
			now := time.Now()
			sc := cell.Scope{Namespace: "acme", Class: "__do__", ID: "import"}
			prior, err := m.Claim(t.Context(), sc, now)
			if err != nil {
				t.Fatal(err)
			}
			l := lease.NodeLease{Node: "agent", Session: prior.Session, Expiry: now.Add(time.Minute).UnixMilli()}
			if state == "expired" {
				l.Expiry = now.Add(-time.Second).UnixMilli()
			}
			if state == "restarted" {
				l.Session = "new-session"
			}
			m.LeaseView = fakeLeaseView{"agent": l}
			got, err := m.ClaimAs(t.Context(), sc, "runtime", "http://runtime", cell.RoleCellAgent, time.Minute, now)
			if state == "live" {
				if !errors.Is(err, ErrOwnerLive) {
					t.Fatalf("live predecessor accepted: %v", err)
				}
			} else if err != nil || got.Epoch <= prior.Epoch {
				t.Fatalf("takeover=%+v err=%v", got, err)
			}
		})
	}
}

func TestExclusiveClaimPinsOwnershipAndRejectsReentry(t *testing.T) {
	m, b := newManager(t, t.TempDir(), "agent")
	sc := DOScope("acme", "worker", "Counter", 0)
	o, release, err := m.ClaimExclusive(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer release(t.Context())
	if o.Address != "" {
		t.Fatal("maintenance owner advertised a runtime endpoint")
	}
	peer := &Manager{B: b, NodeID: m.NodeID, Session: m.Session, OwnerTTL: time.Minute}
	if _, _, err := peer.ClaimExclusive(t.Context(), sc, time.Now()); !errors.Is(err, ErrOwnerLive) {
		t.Errorf("reentered shared identity: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := m.Claim(ctx, sc, time.Now()); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ordinary claim bypassed exclusive guard: %v", err)
	}
	if err := peer.ReleaseAs(t.Context(), sc, m.NodeID, o.Epoch); !errors.Is(err, ErrEpochMismatch) {
		t.Errorf("runtime release bypassed guard: %v", err)
	}
	if _, err := peer.RenewAs(t.Context(), sc, m.NodeID, o.Epoch, time.Minute, time.Now()); !errors.Is(err, ErrEpochMismatch) {
		t.Errorf("runtime renewal changed guard: %v", err)
	}
	if err := release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ClaimAs(t.Context(), sc, "runtime", "runtime:1234", cell.RoleCellAgent, time.Minute, time.Now()); err != nil {
		t.Fatal(err)
	}
}

type failExclusiveDeleteOnce struct {
	bucket.Bucket
	failed bool
}

func (b *failExclusiveDeleteOnce) ConditionalDelete(ctx context.Context, key, etag string) error {
	if !b.failed {
		b.failed = true
		return errors.New("temporary delete failure")
	}
	return b.Bucket.ConditionalDelete(ctx, key, etag)
}
func TestExclusiveReleaseRetriesTransientFailure(t *testing.T) {
	m, b := newManager(t, t.TempDir(), "agent")
	m.B = &failExclusiveDeleteOnce{Bucket: b}
	sc := DOScope("acme", "worker", "Counter", 0)
	_, release, err := m.ClaimExclusive(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := release(t.Context()); err == nil {
		t.Fatal("failure was hidden")
	}
	if err := release(t.Context()); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if _, err := m.ClaimAs(t.Context(), sc, "runtime", "http://runtime", cell.RoleCellAgent, time.Minute, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestFailedExclusiveReleaseBackgroundRecovery(t *testing.T) {
	for _, takeover := range []bool{false, true} {
		t.Run(fmt.Sprint(takeover), func(t *testing.T) {
			m, b := newManager(t, t.TempDir(), "agent")
			sc := DOScope("acme", "worker", "Counter", 0)
			failing := true
			m.BeforeRelease = func(context.Context, cell.Scope, uint64) error {
				if failing {
					return errors.New("storage unavailable")
				}
				return nil
			}
			_, release, err := m.ClaimExclusive(t.Context(), sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if err := release(t.Context()); err == nil {
				t.Fatal("expected foreground failure")
			}
			if err := m.RetryExclusiveReleases(t.Context()); err == nil {
				t.Fatal("expected persistent failure")
			}
			failing = false
			if takeover {
				_, etag, err := m.Resolve(t.Context(), sc)
				if err != nil {
					t.Fatal(err)
				}
				if err := b.ConditionalDelete(t.Context(), Key(sc), etag); err != nil {
					t.Fatal(err)
				}
				if _, err := m.ClaimAs(t.Context(), sc, "successor", "http://runtime", cell.RoleCellAgent, time.Minute, time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.RetryExclusiveReleases(t.Context()); err != nil {
				t.Fatal(err)
			}
			if len(m.pendingExclusiveRelease) != 0 {
				t.Fatal("completed cleanup retained")
			}
			o, _, err := m.Resolve(t.Context(), sc)
			if takeover {
				if err != nil || o.Node != "successor" {
					t.Fatalf("successor deleted: %+v %v", o, err)
				}
			} else if !errors.Is(err, ErrUnowned) {
				t.Fatalf("guard remains: %v", err)
			}
		})
	}
}

// Model a provider that durably accepts the owner mutation but loses its reply.
type lostExclusiveClaimReply struct {
	bucket.Bucket
	key string
}

func (b *lostExclusiveClaimReply) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	etag, err := b.Bucket.ConditionalCreate(ctx, key, data)
	if err == nil && key == b.key {
		return "", errors.New("owner write reply lost")
	}
	return etag, err
}
func (b *lostExclusiveClaimReply) CAS(ctx context.Context, key string, data []byte, expected string) (string, error) {
	etag, err := b.Bucket.CAS(ctx, key, data, expected)
	if err == nil && key == b.key {
		return "", errors.New("owner write reply lost")
	}
	return etag, err
}
func TestExclusiveClaimLostReplyIsEventuallyReleased(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, successor := range []bool{false, true} {
			t.Run(fmt.Sprintf("existing=%v/successor=%v", existing, successor), func(t *testing.T) {
				m, b := newManager(t, t.TempDir(), "agent")
				sc := DOScope("acme", "worker", "Counter", 0)
				if existing {
					if _, err := m.ClaimAs(t.Context(), sc, "old-runtime", "http://old", cell.RoleCellAgent, time.Second, time.Now().Add(-time.Minute)); err != nil {
						t.Fatal(err)
					}
				}
				m.B = &lostExclusiveClaimReply{Bucket: b, key: Key(sc)}
				_, release, err := m.ClaimExclusive(t.Context(), sc, time.Now())
				if err == nil || release != nil {
					t.Fatal("ambiguous acquisition returned a usable guard")
				}
				o, etag, err := m.Resolve(t.Context(), sc)
				if err != nil || o.Node != m.NodeID {
					t.Fatalf("write did not land: %+v %v", o, err)
				}
				if successor {
					if err := b.ConditionalDelete(t.Context(), Key(sc), etag); err != nil {
						t.Fatal(err)
					}
					peer := &Manager{B: b, NodeID: "peer", Session: "peer-session", OwnerTTL: time.Minute}
					if _, err := peer.ClaimAs(t.Context(), sc, "successor", "http://successor", cell.RoleCellAgent, time.Minute, time.Now()); err != nil {
						t.Fatal(err)
					}
				}
				if err := m.RetryExclusiveReleases(t.Context()); err != nil {
					t.Fatal(err)
				}
				current, _, err := m.Resolve(t.Context(), sc)
				if successor {
					if err != nil || current.Node != "successor" {
						t.Fatalf("successor lost: %+v %v", current, err)
					}
				} else if !errors.Is(err, ErrUnowned) {
					t.Fatalf("ambiguous guard stranded: %+v %v", current, err)
				}
			})
		}
	}
}
