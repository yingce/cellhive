// Package owner implements the cell owner claim / resolve protocol.
package owner

import (
	"cellhive/internal/telemetry"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/lease"
)

var (
	// ErrUnowned means no owner record exists (cold cell).
	ErrUnowned = errors.New("owner: unowned")
	// ErrOwnerLive means another node holds a live lease.
	ErrOwnerLive = errors.New("owner: live owner exists")
	// ErrRetired prevents an ordinary claim from resurrecting a deleted
	// resource. Resource activation clears the owner retirement marker first.
	ErrRetired = errors.New("owner: resource retired")
	// ErrEpochMismatch means the expected epoch no longer matches.
	ErrEpochMismatch = errors.New("owner: epoch mismatch")
	// ErrNotOwner means this node is not the scope's owner (a non-owner replica
	// must forward to the owner, ADR-003).
	ErrNotOwner = errors.New("owner: caller is not the owner")
)

type cachedOwner struct {
	owner cell.Owner
	etag  string
	at    time.Time
}

const (
	// Bounds keep per-scope caches from growing without limit when a node sees
	// many one-off cells. Eviction only causes an extra point read.
	maxOwnerCache    = 4096
	maxOwnerCacheRev = 8192
	ownerCacheIdle   = 10 * time.Minute
)

// LeaseView supplies node liveness for ADR-273 owner records.
type LeaseView interface {
	NodeLease(ctx context.Context, node string) (lease.NodeLease, bool, error)
}

// FreshLeaseView is an optional LeaseView extension that reads a node lease
// straight from the bucket instead of the cached snapshot.
type FreshLeaseView interface {
	NodeLeaseFresh(ctx context.Context, node string) (lease.NodeLease, bool, error)
}

// liveFresh is the takeover check. Live confirms negative cached lease
// verdicts with a point read, so a delayed snapshot cannot admit a second
// writer or make a live owner appear dead to request routing.
func (m *Manager) liveFresh(ctx context.Context, o cell.Owner, now time.Time) (bool, error) {
	return m.Live(ctx, o, now)
}

// Live reports whether o is still a valid owner at now. Legacy records use the
// embedded Expiry; node-lease records require a live lease with the same
// session (so a restarted process invalidates its predecessor immediately).
func (m *Manager) Live(ctx context.Context, o cell.Owner, now time.Time) (bool, error) {
	if o.Liveness != "node-lease" {
		return !o.Expired(now), nil
	}
	// This process knows whether its own lease was successfully published and
	// remains inside the self-fence margin. A snapshot miss must not turn a
	// locally healthy owner into a blocking bucket point read on every GET.
	if own, ok := m.LeaseView.(*lease.Manager); ok && o.Node == m.NodeID && o.Session == m.Session && own.NodeID == m.NodeID && own.Session == m.Session {
		return own.Healthy(now), nil
	}
	if m.LeaseView == nil {
		// Compatibility/tests: without a node-lease view, fall back to the
		// record expiry written alongside Liveness. Production wires LeaseView.
		return now.UnixMilli() < o.Expiry, nil
	}
	l, ok, err := m.LeaseView.NodeLease(ctx, o.Node)
	if err != nil {
		return false, err
	}
	if ok && l.Live(now) && l.Session == o.Session {
		return true, nil
	}
	// The snapshot can remain stale past TTL while its background List is
	// delayed or failing. A negative verdict is only authoritative after a
	// fresh point read; otherwise live owners trigger spurious claims/503s.
	if fv, hasFresh := m.LeaseView.(FreshLeaseView); hasFresh {
		l, ok, err = fv.NodeLeaseFresh(ctx, o.Node)
	}
	if err != nil {
		return false, err
	}
	return ok && l.Live(now) && l.Session == o.Session, nil
}

// Forget removes a scope from the local owned set without touching the bucket
// owner record. Eviction uses this: the node may own a hibernated cell without
// paying per-cell renewal, and reopening keeps the same epoch (ADR-273).
func (m *Manager) Forget(s cell.Scope, epoch uint64) {
	m.ownedMu.Lock()
	if cur, ok := m.owned[s.String()]; ok && (epoch == 0 || cur.Epoch == epoch) {
		delete(m.owned, s.String())
	}
	m.ownedMu.Unlock()
}

// Manager performs owner operations for a node.
type Manager struct {
	B              bucket.Bucket
	NodeID         string
	Session        string
	Advertise      string
	AdminAdvertise string
	Role           cell.Role
	OwnerTTL       time.Duration
	// LeaseView resolves a node lease for ADR-273 liveness. Nil keeps legacy
	// expiry semantics (tests/DO only).
	LeaseView LeaseView

	// BeforeTakeover must durably recover the expired predecessor session before
	// its owner record is replaced. Nil is only safe for roles with no fleet
	// acknowledgement path.
	BeforeTakeover func(context.Context, cell.Owner) error
	// BeforeRelease must not reenter same-scope Claim/Release or wait for
	// admitted requests that might claim. It runs under the per-scope transition
	// lock while ownership is still valid, immediately before the
	// owner record is deleted. cell-agent uses it to fold the final L0 tail so a
	// released/rebound cell does not leave an uncompacted predecessor epoch.
	BeforeRelease func(context.Context, cell.Scope, uint64) error
	mu            sync.Mutex
	cache         map[string]cachedOwner
	cacheRev      map[string]uint64
	cacheRevAt    map[string]time.Time
	cacheEpoch    uint64
	cachePending  map[string]int
	guards        map[string]int
	routes        map[string]ownerRoute
	routeFlights  map[string]*routeFlight
	releasing     map[string]int

	claimMu sync.Mutex
	claims  map[string]chan struct{}

	ownedMu                 sync.Mutex
	owned                   map[string]OwnedScope
	pendingExclusiveRelease map[string]OwnedScope

	// Node-lease records are not rewritten by Renew. Touch them at most once
	// per interval so they stay in the current segmented-authority window and
	// readers do not fall back to a bucket List after a rollover.
	touchMu   sync.Mutex
	lastTouch map[string]time.Time

	// Metrics (ADR-165): epoch bumps and takeover outcomes, read by
	// cell-agent's /metrics.
	epochBumps      atomic.Int64
	takeoverSuccess atomic.Int64
	takeoverFailed  atomic.Int64
	takeoverBlocked atomic.Int64
}

// ClaimStats reports owner lifecycle counters for /metrics.
func (m *Manager) ClaimStats() map[string]int64 {
	return map[string]int64{
		"epoch_bumps":      m.epochBumps.Load(),
		"takeover_success": m.takeoverSuccess.Load(),
		"takeover_failed":  m.takeoverFailed.Load(),
		"takeover_blocked": m.takeoverBlocked.Load(),
	}
}

// OwnedScope is a scope this node currently holds, with the epoch it holds it
// under. It is advisory process-local state; callers must still re-check
// ownership before acting on it.
type OwnedScope struct {
	Scope cell.Scope
	Epoch uint64
}

// Key returns the owner record key for a scope.
// Key is the celld-compatible authority record name.  `own.json` is the
// single owner/epoch lineage object; resource fencing must not create a
// second metadata file beside it.
func Key(s cell.Scope) string { return s.Key() + "/own.json" }

// ResolveCached returns the owner record with a short TTL cache so the hot path
// does not read object storage on every request (see docs/cell-protocol.md
// §4.1). A stale hit can lag an ownership change by at most ttl; writers still
// verify the epoch they were handed and self-fence when renewal fails.
func (m *Manager) ResolveCached(ctx context.Context, s cell.Scope, ttl time.Duration) (cell.Owner, string, error) {
	key := s.String()
	m.mu.Lock()
	rev := m.cacheRev[key]
	if c, ok := m.cache[key]; ok && time.Since(c.at) < ttl {
		m.mu.Unlock()
		return c.owner, c.etag, nil
	}
	if m.cachePending == nil {
		m.cachePending = make(map[string]int)
	}
	m.cachePending[key]++
	m.mu.Unlock()

	o, e, err := m.Resolve(ctx, s)
	if err != nil {
		m.mu.Lock()
		m.finishResolveLocked(key)
		m.mu.Unlock()
		return o, e, err
	}
	m.mu.Lock()
	if m.cacheRev[key] == rev {
		if m.cache == nil {
			m.cache = make(map[string]cachedOwner)
		}
		m.cache[key] = cachedOwner{owner: o, etag: e, at: time.Now()}
	}
	m.finishResolveLocked(key)
	m.trimCachesLocked(time.Now())
	m.mu.Unlock()
	return o, e, nil
}

func (m *Manager) finishResolveLocked(key string) {
	if n := m.cachePending[key] - 1; n > 0 {
		m.cachePending[key] = n
	} else {
		delete(m.cachePending, key)
	}
}

func (m *Manager) cacheKeyActiveLocked(key string) bool {
	if _, ok := m.cache[key]; ok {
		return true
	}
	if _, ok := m.routes[key]; ok {
		return true
	}
	if _, ok := m.routeFlights[key]; ok {
		return true
	}
	if m.releasing[key] > 0 || m.cachePending[key] > 0 || m.guards[key] > 0 {
		return true
	}
	return false
}

// trimCachesLocked bounds the owner caches. A key with no cached record, route,
// in-flight lookup or active read fence cannot be observed by a concurrent
// caller, so dropping its revision is safe; it only loses a race token.
func (m *Manager) trimCachesLocked(now time.Time) {
	if len(m.cache) > maxOwnerCache {
		for key, c := range m.cache {
			if now.Sub(c.at) >= ownerCacheIdle {
				delete(m.cache, key)
			}
		}
		for key := range m.cache {
			if len(m.cache) <= maxOwnerCache {
				break
			}
			delete(m.cache, key)
		}
	}
	if len(m.cacheRev) > maxOwnerCacheRev {
		for key := range m.cacheRev {
			if m.cacheKeyActiveLocked(key) {
				continue
			}
			if now.Sub(m.cacheRevAt[key]) >= ownerCacheIdle {
				m.dropRevLocked(key)
			}
		}
		for key := range m.cacheRev {
			if len(m.cacheRev) <= maxOwnerCacheRev {
				break
			}
			if !m.cacheKeyActiveLocked(key) {
				m.dropRevLocked(key)
			}
		}
	}
}

// dropRevLocked discards a revision and advances the epoch so reusable read
// fences captured before the trim cannot compare 0==0 and accept a stale read.
func (m *Manager) dropRevLocked(key string) {
	delete(m.cacheRev, key)
	delete(m.cacheRevAt, key)
	m.cacheEpoch++
}

// Invalidate drops a cached owner record (after claim/release).
func (m *Manager) Invalidate(s cell.Scope) {
	m.mu.Lock()
	m.invalidateLocked(s.String())
	m.mu.Unlock()
}

func (m *Manager) invalidateLocked(key string) {
	delete(m.cache, key)
	delete(m.routes, key)
	if m.cacheRev == nil {
		m.cacheRev = make(map[string]uint64)
	}
	m.cacheRev[key]++
	if m.cacheRevAt == nil {
		m.cacheRevAt = make(map[string]time.Time)
	}
	m.cacheRevAt[key] = time.Now()
	m.trimCachesLocked(time.Now())
}

// Resolve returns the current owner record and its etag, or ErrUnowned.
func (m *Manager) Resolve(ctx context.Context, s cell.Scope) (cell.Owner, string, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "cell.owner_bucket_read")
	defer span.End()
	data, e, err := m.B.Get(ctx, Key(s))
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) {
			return cell.Owner{}, "", ErrUnowned
		}
		return cell.Owner{}, "", err
	}
	o, err := cell.UnmarshalOwner(data)
	if err != nil {
		return cell.Owner{}, "", fmt.Errorf("owner: corrupt record: %w", err)
	}
	return o, e, nil
}

// Claim attempts to acquire ownership of a scope (ADR-037: cell-agent driven).
//
// On success it returns the new owner record. If another node holds a live
// lease it returns ErrOwnerLive with that record. Races return
// bucket.ErrPrecondition.
func (m *Manager) Claim(ctx context.Context, s cell.Scope, now time.Time) (cell.Owner, error) {
	return m.ClaimWithReset(ctx, s, now, nil)
}

// ClaimWithReset finishes predecessor recovery and resets the obsolete local
// replica BEFORE publishing ownership. Failed resets leave the old owner in
// place, so retries cannot mistake an incomplete takeover for a ready owner.
// reset must not re-enter Claim or wait on requests waiting for this claim.
func (m *Manager) ClaimWithReset(ctx context.Context, s cell.Scope, now time.Time, reset func(context.Context, cell.Scope) error) (cell.Owner, error) {
	return m.claimWithReset(ctx, s, now, reset, false)
}

// ClaimExclusive acquires maintenance ownership without reentering a local
// owner. Concurrent maintenance operations must never share the same epoch.
func (m *Manager) ClaimExclusive(ctx context.Context, s cell.Scope, now time.Time) (cell.Owner, func(context.Context) error, error) {
	unlock, err := m.lockClaim(ctx, s.String())
	if err != nil {
		return cell.Owner{}, nil, err
	}
	o, err := m.claimLocked(ctx, s, now, nil, true)
	if err != nil {
		unlock()
		return o, nil, err
	}
	// Keep the claim gate until release: idle eviction/rebalancing must not
	// relinquish maintenance ownership while the caller is still publishing.
	var releaseMu sync.Mutex
	locked, released := true, false
	release := func(ctx context.Context) error {
		releaseMu.Lock()
		defer releaseMu.Unlock()
		if released {
			return nil
		}
		var err error
		if locked {
			finish := m.beginOwnershipTransition(s)
			err = m.releaseLocked(ctx, s, o.Epoch, false)
			finish()
			locked = false
			unlock()
		} else {
			// Retry an ambiguous/transient cleanup failure with the exact epoch;
			// never re-claim or delete a successor's owner record.
			err = m.Release(ctx, s, o.Epoch)
		}
		if err == nil || errors.Is(err, ErrUnowned) {
			released = true
			m.finishExclusiveRelease(s, o.Epoch)
			return nil
		}
		if errors.Is(err, ErrEpochMismatch) {
			m.finishExclusiveRelease(s, o.Epoch)
		} else {
			m.deferExclusiveRelease(s, o.Epoch)
		}
		return err
	}
	return o, release, nil
}

func (m *Manager) claimWithReset(ctx context.Context, s cell.Scope, now time.Time, reset func(context.Context, cell.Scope) error, exclusive bool) (cell.Owner, error) {
	release, err := m.lockClaim(ctx, s.String())
	if err != nil {
		return cell.Owner{}, err
	}
	defer release()
	return m.claimLocked(ctx, s, now, reset, exclusive)
}

func (m *Manager) claimLocked(ctx context.Context, s cell.Scope, now time.Time, reset func(context.Context, cell.Scope) error, exclusive bool) (cell.Owner, error) {
	cur, e, err := m.Resolve(ctx, s)
	switch {
	case errors.Is(err, ErrUnowned):
		epoch, gerr := m.nextEpoch(ctx, s, 0)
		if gerr != nil {
			return cell.Owner{}, gerr
		}
		if reset != nil {
			if err := reset(ctx, s); err != nil {
				return cell.Owner{}, err
			}
		}
		o := m.newOwner(epoch, now)
		if exclusive {
			o.Address = ""
		}
		data, _ := cell.MarshalOwner(o)
		if _, cerr := m.B.ConditionalCreate(ctx, Key(s), data); cerr != nil {
			// Failure may mean the write committed but its response was lost.
			if exclusive {
				m.deferExclusiveRelease(s, o.Epoch)
			}
			return cell.Owner{}, cerr // lost the race
		}
		m.Invalidate(s)
		m.rememberOwned(s, o.Epoch)
		return o, nil
	case err != nil:
		return cell.Owner{}, err
	}
	if cur.Retired {
		return cur, ErrRetired
	}

	// Re-entering our own live record is the common local re-open: keep the
	// epoch, write nothing. A node-lease record does not self-expire while the
	// node lease is published, so this also removes the old renew-driven epoch
	// churn (ADR-273).
	if cur.Node == m.NodeID && cur.Session == m.Session {
		if exclusive {
			return cur, ErrOwnerLive
		}
		m.rememberOwned(s, cur.Epoch)
		m.touchOwnerSegment(ctx, s, now)
		return cur, nil
	}
	live, lerr := m.liveFresh(ctx, cur, now)
	if lerr != nil {
		return cell.Owner{}, lerr
	}
	if live {
		m.takeoverBlocked.Add(1)
		return cur, ErrOwnerLive
	}

	takeover := cur.Node != m.NodeID || cur.Session != m.Session
	// A released lineage retains the epoch but has no predecessor session log.
	// Recovery of an empty tuple would query node-logs//.json on S3.
	if takeover && (cur.Node != "" || cur.Session != "") && m.BeforeTakeover != nil {
		if err := m.BeforeTakeover(ctx, cur); err != nil {
			m.takeoverFailed.Add(1)
			return cur, err
		}
	}
	if reset != nil {
		if err := reset(ctx, s); err != nil {
			return cur, err
		}
	}
	epoch, gerr := m.nextEpoch(ctx, s, cur.Epoch)
	if gerr != nil {
		return cell.Owner{}, gerr
	}
	o := m.newOwner(epoch, now)
	if exclusive {
		o.Address = ""
	}
	o.RetirementJob, o.RetiredEpoch = cur.RetirementJob, cur.RetiredEpoch
	data, _ := cell.MarshalOwner(o)
	if _, cerr := m.B.CAS(ctx, Key(s), data, e); cerr != nil {
		if exclusive {
			m.deferExclusiveRelease(s, o.Epoch)
		}
		if takeover {
			m.takeoverFailed.Add(1)
		}
		return cell.Owner{}, cerr // lost the race; caller re-resolves
	}
	if takeover {
		m.takeoverSuccess.Add(1)
	}
	m.Invalidate(s)
	m.rememberOwned(s, o.Epoch)
	return o, nil
}

// lockClaim serializes only the same scope; recovery of one large cell does
// not hold a global/striped lock over unrelated claims. Waits are cancellable.
func (m *Manager) lockClaim(ctx context.Context, key string) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m.claimMu.Lock()
		if m.claims == nil {
			m.claims = make(map[string]chan struct{})
		}
		wait, busy := m.claims[key]
		if !busy {
			done := make(chan struct{})
			m.claims[key] = done
			m.claimMu.Unlock()
			return func() {
				m.claimMu.Lock()
				delete(m.claims, key)
				close(done)
				m.claimMu.Unlock()
			}, nil
		}
		m.claimMu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Renew refreshes the lease for a scope this node owns.
func (m *Manager) Renew(ctx context.Context, s cell.Scope, epoch uint64, now time.Time) (cell.Owner, error) {
	cur, e, err := m.Resolve(ctx, s)
	if err != nil {
		return cell.Owner{}, err
	}
	if cur.Node != m.NodeID || cur.Session != m.Session || cur.Epoch != epoch {
		m.Invalidate(s)
		return cell.Owner{}, ErrEpochMismatch
	}
	if cur.Liveness == "node-lease" {
		if m.AdminAdvertise != "" && cur.AdminAddress != m.AdminAdvertise {
			cur.AdminAddress = m.AdminAdvertise
			data, _ := cell.MarshalOwner(cur)
			if _, err := m.B.CAS(ctx, Key(s), data, e); err != nil {
				return cell.Owner{}, err
			}
		}
		m.rememberOwned(s, cur.Epoch)
		m.touchOwnerSegment(ctx, s, now)
		return cur, nil
	}
	cur.Expiry = now.Add(m.OwnerTTL).UnixMilli()
	if m.AdminAdvertise != "" {
		cur.AdminAddress = m.AdminAdvertise
	}
	data, _ := cell.MarshalOwner(cur)
	if _, err := m.B.CAS(ctx, Key(s), data, e); err != nil {
		return cell.Owner{}, err
	}
	m.rememberOwned(s, cur.Epoch)
	return cur, nil
}

// Release removes the owner record if this node still owns the given epoch.
func (m *Manager) Release(ctx context.Context, s cell.Scope, epoch uint64) error {
	return m.release(ctx, s, epoch, false)
}

// ReleaseForPurge releases ownership for a resource that is being deleted. It
// skips BeforeRelease because the cell data is fenced and purged, not handed to
// a future owner.
func (m *Manager) ReleaseForPurge(ctx context.Context, s cell.Scope, epoch uint64) error {
	return m.release(ctx, s, epoch, true)
}

func (m *Manager) release(ctx context.Context, s cell.Scope, epoch uint64, skipBeforeRelease bool) error {
	unlock, err := m.lockClaim(ctx, s.String())
	if err != nil {
		return err
	}
	defer unlock()
	finish := m.beginOwnershipTransition(s)
	defer finish()
	return m.releaseLocked(ctx, s, epoch, skipBeforeRelease)
}

// beginOwnershipTransition invalidates read evidence for the whole transition,
// including ambiguous failures. It does not wait for readers; retirement callers
// must close admission and drain before acquiring the per-scope transition lock.
func (m *Manager) beginOwnershipTransition(s cell.Scope) func() {
	key := s.String()
	m.mu.Lock()
	if m.releasing == nil {
		m.releasing = make(map[string]int)
	}
	m.releasing[key]++
	m.invalidateLocked(key)
	m.mu.Unlock()
	return func() {
		m.mu.Lock()
		m.releasing[key]--
		if m.releasing[key] == 0 {
			delete(m.releasing, key)
		}
		m.invalidateLocked(key)
		m.mu.Unlock()
	}
}

func (m *Manager) releaseLocked(ctx context.Context, s cell.Scope, epoch uint64, skipBeforeRelease bool) error {
	cur, e, err := m.Resolve(ctx, s)
	if err != nil {
		return err
	}
	if cur.Node != m.NodeID || cur.Session != m.Session || cur.Epoch != epoch {
		m.Invalidate(s)
		return ErrEpochMismatch
	}
	if m.BeforeRelease != nil && !skipBeforeRelease {
		if err := m.BeforeRelease(ctx, s, epoch); err != nil {
			m.Invalidate(s)
			return err
		}
	}
	if skipBeforeRelease || cur.RetirementJob != "" {
		// celld keeps the lineage record after a purge.  Clear the live owner but
		// retain the epoch and retirement marker so a delayed predecessor cannot
		// publish into the next incarnation.
		cur.Node = ""
		cur.Role = ""
		cur.Session = ""
		cur.Expiry = 0
		cur.Liveness = ""
		cur.Address = ""
		cur.AdminAddress = ""
		if skipBeforeRelease {
			cur.Retired = true
			cur.RetiredEpoch = epoch
		}
		data, _ := cell.MarshalOwner(cur)
		if _, err := m.B.CAS(ctx, Key(s), data, e); err != nil {
			if errors.Is(err, bucket.ErrPrecondition) {
				return ErrEpochMismatch
			}
			return err
		}
		m.Invalidate(s)
		m.forgetOwned(s)
		return nil
	}
	// Conditional delete: if the lease expired and a peer re-claimed between the
	// resolve above and this delete, the CAS fails and the peer's claim survives
	// (never break the single-writer fence).
	if err := m.B.ConditionalDelete(ctx, Key(s), e); err != nil {
		if errors.Is(err, bucket.ErrPrecondition) {
			return ErrEpochMismatch
		}
		return err
	}
	m.Invalidate(s)
	m.forgetOwned(s)
	return nil
}

// touchOwnerSegment moves a node-lease owner record into the current authority
// window at most once per ownerSegmentTouchInterval. The write is best-effort:
// a failure only means a later read may fall back to the bounded List.
func (m *Manager) touchOwnerSegment(ctx context.Context, s cell.Scope, now time.Time) {
	toucher, ok := m.B.(interface {
		Touch(context.Context, string) error
	})
	if !ok {
		return
	}
	key := s.String()
	m.touchMu.Lock()
	last, seen := m.lastTouch[key]
	due := !seen || now.Sub(last) >= 30*time.Minute
	if due {
		if m.lastTouch == nil {
			m.lastTouch = map[string]time.Time{}
		}
		m.lastTouch[key] = now
	}
	m.touchMu.Unlock()
	if due {
		_ = toucher.Touch(ctx, Key(s))
	}
}

func (m *Manager) rememberOwned(s cell.Scope, epoch uint64) {
	m.ownedMu.Lock()
	if m.owned == nil {
		m.owned = map[string]OwnedScope{}
	}
	m.owned[s.String()] = OwnedScope{Scope: s, Epoch: epoch}
	m.ownedMu.Unlock()
}

func (m *Manager) forgetOwned(s cell.Scope) {
	m.ownedMu.Lock()
	delete(m.owned, s.String())
	m.ownedMu.Unlock()
	m.touchMu.Lock()
	delete(m.lastTouch, s.String())
	m.touchMu.Unlock()
}

// OwnedScopes returns a snapshot of the scopes this node currently claims.
func (m *Manager) OwnedScopes() []OwnedScope {
	m.ownedMu.Lock()
	defer m.ownedMu.Unlock()
	out := make([]OwnedScope, 0, len(m.owned))
	for _, v := range m.owned {
		out = append(out, v)
	}
	return out
}

func (m *Manager) newOwner(epoch uint64, now time.Time) cell.Owner {
	return cell.Owner{
		Node:         m.NodeID,
		Role:         m.Role,
		Session:      m.Session,
		Epoch:        epoch,
		Expiry:       now.Add(m.OwnerTTL).UnixMilli(),
		Liveness:     "node-lease",
		Address:      m.Advertise,
		AdminAddress: m.AdminAdvertise,
		ProtoVersion: cell.ProtoVersion,
	}
}

// RetryExclusiveReleases retries only maintenance owners whose acquisition
// outcome was uncertain or whose foreground cleanup failed. Exact epochs prevent an old task from releasing a successor.
func (m *Manager) RetryExclusiveReleases(ctx context.Context) error {
	m.ownedMu.Lock()
	pending := make([]OwnedScope, 0, min(len(m.pendingExclusiveRelease), 32))
	for _, o := range m.pendingExclusiveRelease {
		pending = append(pending, o)
		if len(pending) == 32 {
			break
		}
	}
	m.ownedMu.Unlock()
	var result error
	for _, o := range pending {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := m.Release(attempt, o.Scope, o.Epoch)
		cancel()
		if err == nil || errors.Is(err, ErrUnowned) || errors.Is(err, ErrEpochMismatch) {
			m.finishExclusiveRelease(o.Scope, o.Epoch)
		} else {
			result = errors.Join(result, err)
		}
	}
	return result
}
func (m *Manager) finishExclusiveRelease(sc cell.Scope, epoch uint64) {
	m.ownedMu.Lock()
	defer m.ownedMu.Unlock()
	if o, ok := m.pendingExclusiveRelease[sc.String()]; ok && o.Epoch == epoch {
		delete(m.pendingExclusiveRelease, sc.String())
	}
}

func (m *Manager) deferExclusiveRelease(sc cell.Scope, epoch uint64) {
	m.ownedMu.Lock()
	defer m.ownedMu.Unlock()
	if m.pendingExclusiveRelease == nil {
		m.pendingExclusiveRelease = make(map[string]OwnedScope)
	}
	if existing, ok := m.pendingExclusiveRelease[sc.String()]; !ok || existing.Epoch <= epoch {
		m.pendingExclusiveRelease[sc.String()] = OwnedScope{Scope: sc, Epoch: epoch}
	}
}
