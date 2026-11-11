// Package lease manages per-node liveness leases in the object store.
package lease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cellhive/internal/bucket"
)

// ErrFenced means this process has irreversibly lost node authority. Only a
// new process session, after startup recovery, may publish again.
var ErrFenced = errors.New("node lease fenced; restart with a new session")

// Load is the node load sample exposed for discovery and autoscaling.
type Load struct {
	OwnedCells     int   `json:"owned_cells"`
	Weight         int   `json:"placement_weight"`
	ResidentCells  int   `json:"resident_cells"`
	RSSBytes       int64 `json:"rss_bytes"`
	CPUPercentX100 int   `json:"cpu_percent_x100"`
	Pressured      bool  `json:"pressured"`
	MemoryHeadroom int64 `json:"memory_headroom"`
	ShedCells      int   `json:"shed_cells"`
	Restoring      int   `json:"restoring"`
	SampledMs      int64 `json:"sampled_ms"`
}

// NodeLease is a node's self-published liveness record.
type NodeLease struct {
	Node    string `json:"node"`
	Session string `json:"session"`
	// Instance is a random id persisted in the data directory. It lets a
	// restarted process prove that a still-live lease belongs to its own
	// previous incarnation rather than another host using the same node id.
	Instance string `json:"instance,omitempty"`
	// Liveness names the owner-liveness protocol this node speaks. Nodes that
	// predate ADR-273 omit it, which is how a new node recognises (and refuses
	// to run beside) a live legacy peer.
	Liveness  string `json:"liveness,omitempty"`
	Advertise string `json:"advertise"`
	PeerURL   string `json:"peer_url"`
	// AZ is the node's failure domain (rack/zone), used to prefer cross-AZ
	// followers so one zone loss cannot take all replicas (ADR-151).
	AZ           string `json:"az,omitempty"`
	Expiry       int64  `json:"expiry"`
	ProtoVersion string `json:"proto_version"`
	Load         Load   `json:"load"`
}

// NodeLeaseLiveness gates owner, resource-fence and recovery compatibility.
// Pre-v3 recovery writers could misinterpret identified logs as legacy Held
// logs, so ADR-285 requires a coordinated replacement, not a rolling mix.
const NodeLeaseLiveness = "node-lease-fence-v2-recovery-v3-retirement-v1"

// Key returns the bucket key for a node lease.
func Key(node string) string { return "nodes/" + node + ".json" }

// Manager publishes and reads node leases.
type Manager struct {
	B       bucket.Bucket
	NodeID  string
	Session string
	Advert  string
	PeerURL string
	// AZ is this node's failure domain, published for cross-AZ follower choice.
	AZ       string
	TTL      time.Duration
	Instance string
	renewKey string

	// publishedExpiry is the Expiry of the newest lease this node successfully
	// wrote. Peers judge us dead once it passes, so Healthy measures from it and
	// not from when the PUT happened to return.
	publishedExpiry    time.Time
	healthRevision     uint64
	healthBroken       bool
	fenced             bool
	fenceReason        string
	publicationStarted time.Time
	publicationPhase   string
	publishMu          sync.Mutex
	publishedETag      string
	pending            *pendingPublication

	mu         sync.Mutex
	cached     []NodeLease
	cachedAt   time.Time
	refreshing bool
	retryAt    time.Time
	// observed fences late discovery results against newer point reads.
	observed     map[string]time.Time
	knownRunning bool
}

// NewManager builds a lease manager.
func NewManager(b bucket.Bucket, nodeID, session, advertise, peerURL string, ttl time.Duration) *Manager {
	return &Manager{B: b, NodeID: nodeID, Session: session, Advert: advertise, PeerURL: peerURL, TTL: ttl, renewKey: Key(nodeID)}
}

// Publish writes (or renews) this node's lease.
func (m *Manager) Publish(ctx context.Context, load Load) error {
	m.publishMu.Lock()
	defer m.publishMu.Unlock()
	m.mu.Lock()
	m.publicationStarted = time.Now()
	m.publicationPhase = "prepare"
	m.mu.Unlock()
	if m.Fenced() {
		return ErrFenced
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	budget := m.TTL / 3
	if !m.publishedExpiry.IsZero() {
		budget = min(budget, time.Until(m.publishedExpiry)/2)
	}
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(bucket.WithDeferredAuthorityRetirement(ctx), budget)
	defer cancel()
	if m.pending != nil {
		confirmed, err := m.confirmPublication(ctx)
		if err != nil || confirmed {
			return err
		}
		return m.writePendingPublication(ctx, budget)
	}
	if load.SampledMs == 0 {
		load.SampledMs = time.Now().UnixMilli()
	}
	expiry := time.Now().Add(m.TTL)
	// Never shorten the actual bucket lease while keeping a longer local proof.
	m.mu.Lock()
	if m.publishedExpiry.After(expiry) {
		expiry = m.publishedExpiry
	}
	m.mu.Unlock()
	l := NodeLease{
		Node: m.NodeID, Session: m.Session, Instance: m.Instance,
		Advertise: m.Advert, PeerURL: m.PeerURL, AZ: m.AZ,
		Expiry: expiry.UnixMilli(), ProtoVersion: "v1",
		Liveness: NodeLeaseLiveness, Load: load,
	}
	data, err := json.Marshal(l)
	if err != nil {
		return err
	}
	expected := m.publishedETag
	if expected == "" {
		m.setPublicationPhase("initial_read")
		previous, etag, rerr := m.B.Get(ctx, m.renewKey)
		switch {
		case errors.Is(rerr, bucket.ErrNotFound):
		case rerr != nil:
			return fmt.Errorf("read initial lease: %w", rerr)
		default:
			var prev NodeLease
			if err := json.Unmarshal(previous, &prev); err != nil {
				return fmt.Errorf("decode initial lease: %w", err)
			}
			// The process holds its data-directory instance lock. Only that same
			// persistent instance may replace its live predecessor during restart.
			if prev.Live(time.Now()) && prev.Session != m.Session && (m.Instance == "" || prev.Instance != m.Instance) {
				return fmt.Errorf("node lease held by another instance")
			}
			expected = etag
		}
	}
	// A slow initial read must not let a previously held lease cross expiry.
	if m.Fenced() {
		return ErrFenced
	}
	m.pending = &pendingPublication{data: data, expected: expected, expiry: expiry}
	return m.writePendingPublication(ctx, budget)
}

// Retry exactly the same conditional operation: a canceled request may still
// commit remotely after a confirming read returned the previous value.
func (m *Manager) writePendingPublication(ctx context.Context, budget time.Duration) error {
	if m.Fenced() {
		return ErrFenced
	}
	p := m.pending
	writeCtx, cancelWrite := context.WithTimeout(ctx, budget/2)
	var etag string
	var err error
	m.setPublicationPhase("conditional_write")
	if p.expected == "" {
		etag, err = m.B.ConditionalCreate(writeCtx, m.renewKey, p.data)
	} else {
		etag, err = m.B.CAS(writeCtx, m.renewKey, p.data, p.expected)
	}
	cancelWrite()
	if err != nil {
		confirmed, confirmErr := m.confirmPublication(ctx)
		if !confirmed && errors.Is(err, bucket.ErrPrecondition) {
			// A definitive CAS conflict revokes authority even if the confirming
			// read is unavailable. Only an exact successful-write proof overrides it.
			return m.fenceReplaced()
		}
		if confirmErr != nil {
			return fmt.Errorf("publish lease (%v); confirmation: %w", err, confirmErr)
		}
		if confirmed {
			return nil
		}
		return fmt.Errorf("publish lease: %w", err)
	}
	m.pending = nil
	return m.acceptPublication(etag, p.expiry)
}

type pendingPublication struct {
	data     []byte
	expected string
	expiry   time.Time
}

// Only the exact attempted value can resolve an ambiguous write. The previous
// deadline is checked again in acceptPublication; confirmation cannot revive it.
func (m *Manager) confirmPublication(ctx context.Context) (bool, error) {
	p := m.pending
	m.setPublicationPhase("confirm_read")
	data, etag, err := m.B.Get(ctx, m.renewKey)
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) && p.expected == "" {
			return false, nil
		}
		return false, err
	}
	if etag == p.expected {
		// Seeing the old version does not rule out a delayed remote commit.
		return false, nil
	}
	if bytes.Equal(data, p.data) {
		if err := m.acceptPublication(etag, p.expiry); err != nil {
			return false, err
		}
		m.pending = nil
		return true, nil
	}
	return false, m.fenceReplaced()
}

func (m *Manager) acceptPublication(etag string, expiry time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if m.expiredLocked(now) {
		return ErrFenced
	}
	if !now.Before(expiry) || now.UnixMilli() >= expiry.UnixMilli() {
		// An initial write that returned already expired never granted authority.
		return fmt.Errorf("initial node lease write returned expired")
	}
	m.publicationPhase = "confirmed"
	m.publishedETag = etag
	// Check the old deadline before replacing it: requests may have been idle
	// throughout a safety-margin gap, so Healthy alone cannot observe it.
	_, continuous := m.healthLocked(now)
	if m.healthRevision == 0 || !continuous || m.healthBroken {
		m.healthRevision++
	}
	m.publishedExpiry = expiry
	m.healthBroken = false
	return nil
}

// expiredLocked permanently revokes a previously acquired lease at its real
// published deadline. The earlier safety margin only pauses service; a renewal
// within the original lease can still restore health without losing authority.
func (m *Manager) expiredLocked(now time.Time) bool {
	if !m.publishedExpiry.IsZero() && (!now.Before(m.publishedExpiry) || now.UnixMilli() >= m.publishedExpiry.UnixMilli()) {
		if !m.fenced {
			m.fenceReason = "lease_expired"
		}
		m.fenced = true
	}
	return m.fenced
}

// Fenced is also checked by a watchdog independently of bucket I/O, so a hung
// renewal cannot keep a dead session running indefinitely.
func (m *Manager) Fenced() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.expiredLocked(time.Now())
}

// Healthy reports whether peers will still consider this node live for the
// next margin. It measures from the Expiry of the newest lease we managed to
// write (ADR-273): a slow PUT shrinks the window instead of extending it, so a
// node never acks after the moment a peer may start taking its cells over.
func (m *Manager) Healthy(now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, healthy := m.healthLocked(now)
	return healthy
}

// HealthProof identifies one uninterrupted safe service interval. Revision
// alone is not authority: callers must check Healthy and bind the local session.
// A new revision after a gap cannot revive evidence from the previous interval.
type HealthProof struct {
	Revision uint64
	Until    time.Time
	Healthy  bool
}

func (m *Manager) HealthProof() HealthProof {
	m.mu.Lock()
	defer m.mu.Unlock()
	until, healthy := m.healthLocked(time.Now())
	return HealthProof{Revision: m.healthRevision, Until: until, Healthy: healthy && !m.healthBroken && m.healthRevision != 0}
}

func (m *Manager) healthLocked(now time.Time) (time.Time, bool) {
	margin := m.TTL / 5
	if margin <= 0 {
		margin = time.Second
	}
	until := m.publishedExpiry.Add(-margin)
	healthy := !m.expiredLocked(now) && !m.publishedExpiry.IsZero() && now.Before(until) && now.UnixMilli() < until.UnixMilli()
	if !healthy {
		m.healthBroken = true
	}
	return until, healthy
}

// HealthyUntil returns the last published lease deadline minus the self-fence
// margin. The time retains its monotonic component. Callers extending cached
// evidence across publications must also bind HealthProof.Revision.
func (m *Manager) HealthyUntil() time.Time {
	m.mu.Lock()
	exp := m.publishedExpiry
	if m.expiredLocked(time.Now()) {
		exp = time.Time{}
	}
	m.mu.Unlock()
	if exp.IsZero() {
		return time.Time{}
	}
	margin := m.TTL / 5
	if margin <= 0 {
		margin = time.Second
	}
	return exp.Add(-margin)
}

// LegacyPeers returns live nodes using an incompatible owner or resource-fence
// protocol. Mixing either protocol on one bucket is unsafe.
func LegacyPeers(leases []NodeLease, self string, now time.Time) []NodeLease {
	var out []NodeLease
	for _, l := range leases {
		if l.Node == self || !l.Live(now) || l.Liveness == NodeLeaseLiveness {
			continue
		}
		out = append(out, l)
	}
	return out
}

// LegacyPeerVisible reports whether the in-memory lease snapshot currently shows
// a live pre-ADR-273 peer. It never touches the bucket, so the write path can
// call it; the snapshot is kept fresh by the follower-selection refresh.
func (m *Manager) LegacyPeerVisible(now time.Time) bool {
	m.mu.Lock()
	cached := m.cached
	m.mu.Unlock()
	return len(LegacyPeers(cached, m.NodeID, now)) > 0
}

// NodeLease returns a node's lease from the in-memory snapshot, falling back to
// a point read for a node not present in the current snapshot. Never calls List
// on the caller's hot path.
func (m *Manager) NodeLease(ctx context.Context, node string) (NodeLease, bool, error) {
	// Do not initialize fleet discovery here: a cold snapshot must not turn a
	// single-owner lookup into List(nodes/) plus a GET for every historical node.
	m.mu.Lock()
	cached := m.cached
	m.mu.Unlock()
	for _, l := range cached {
		if l.Node == node {
			return l, true, nil
		}
	}
	started := time.Now()
	l, err := m.Get(ctx, node)
	if ctx.Err() == nil {
		m.applyKnownRead(node, l, err, started)
	}
	if errors.Is(err, bucket.ErrNotFound) {
		return NodeLease{}, false, nil
	}
	if err != nil {
		return NodeLease{}, false, err
	}
	return l, true, nil
}

// NodeLeaseFresh reads node's lease straight from the bucket. Takeover
// decisions use it: the cached snapshot can lag by minutes when its background
// refresh keeps failing, and judging a live owner dead from a stale snapshot is
// a double-writer risk. It is a single point read, never a List.
func (m *Manager) NodeLeaseFresh(ctx context.Context, node string) (NodeLease, bool, error) {
	started := time.Now()
	l, err := m.Get(ctx, node)
	if ctx.Err() == nil {
		m.applyKnownRead(node, l, err, started)
	}
	if errors.Is(err, bucket.ErrNotFound) {
		return NodeLease{}, false, nil
	}
	if err != nil {
		return NodeLease{}, false, err
	}
	return l, true, nil
}

// Get reads a single node lease.
func (m *Manager) Get(ctx context.Context, node string) (NodeLease, error) {
	data, _, err := m.B.Get(ctx, Key(node))
	if err != nil {
		return NodeLease{}, err
	}
	var l NodeLease
	if err := json.Unmarshal(data, &l); err != nil {
		return NodeLease{}, err
	}
	return l, nil
}

// Live reports whether a lease is unexpired at now.
func (l NodeLease) Live(now time.Time) bool { return now.UnixMilli() < l.Expiry }

// HasShedTarget reports whether some live peer has placement headroom (it owns
// fewer cells than its share), so a pressured node may refuse new claims knowing
// another node can take them (ADR-123). A single node therefore never refuses
// work: it keeps claiming and relies on the disk janitor to free space.
func HasShedTarget(leases []NodeLease, self string, now time.Time) bool {
	for _, l := range leases {
		if l.Node == self || !l.Live(now) || l.Load.Pressured {
			continue
		}
		weight := l.Load.Weight
		if weight <= 0 {
			weight = 1
		}
		if l.Load.OwnedCells < weight {
			return true
		}
	}
	return false
}

// Sample lists all node leases (operator/diagnostic use; never on hot path).
func (m *Manager) Sample(ctx context.Context) ([]NodeLease, error) {
	var keys []string
	var err error
	if lister, ok := m.B.(bucket.ImmediateLister); ok {
		keys, err = lister.ListImmediate(ctx, "nodes/")
	} else {
		keys, err = m.B.List(ctx, "nodes/")
	}
	if err != nil {
		return nil, err
	}
	// Bound discovery I/O so historical nodes cannot starve later live nodes
	// within refresh's deadline. A cancelled pass must never publish a partial list.
	results := make([]NodeLease, len(keys))
	valid := make([]bool, len(keys))
	failures := make([]error, len(keys))
	slots := make(chan struct{}, 8)
	var workers sync.WaitGroup
scan:
	for i, k := range keys {
		if strings.Contains(strings.TrimPrefix(k, "nodes/"), "/") || !strings.HasSuffix(k, ".json") {
			continue
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			break scan
		}
		workers.Add(1)
		go func(i int, k string) {
			defer workers.Done()
			defer func() { <-slots }()
			data, _, err := m.B.Get(ctx, k)
			if errors.Is(err, bucket.ErrNotFound) {
				return
			}
			if err != nil {
				failures[i] = fmt.Errorf("sample node lease %s: %w", k, err)
				return
			}
			if err := json.Unmarshal(data, &results[i]); err != nil {
				failures[i] = fmt.Errorf("decode node lease %s: %w", k, err)
				return
			}
			valid[i] = true
		}(i, k)
	}
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, err := range failures {
		if err != nil {
			return nil, err
		}
	}
	out := make([]NodeLease, 0, len(keys))
	for i, l := range results {
		if valid[i] {
			out = append(out, l)
		}
	}
	return out, nil
}

// SampleCached returns the latest node snapshot without waiting for bucket I/O.
// Both initial population and stale refresh run in the background. An empty
// cache returns nil; callers must use the bucket durability posture until a
// usable follower snapshot is available.
func (m *Manager) SampleCached(_ context.Context, ttl time.Duration) []NodeLease {
	m.mu.Lock()
	cached := m.cached
	fresh := cached != nil && time.Since(m.cachedAt) < ttl
	refresh := !fresh && !m.refreshing && time.Now().After(m.retryAt)
	if refresh {
		m.refreshing = true
	}
	m.mu.Unlock()
	if refresh {
		go m.refresh()
	}
	return cached
}

// refresh updates the lease snapshot without blocking a request path. The
// object-store List can take seconds in SG; writes only need the previous
// snapshot to choose a follower.
func (m *Manager) refresh() {
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sample, err := m.Sample(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshing = false
	if err != nil {
		m.retryAt = time.Now().Add(5 * time.Second)
		return
	}
	m.mergeSampleLocked(sample, started)
	m.cachedAt = time.Now()
}

// SetCachedForTest seeds the in-memory lease snapshot. Tests in other packages
// use it to exercise snapshot-driven checks without a bucket.
func (m *Manager) SetCachedForTest(leases []NodeLease) {
	m.mu.Lock()
	m.cached, m.cachedAt = leases, time.Now()
	m.mu.Unlock()
}

// CachedNodeLease returns only an observed lease snapshot. It never performs
// I/O or invents a new expiry; forwarding may use it as a destination hint.
func (m *Manager) CachedNodeLease(node string) (NodeLease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range m.cached {
		if l.Node == node {
			return l, true
		}
	}
	return NodeLease{}, false
}

// Remaining reports only confirmed authority, never an in-flight renewal.
func (m *Manager) Remaining() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishedExpiry.IsZero() {
		return 0
	}
	return time.Until(m.publishedExpiry)
}

func (m *Manager) setPublicationPhase(phase string) {
	m.mu.Lock()
	m.publicationPhase = phase
	m.mu.Unlock()
}

// Diagnostics never waits for the publication lock or bucket I/O. In particular
// the watchdog can report which operation was pending when authority expired.
func (m *Manager) Diagnostics() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return map[string]any{"phase": m.publicationPhase, "started": m.publicationStarted, "confirmed_expiry": m.publishedExpiry, "fence_reason": m.fenceReason}
}

func (m *Manager) fenceReplaced() error {
	m.mu.Lock()
	m.fenced = true
	m.fenceReason = "lease_replaced"
	m.mu.Unlock()
	return fmt.Errorf("%w: lease was replaced", ErrFenced)
}
