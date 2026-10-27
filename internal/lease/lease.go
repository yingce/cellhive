// Package lease manages per-node liveness leases in the object store.
package lease

import (
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
const NodeLeaseLiveness = "node-lease-fence-v2-recovery-v3"

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
	publishedExpiry time.Time
	fenced          bool
	publishMu       sync.Mutex
	publishedETag   string

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
	if m.Fenced() {
		return ErrFenced
	}
	if err := ctx.Err(); err != nil {
		return err
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
	var etag string
	if expected == "" {
		etag, err = m.B.ConditionalCreate(ctx, m.renewKey, data)
	} else {
		etag, err = m.B.CAS(ctx, m.renewKey, data, expected)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if m.expiredLocked(now) {
		return ErrFenced
	}
	if err != nil {
		// A changed ETag may be another process or an ambiguous earlier write.
		// Never overwrite it or guess that authority survived.
		if m.publishedETag != "" && errors.Is(err, bucket.ErrPrecondition) {
			m.fenced = true
			return fmt.Errorf("%w: lease was replaced", ErrFenced)
		}
		return fmt.Errorf("publish lease: %w", err)
	}
	if !now.Before(expiry) || now.UnixMilli() >= l.Expiry {
		// An initial write that returned already expired never granted authority.
		return fmt.Errorf("initial node lease write returned expired")
	}
	m.publishedETag = etag
	m.publishedExpiry = expiry
	return nil
}

// expiredLocked permanently revokes a previously acquired lease at its real
// published deadline. The earlier safety margin only pauses service; a renewal
// within the original lease can still restore health without losing authority.
func (m *Manager) expiredLocked(now time.Time) bool {
	if !m.publishedExpiry.IsZero() && (!now.Before(m.publishedExpiry) || now.UnixMilli() >= m.publishedExpiry.UnixMilli()) {
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
	fenced := m.expiredLocked(now)
	m.mu.Unlock()
	return !fenced && now.Before(m.HealthyUntil())
}

// HealthyUntil returns the last published lease deadline minus the self-fence
// margin. The time retains its monotonic component; renewal cannot authorize
// reuse of a cell route whose previously observed deadline has already passed.
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
	l, err := m.Get(ctx, node)
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
	l, err := m.Get(ctx, node)
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
	keys, err := m.B.List(ctx, "nodes/")
	if err != nil {
		return nil, err
	}
	// Bound discovery I/O so historical nodes cannot starve later live nodes
	// within refresh's deadline. A cancelled pass must never publish a partial list.
	results := make([]NodeLease, len(keys))
	valid := make([]bool, len(keys))
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
			if err == nil && json.Unmarshal(data, &results[i]) == nil {
				valid[i] = true
			}
		}(i, k)
	}
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
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
