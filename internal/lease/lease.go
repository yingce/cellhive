// Package lease manages per-node liveness leases in the object store.
package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"cellhive/internal/bucket"
)

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

// NodeLeaseLiveness identifies the owner-liveness and resource-fence protocol.
// The prior "node-lease" release writes resource-fence.json; it must not run
// alongside nodes that use resource-fence.v2 (ADR-276).
const NodeLeaseLiveness = "node-lease-fence-v2"

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

	mu         sync.Mutex
	cached     []NodeLease
	cachedAt   time.Time
	refreshing bool
	retryAt    time.Time
}

// NewManager builds a lease manager.
func NewManager(b bucket.Bucket, nodeID, session, advertise, peerURL string, ttl time.Duration) *Manager {
	return &Manager{B: b, NodeID: nodeID, Session: session, Advert: advertise, PeerURL: peerURL, TTL: ttl, renewKey: Key(nodeID), publishedExpiry: time.Now().Add(ttl)}
}

// Publish writes (or renews) this node's lease.
func (m *Manager) Publish(ctx context.Context, load Load) error {
	if load.SampledMs == 0 {
		load.SampledMs = time.Now().UnixMilli()
	}
	expiry := time.Now().Add(m.TTL)
	l := NodeLease{
		Node:         m.NodeID,
		Session:      m.Session,
		Instance:     m.Instance,
		Advertise:    m.Advert,
		PeerURL:      m.PeerURL,
		AZ:           m.AZ,
		Expiry:       expiry.UnixMilli(),
		ProtoVersion: "v1",
		Liveness:     NodeLeaseLiveness,
		Load:         load,
	}
	data, err := json.Marshal(l)
	if err != nil {
		return err
	}
	if _, err := m.B.Put(ctx, m.renewKey, data); err != nil {
		return fmt.Errorf("publish lease: %w", err)
	}
	m.mu.Lock()
	if expiry.After(m.publishedExpiry) {
		m.publishedExpiry = expiry
	}
	m.mu.Unlock()
	return nil
}

// Healthy reports whether peers will still consider this node live for the
// next margin. It measures from the Expiry of the newest lease we managed to
// write (ADR-273): a slow PUT shrinks the window instead of extending it, so a
// node never acks after the moment a peer may start taking its cells over.
func (m *Manager) Healthy(now time.Time) bool {
	m.mu.Lock()
	exp := m.publishedExpiry
	m.mu.Unlock()
	if exp.IsZero() {
		return false
	}
	margin := m.TTL / 5
	if margin <= 0 {
		margin = time.Second
	}
	return now.Before(exp.Add(-margin))
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
	for _, l := range m.SampleCached(ctx, 5*time.Second) {
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
	out := make([]NodeLease, 0, len(keys))
	for _, k := range keys {
		data, _, err := m.B.Get(ctx, k)
		if err != nil {
			continue
		}
		var l NodeLease
		if json.Unmarshal(data, &l) == nil {
			out = append(out, l)
		}
	}
	return out, nil
}

// SampleCached returns the node list, refreshing at most once per ttl. Hot
// paths use this so they never hit the object store's List on every request.
func (m *Manager) SampleCached(ctx context.Context, ttl time.Duration) []NodeLease {
	m.mu.Lock()
	if m.cached != nil && time.Since(m.cachedAt) < ttl {
		cached := m.cached
		m.mu.Unlock()
		return cached
	}
	if m.cached != nil {
		// Stale-while-revalidate: a hot write must never block on a bucket
		// List. Return the previous snapshot and refresh in the background.
		if !m.refreshing && time.Now().After(m.retryAt) {
			m.refreshing = true
			go m.refresh()
		}
		cached := m.cached
		m.mu.Unlock()
		return cached
	}
	if m.refreshing || time.Now().Before(m.retryAt) {
		// A cold first sample is already in flight; do not pile more bucket
		// Lists behind it. The caller falls back to the bucket posture.
		m.mu.Unlock()
		return nil
	}
	m.refreshing = true
	m.mu.Unlock()

	sample, err := m.Sample(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshing = false
	if err != nil {
		m.retryAt = time.Now().Add(5 * time.Second)
		return nil
	}
	m.cached = sample
	m.cachedAt = time.Now()
	return sample
}

// refresh updates the lease snapshot without blocking a request path. The
// object-store List can take seconds in SG; writes only need the previous
// snapshot to choose a follower.
func (m *Manager) refresh() {
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
	m.cached = sample
	m.cachedAt = time.Now()
}

// SetCachedForTest seeds the in-memory lease snapshot. Tests in other packages
// use it to exercise snapshot-driven checks without a bucket.
func (m *Manager) SetCachedForTest(leases []NodeLease) {
	m.mu.Lock()
	m.cached, m.cachedAt = leases, time.Now()
	m.mu.Unlock()
}
