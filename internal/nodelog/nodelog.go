// Package nodelog stores per-session recovery records (docs/cell-protocol.md §6).
//
// A node creates an "open" node-log before its first fleet-durable
// acknowledgement. A cold activation checks the prior owner's log: absent means
// the session never acknowledged past the bucket; sealed means recovery is
// done; open/recovering means recovery must run before restore.
package nodelog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"cellhive/internal/bucket"
)

// Status is the recovery state of a session.
type Status string

const (
	StatusOpen       Status = "open"
	StatusRecovering Status = "recovering"
	StatusSealed     Status = "sealed"

	ProtoVersionLegacy  = "v1"
	ProtoVersionSession = "v2"
)

// Record is a node-log entry.
type Record struct {
	Node    string `json:"node"`
	Session string `json:"session"`
	// Epoch is the first cell epoch observed in this process session. Each v2
	// follower fragment carries its own scope and epoch for recovery.
	Epoch        uint64            `json:"epoch"`
	Followers    []string          `json:"followers"`
	Status       Status            `json:"status"`
	ProtoVersion string            `json:"proto_version"`
	UpdatedMs    int64             `json:"updated_ms"`
	CoveredSeq   map[string]uint64 `json:"covered_seq,omitempty"`
}

// ErrNotFound is returned when a node-log record does not exist.
var ErrNotFound = errors.New("nodelog: not found")

// ErrEnsembleMismatch means a session record already exists with recovery
// coordinates different from the ensemble being opened. Reusing that record
// would make later acknowledgements unrecoverable from the recorded peers.
var ErrEnsembleMismatch = errors.New("nodelog: session ensemble mismatch")

// Manager reads and writes node-log records.
type Manager struct {
	B      bucket.Bucket
	NodeID string
}

// New creates a node-log manager for a node.
func New(b bucket.Bucket, nodeID string) *Manager { return &Manager{B: b, NodeID: nodeID} }

// Key returns the bucket key for a session.
func Key(node, session string) string { return "node-logs/" + node + "/" + session + ".json" }

// Open creates an open record for a session (conditional create). Reopening is
// a no-op only when the immutable recovery coordinates match. Follower order
// is not significant. Cell epochs may differ within one process session: each
// v2 follower fragment persists its own epoch.
func (m *Manager) Open(ctx context.Context, session string, epoch uint64, followers []string) (Record, error) {
	followers = canonicalFollowers(followers)
	r := Record{
		Node:         m.NodeID,
		Session:      session,
		Epoch:        epoch,
		Followers:    followers,
		Status:       StatusOpen,
		ProtoVersion: ProtoVersionSession,
		UpdatedMs:    time.Now().UnixMilli(),
	}
	data, _ := json.Marshal(r)
	_, err := m.B.ConditionalCreate(ctx, Key(m.NodeID, session), data)
	if err != nil {
		if errors.Is(err, bucket.ErrPrecondition) {
			existing, _, gerr := m.Get(ctx, m.NodeID, session)
			if gerr != nil {
				return Record{}, gerr
			}
			if existing.Node != m.NodeID || existing.Session != session || existing.ProtoVersion != ProtoVersionSession || !sameFollowers(existing.Followers, followers) {
				return Record{}, ErrEnsembleMismatch
			}
			return existing, nil
		}
		return Record{}, err
	}
	return r, nil
}

func canonicalFollowers(followers []string) []string {
	out := append([]string(nil), followers...)
	sort.Strings(out)
	return out
}

func sameFollowers(a, b []string) bool {
	a = canonicalFollowers(a)
	b = canonicalFollowers(b)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Get reads a record for a node/session.
func (m *Manager) Get(ctx context.Context, node, session string) (Record, string, error) {
	data, e, err := m.B.Get(ctx, Key(node, session))
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) {
			return Record{}, "", ErrNotFound
		}
		return Record{}, "", err
	}
	var r Record
	if err := json.Unmarshal(data, &r); err != nil {
		return Record{}, "", fmt.Errorf("nodelog: corrupt record: %w", err)
	}
	return r, e, nil
}

// SetStatus updates a record's status with CAS.
func (m *Manager) SetStatus(ctx context.Context, node, session string, status Status) (Record, error) {
	r, e, err := m.Get(ctx, node, session)
	if err != nil {
		return Record{}, err
	}
	r.Status = status
	r.UpdatedMs = time.Now().UnixMilli()
	data, _ := json.Marshal(r)
	if _, err := m.B.CAS(ctx, Key(node, session), data, e); err != nil {
		return Record{}, err
	}
	return r, nil
}

// SealCovered records the durable follower coverage and seals the session in
// the same CAS update. The map is keyed by follower address.
func (m *Manager) SealCovered(ctx context.Context, node, session string, covered map[string]uint64) (Record, error) {
	r, e, err := m.Get(ctx, node, session)
	if err != nil {
		return Record{}, err
	}
	r.Status = StatusSealed
	r.CoveredSeq = covered
	r.UpdatedMs = time.Now().UnixMilli()
	data, _ := json.Marshal(r)
	if _, err := m.B.CAS(ctx, Key(node, session), data, e); err != nil {
		return Record{}, err
	}
	return r, nil
}

// Seal marks a session sealed (graceful stop or completed recovery).
func (m *Manager) Seal(ctx context.Context, node, session string) (Record, error) {
	return m.SetStatus(ctx, node, session, StatusSealed)
}

// ListNode lists all records for a node (cold path; recovery only).
func (m *Manager) ListNode(ctx context.Context, node string) ([]Record, error) {
	keys, err := m.B.List(ctx, "node-logs/"+node+"/")
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(keys))
	for _, k := range keys {
		data, _, err := m.B.Get(ctx, k)
		if err != nil {
			continue
		}
		var r Record
		if json.Unmarshal(data, &r) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

// Nodes returns the distinct node ids that have node-log records. It is a cold
// path (bucket List) used only by recovery orchestration / operations, never on
// the request hot path.
func (m *Manager) Nodes(ctx context.Context) ([]string, error) {
	keys, err := m.B.List(ctx, "node-logs/")
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, k := range keys {
		rest, ok := strings.CutPrefix(k, "node-logs/")
		if !ok {
			continue
		}
		node, _, ok := strings.Cut(rest, "/")
		if ok && node != "" {
			seen[node] = true
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}
