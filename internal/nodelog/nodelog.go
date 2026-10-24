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
	"maps"
	"sort"
	"strings"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/objectstore"
)

// Status is the recovery state of a session.
type Status string

const (
	StatusOpen       Status = "open"
	StatusRecovering Status = "recovering"
	StatusSealed     Status = "sealed"

	ProtoVersionLegacy   = "v1"
	ProtoVersionSession  = "v2"
	ProtoVersionIdentity = "v3"
)

// RecoveryResolution records an explicit operator exception, never an automatic
// recovery result. Its audit preserves original bytes and loss authorization.
type RecoveryResolution struct {
	Kind           string `json:"kind"`
	AcceptedMs     int64  `json:"accepted_ms"`
	AuditKey       string `json:"audit_key"`
	OriginalSHA256 string `json:"original_sha256"`
}

// Record is a node-log entry.
type Record struct {
	RecoveryResolution *RecoveryResolution `json:"recovery_resolution,omitempty"`
	Node               string              `json:"node"`
	Session            string              `json:"session"`
	// Epoch is the first cell epoch observed in this process session. Each v2
	// follower fragment carries its own scope and epoch for recovery.
	Epoch        uint64                       `json:"epoch"`
	FollowerRefs map[string]cell.PeerIdentity `json:"follower_refs,omitempty"`
	Progress     map[string]uint64            `json:"recovered_seq,omitempty"`
	Followers    []string                     `json:"followers"`
	Status       Status                       `json:"status"`
	ProtoVersion string                       `json:"proto_version"`
	UpdatedMs    int64                        `json:"updated_ms"`
	CoveredSeq   map[string]uint64            `json:"covered_seq,omitempty"`
}

// ErrNotFound is returned when a node-log record does not exist.
var ErrNotFound = errors.New("nodelog: not found")

// ErrSessionClosed means a session record is sealed/recovering and cannot be
// reopened for new acknowledgements.
var ErrSessionClosed = errors.New("nodelog: session already closed")

// ErrEnsembleMismatch means a session record already exists with recovery
// coordinates different from the ensemble being opened. Reusing that record
// would make later acknowledgements unrecoverable from the recorded peers.
var ErrEnsembleMismatch = errors.New("nodelog: session ensemble mismatch")

// Manager reads and writes node-log records.
type Manager struct {
	B      bucket.Bucket
	Objs   *objectstore.Objects
	NodeID string
}

// New creates a node-log manager for a node. The owned object view enforces
// that every node-log key stays under the reserved node-logs/ prefix.
func New(b bucket.Bucket, nodeID string) *Manager {
	return &Manager{
		B:      b,
		Objs:   objectstore.NewOwned(b, objectstore.PrefixNodeLogs, objectstore.OwnerNodeLog),
		NodeID: nodeID,
	}
}

func (m *Manager) get(ctx context.Context, key string) ([]byte, string, error) {
	if m.Objs != nil {
		return m.Objs.GetVersion(ctx, key)
	}
	return m.B.Get(ctx, key)
}

func (m *Manager) conditionalCreate(ctx context.Context, key string, value []byte) (string, error) {
	if m.Objs != nil {
		return m.Objs.ConditionalCreate(ctx, key, value)
	}
	return m.B.ConditionalCreate(ctx, key, value)
}

func (m *Manager) cas(ctx context.Context, key string, value []byte, etag string) (string, error) {
	if m.Objs != nil {
		return m.Objs.CAS(ctx, key, value, etag)
	}
	return m.B.CAS(ctx, key, value, etag)
}

func (m *Manager) list(ctx context.Context, prefix string) ([]string, error) {
	if m.Objs != nil {
		return m.Objs.ListPrefix(ctx, prefix)
	}
	return m.B.List(ctx, prefix)
}

// Key returns the bucket key for a session.
func Key(node, session string) string { return "node-logs/" + node + "/" + session + ".json" }

// Open creates an open record for a session (conditional create). Reopening is
// a no-op only when the immutable recovery coordinates match. Follower order
// is not significant. Cell epochs may differ within one process session: each
// v2 follower fragment persists its own epoch.
func (m *Manager) Open(ctx context.Context, session string, epoch uint64, followers []string) (Record, error) {
	return m.open(ctx, session, epoch, followers, nil)
}

func (m *Manager) OpenIdentified(ctx context.Context, session string, epoch uint64, refs map[string]cell.PeerIdentity) (Record, error) {
	var followers []string
	seen := map[cell.PeerIdentity]bool{}
	for addr, id := range refs {
		if addr == "" || id.Node == "" || id.Instance == "" || seen[id] {
			return Record{}, ErrEnsembleMismatch
		}
		seen[id] = true
		followers = append(followers, addr)
	}
	if len(followers) == 0 {
		return Record{}, ErrEnsembleMismatch
	}
	return m.open(ctx, session, epoch, followers, refs)
}

func (m *Manager) open(ctx context.Context, session string, epoch uint64, followers []string, refs map[string]cell.PeerIdentity) (Record, error) {
	followers = canonicalFollowers(followers)
	proto := ProtoVersionSession
	if refs != nil {
		proto = ProtoVersionIdentity
	}
	r := Record{
		Node:         m.NodeID,
		Session:      session,
		Epoch:        epoch,
		Followers:    followers,
		Status:       StatusOpen,
		ProtoVersion: proto,
		FollowerRefs: maps.Clone(refs),
		UpdatedMs:    time.Now().UnixMilli(),
	}
	data, _ := json.Marshal(r)
	_, err := m.conditionalCreate(ctx, Key(m.NodeID, session), data)
	if err != nil {
		if errors.Is(err, bucket.ErrPrecondition) {
			existing, _, gerr := m.Get(ctx, m.NodeID, session)
			if gerr != nil {
				return Record{}, gerr
			}
			if existing.Node != m.NodeID || existing.Session != session || existing.ProtoVersion != proto || !sameFollowers(existing.Followers, followers) || !maps.Equal(existing.FollowerRefs, refs) {
				return Record{}, ErrEnsembleMismatch
			}
			if existing.Status != StatusOpen {
				return Record{}, fmt.Errorf("%w: %s", ErrSessionClosed, existing.Status)
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
	data, e, err := m.get(ctx, Key(node, session))
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
	if r.Status == StatusSealed && status != StatusSealed {
		return r, ErrSessionClosed
	}
	r.Status = status
	r.UpdatedMs = time.Now().UnixMilli()
	data, _ := json.Marshal(r)
	if _, err := m.cas(ctx, Key(node, session), data, e); err != nil {
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
	if _, err := m.cas(ctx, Key(node, session), data, e); err != nil {
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
	keys, err := m.list(ctx, "node-logs/"+node+"/")
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(keys))
	for _, k := range keys {
		data, _, err := m.get(ctx, k)
		if err != nil {
			if errors.Is(err, bucket.ErrNotFound) {
				continue
			}
			// Recovery must fail closed: treating an unreadable session as absent
			// would skip the only copy of follower-acked segments.
			return nil, fmt.Errorf("nodelog: read %s: %w", k, err)
		}
		var r Record
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, fmt.Errorf("nodelog: decode %s: %w", k, err)
		}
		out = append(out, r)
	}
	return out, nil
}

// Nodes returns the distinct node ids that have node-log records. It is a cold
// path (bucket List) used only by recovery orchestration / operations, never on
// the request hot path.
func (m *Manager) Nodes(ctx context.Context) ([]string, error) {
	keys, err := m.list(ctx, "node-logs/")
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

// Checkpoint persists only fragments already uploaded to the bucket. CAS
// contention never loses another recoverer's progress; replay is idempotent.
func (m *Manager) Checkpoint(ctx context.Context, node, session, follower string, after uint64) error {
	for attempts := 0; attempts < 8; attempts++ {
		r, e, err := m.Get(ctx, node, session)
		if err != nil {
			return err
		}
		if r.Status == StatusSealed || r.Progress[follower] >= after {
			return nil
		}
		if r.Status != StatusRecovering {
			return ErrSessionClosed
		}
		if r.Progress == nil {
			r.Progress = map[string]uint64{}
		}
		r.Progress[follower] = after
		r.UpdatedMs = time.Now().UnixMilli()
		data, _ := json.Marshal(r)
		_, err = m.cas(ctx, Key(node, session), data, e)
		if errors.Is(err, bucket.ErrPrecondition) {
			continue
		}
		return err
	}
	return bucket.ErrPrecondition
}
