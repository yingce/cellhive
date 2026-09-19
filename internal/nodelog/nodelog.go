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
)

// Record is a node-log entry.
type Record struct {
	Node         string   `json:"node"`
	Session      string   `json:"session"`
	Epoch        uint64   `json:"epoch"`
	Followers    []string `json:"followers"`
	Status       Status   `json:"status"`
	ProtoVersion string   `json:"proto_version"`
	UpdatedMs    int64    `json:"updated_ms"`
}

// ErrNotFound is returned when a node-log record does not exist.
var ErrNotFound = errors.New("nodelog: not found")

// Manager reads and writes node-log records.
type Manager struct {
	B      bucket.Bucket
	NodeID string
}

// New creates a node-log manager for a node.
func New(b bucket.Bucket, nodeID string) *Manager { return &Manager{B: b, NodeID: nodeID} }

// Key returns the bucket key for a session.
func Key(node, session string) string { return "node-logs/" + node + "/" + session + ".json" }

// Open creates an open record for a session (conditional create). If the record
// already exists with the same content it is a no-op.
func (m *Manager) Open(ctx context.Context, session string, epoch uint64, followers []string) (Record, error) {
	r := Record{
		Node:         m.NodeID,
		Session:      session,
		Epoch:        epoch,
		Followers:    followers,
		Status:       StatusOpen,
		ProtoVersion: "v1",
		UpdatedMs:    time.Now().UnixMilli(),
	}
	data, _ := json.Marshal(r)
	_, err := m.B.ConditionalCreate(ctx, Key(m.NodeID, session), data)
	if err != nil {
		if errors.Is(err, bucket.ErrPrecondition) {
			existing, _, gerr := m.Get(ctx, m.NodeID, session)
			return existing, gerr
		}
		return Record{}, err
	}
	return r, nil
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
