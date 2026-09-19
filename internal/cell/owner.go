package cell

import (
	"encoding/json"
	"time"
)

// Role identifies which kind of node owns a cell.
type Role string

const (
	RoleCellAgent Role = "cell-agent" // backend A: KV/D1/Queue/Workflows/Cron
	RoleDORuntime Role = "do-runtime" // backend B: Durable Objects
)

// ProtoVersion is the current cell protocol version (ADR-034).
const ProtoVersion = "v1"

// SupportedProtoVersions are the cell protocol versions this build can READ.
// A peer that sends no version is treated as v1. This is the reader-before-writer
// contract (ADR-034/roadmap P5): during a rolling upgrade the new binary accepts
// the old version before any writer starts emitting a new one.
var SupportedProtoVersions = map[string]bool{"v1": true}

// SupportedProtoVersion reports whether v is readable by this build ("" = v1).
func SupportedProtoVersion(v string) bool {
	if v == "" {
		return true
	}
	return SupportedProtoVersions[v]
}

// Owner is the bucket owner record for a cell.
//
// Acquisition uses a conditional create (if absent) or CAS (if present), so at
// most one node can own a cell at a time. The record is normally refreshed only
// on activation/takeover/migration, not on the hot path.
type Owner struct {
	Node         string `json:"node"`
	Role         Role   `json:"role"`
	Session      string `json:"session"`
	Epoch        uint64 `json:"epoch"`
	Expiry       int64  `json:"expiry"` // unix millis; always <= node lease expiry
	Address      string `json:"address"`
	ProtoVersion string `json:"proto_version"`
}

// Expired reports whether the owner record has expired at now.
func (o Owner) Expired(now time.Time) bool {
	return now.UnixMilli() >= o.Expiry
}

// MarshalOwner encodes an owner record.
func MarshalOwner(o Owner) ([]byte, error) {
	if o.ProtoVersion == "" {
		o.ProtoVersion = ProtoVersion
	}
	return json.Marshal(o)
}

// UnmarshalOwner decodes an owner record.
func UnmarshalOwner(b []byte) (Owner, error) {
	var o Owner
	err := json.Unmarshal(b, &o)
	return o, err
}
