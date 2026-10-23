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
	Node    string `json:"node"`
	Role    Role   `json:"role"`
	Session string `json:"session"`
	Epoch   uint64 `json:"epoch"`
	// Expiry is the legacy lease expiry (unix millis). Records written by
	// ADR-273 set Expiry=0 and use the node lease named by Node instead.
	Expiry   int64  `json:"expiry"`
	Liveness string `json:"liveness,omitempty"` // "node-lease" for ADR-273 records
	Address  string `json:"address"`
	// AdminAddress is where owner-forwarded admin requests should land. It is
	// separate from Address (the cell REST/peer endpoint): the admin listener
	// has its own authentication surface and must not be folded into :7001.
	AdminAddress string `json:"admin_address,omitempty"`
	ProtoVersion string `json:"proto_version"`
}

// Expired reports whether a legacy (expiry-based) owner record has expired.
// New node-lease records carry Expiry=0; callers must use owner.Manager.Live,
// which consults nodes/<node>.json, for those.
func (o Owner) Expired(now time.Time) bool {
	if o.Liveness == "node-lease" {
		return false // liveness comes from the node lease, never this field
	}
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
