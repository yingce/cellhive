package cell

// PeerIdentity binds replication to a node's persistent data directory, not an
// address or process incarnation. A replacement disk is a different replica.
type PeerIdentity struct {
	Node     string `json:"node"`
	Instance string `json:"instance"`
}
