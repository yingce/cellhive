package cell

// Durable Object host sharding (ADR-078/080). The shard must be identical in Go
// (cell-agent placement) and JS (do-runtime host), so both use FNV-1a over the
// string "<ns>/<worker>/<class>/<objectName>" modulo DOShardCount. Byte-wise
// FNV-1a over ASCII identifiers matches JS charCodeAt exactly.
const DOShardCount = 16

func fnv1a(s string) uint32 {
	var h uint32 = 0x811c9dc5
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 0x01000193
	}
	return h
}

// DOShard returns the host shard for a Durable Object.
func DOShard(ns, worker, class, id string) int {
	return int(fnv1a(ns+"/"+worker+"/"+class+"/"+id) % DOShardCount)
}
