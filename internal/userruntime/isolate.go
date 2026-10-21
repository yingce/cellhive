package userruntime

// Warm-isolate bounds for the public loader. Deployments set them through
// CH_ISOLATE_MAX / CH_ISOLATE_BYTES (CELLHIVE_ISOLATE_MAX /
// CELLHIVE_ISOLATE_BYTES); no host or cgroup probing is performed.
const (
	// DefaultIsolateMax is the fallback number of warm worker variants.
	DefaultIsolateMax = 128
	// IsolateEstimateBytes is the conservative per-resident weight used to
	// derive the byte budget when only the count is configured.
	IsolateEstimateBytes = int64(2) << 20
	// DefaultIsolateBytes is the fallback estimated byte budget (MAX × estimate).
	DefaultIsolateBytes = int64(DefaultIsolateMax) * IsolateEstimateBytes
)

// IsolateBytesFor derives the default byte budget from a resident count.
func IsolateBytesFor(max int) int64 {
	if max <= 0 {
		return DefaultIsolateBytes
	}
	return int64(max) * IsolateEstimateBytes
}
