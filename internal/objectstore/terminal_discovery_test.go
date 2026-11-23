package objectstore

import (
	"cellhive/internal/bucket"
	"context"
	"testing"
)

type terminalListingBucket struct {
	bucket.Bucket
	keys   []string
	values map[string][]byte
	calls  int
}

func (b *terminalListingBucket) ListWithResolvedValuesExcept(context.Context, string, map[string]bool) ([]string, map[string][]byte, error) {
	b.calls++
	return b.keys, b.values, nil
}
func TestTerminalDiscoveryPreservesOwnedPrefixBoundary(t *testing.T) {
	fs, _ := bucket.NewFSBucket(t.TempDir())
	for _, test := range []struct {
		name   string
		keys   []string
		values map[string][]byte
	}{
		{"foreign key", []string{"nodes/foreign.json"}, nil},
		{"foreign body", []string{"node-logs/n/s.json"}, map[string][]byte{"nodes/foreign.json": []byte("x")}},
		{"traversal", []string{"node-logs/../nodes/foreign.json"}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := &terminalListingBucket{Bucket: fs, keys: test.keys, values: test.values}
			o := NewOwned(b, PrefixNodeLogs, OwnerNodeLog)
			if _, _, err := o.ListWithResolvedValuesExcept(t.Context(), PrefixNodeLogs, nil); err == nil {
				t.Fatal("unsafe discovery accepted")
			}
		})
	}
	b := &terminalListingBucket{Bucket: fs}
	o := NewOwned(b, PrefixNodeLogs, OwnerNodeLog)
	if _, _, err := o.ListWithResolvedValuesExcept(t.Context(), "nodes/", nil); err == nil || b.calls != 0 {
		t.Fatal("invalid prefix reached backend")
	}
}
