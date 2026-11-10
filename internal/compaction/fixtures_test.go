package compaction

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/replica"
	"context"
	"testing"
)

func newCompactionFixture(t *testing.T) (context.Context, *bucket.FSBucket, *replica.Manager, cell.Scope, uint64) {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return t.Context(), b, replica.New(b), cell.Scope{Namespace: "native", Class: "__kv__", ID: "lineage"}, 1
}
func appendSegment(t *testing.T, m *replica.Manager, s cell.Scope, e uint64, raw []byte) string {
	t.Helper()
	key, _, err := m.Append(t.Context(), s, e, raw)
	if err != nil {
		t.Fatal(err)
	}
	return key
}
