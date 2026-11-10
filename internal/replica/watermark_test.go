package replica

import (
	"testing"

	"cellhive/internal/cell"
)

func TestNativeWatermarkRejectsMissingPredecessorBelowCut(t *testing.T) {
	m := newManager(t)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "missing-cut"}
	putNativeEpochFile(t, m, sc, 1, 9, 1, 5, false)
	putNativeEpochFile(t, m, sc, 1, 9, 1, 20, false) // late snapshot beyond successor cut
	putNativeEpochFile(t, m, sc, 2, 0, 8, 8, true)
	if _, err := m.DurableWatermarkChain(t.Context(), sc, 2); err == nil {
		t.Fatal("missing predecessor6-7 was hidden")
	}
}
