package owner

import (
	"cellhive/internal/cell"
	"testing"
)

func TestTrackLocalRejectsForeignAndOlderEvidence(t *testing.T) {
	sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: "track"}
	m := &Manager{NodeID: "local", Session: "current"}
	for _, o := range []cell.Owner{
		{Node: "remote", Session: "current", Epoch: 4},
		{Node: "local", Session: "old", Epoch: 4},
		{Node: "local", Session: "current", Epoch: 0},
		{Node: "local", Session: "current", Epoch: 4, Retired: true},
	} {
		m.TrackLocal(sc, o)
	}
	if got := m.OwnedScopes(); len(got) != 0 {
		t.Fatalf("tracked invalid evidence: %+v", got)
	}
	m.TrackLocal(sc, cell.Owner{Node: "local", Session: "current", Epoch: 4})
	m.TrackLocal(sc, cell.Owner{Node: "local", Session: "current", Epoch: 3})
	if got := m.OwnedScopes(); len(got) != 1 || got[0].Epoch != 4 {
		t.Fatalf("regressed epoch: %+v", got)
	}
	m.Forget(sc, 4)
	m.TrackLocal(sc, cell.Owner{Node: "local", Session: "current", Epoch: 4})
	if got := m.OwnedScopes(); len(got) != 1 || got[0].Epoch != 4 {
		t.Fatalf("did not restore advisory registration: %+v", got)
	}
}
