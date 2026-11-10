package owner

import (
	"errors"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestRetirementLineageSurvivesClaimAndRelease(t *testing.T) {
	for _, delegated := range []bool{false, true} {
		name := "claim"
		if delegated {
			name = "claim-as"
		}
		t.Run(name, func(t *testing.T) {
			m, _ := newManager(t, t.TempDir(), "new")
			sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "recreated"}
			old := cell.Owner{Epoch: 7, RetirementJob: "delete-1", RetiredEpoch: 7}
			raw, _ := cell.MarshalOwner(old)
			if _, err := m.B.Put(t.Context(), Key(sc), raw); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				var o cell.Owner
				var err error
				if delegated {
					o, err = m.ClaimAs(t.Context(), sc, "delegate", "", cell.RoleCellAgent, time.Minute, time.Now())
				} else {
					o, err = m.Claim(t.Context(), sc, time.Now())
				}
				if err != nil {
					t.Fatal(err)
				}
				if o.RetirementJob != old.RetirementJob || o.RetiredEpoch != 7 || o.Retired || o.Epoch <= old.Epoch {
					t.Fatalf("claim lost lineage: %+v", o)
				}
				if delegated {
					err = m.ReleaseAs(t.Context(), sc, "delegate", o.Epoch)
				} else {
					err = m.Release(t.Context(), sc, o.Epoch)
				}
				if err != nil {
					t.Fatal(err)
				}
				released, _, err := m.Resolve(t.Context(), sc)
				if err != nil || released.Node != "" || released.RetirementJob != old.RetirementJob || released.RetiredEpoch != 7 || released.Retired {
					t.Fatalf("release lost lineage: %+v %v", released, err)
				}
				old.Epoch = o.Epoch
			}
		})
	}
}

func TestClaimAsRejectsRetiredResource(t *testing.T) {
	m, _ := newManager(t, t.TempDir(), "agent")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "deleted"}
	raw, _ := cell.MarshalOwner(cell.Owner{Epoch: 7, Retired: true, RetirementJob: "delete-1", RetiredEpoch: 7})
	if _, err := m.B.Put(t.Context(), Key(sc), raw); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ClaimAs(t.Context(), sc, "delegate", "", cell.RoleCellAgent, time.Minute, time.Now()); !errors.Is(err, ErrRetired) {
		t.Fatalf("claim deleted resource: %v", err)
	}
}
