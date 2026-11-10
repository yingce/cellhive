package dosupervisor

import (
	"cellhive/internal/cell"
	"os"
	"path/filepath"
	"testing"
)

func TestObjectClassComesFromFacetIdentity(t *testing.T) {
	s, _, hash, dir := loaderFixture(t)
	if err := os.WriteFile(filepath.Join(dir, hash+".facets"), facetsBytes("SessionDO/a", "SchedulerDO/global"), 0600); err != nil {
		t.Fatal(err)
	}
	s.refreshFacets()
	for _, last := range []string{"SessionDO", "SchedulerDO"} {
		s.Bind("sid/shard0", hash, "sid", last, "app")
		for i, cls := range []string{"SessionDO", "SchedulerDO"} {
			rel := hash + ".1.sqlite"
			name := "SessionDO/a"
			if i == 1 {
				rel = hash + ".2.sqlite"
				name = "SchedulerDO/global"
			}
			got, ok := s.objectForRel(rel)
			want := cell.Scope{Namespace: "app", Class: cls, ID: objScopeID("sid", cls, name)}
			if !ok || got.Class != cls || got.Scope != want.String() {
				t.Fatalf("last bind %s: got %+v, want %s", last, got, want.String())
			}
		}
	}
}
