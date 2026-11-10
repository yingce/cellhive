package dosupervisor

import (
	"errors"
	"os"
	"testing"
)

func TestRestoreConflictPreservesLiveView(t *testing.T) {
	p, rep, scope, path := pagingFixture(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := &Supervisor{Dir: p.root, Store: rep, Paging: p, Epoch: 1}
	for i := 0; i < 2; i++ {
		err = s.restoreScope(t.Context(), scope, path)
		if !errors.Is(err, ErrPagingRegistered) {
			t.Fatalf("restore conflict: %v", err)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil || string(before) != string(after) {
		t.Fatalf("live database changed: %v", err)
	}
	// A conflicting restore must not discard the existing native registration.
	v, err := p.lookup(path)
	if err != nil || v == nil {
		t.Fatalf("lost live view: %v", err)
	}
}
