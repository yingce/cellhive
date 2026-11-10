package dosupervisor

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func recoveryLease(t *testing.T, s *Supervisor) RecoveryClaim {
	t.Helper()
	claim := RecoveryClaim{Scope: cell.Scope{Namespace: "p", Class: "__do__", ID: "w~c~shard0"}, Node: "new", Epoch: 2}
	leases := NewOwnerLeases(t.Context(), nil)
	t.Cleanup(leases.Close)
	leases.entries[claim.Scope.String()] = &ownerLease{claim: claim, cancel: func() {}, state: LeaseState{Node: claim.Node, Epoch: claim.Epoch, Expiry: time.Now().Add(time.Minute).UnixMilli()}}
	s.OwnerLeases = leases
	s.ScopedRecovery = true
	return claim
}

func TestHostRecoveryRequiresNativeDetachAndPreservesUnrelatedFiles(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	q := addLoaderSource(t, s, m, hash, []string{"c/a"})
	token, err := s.AdmitPagingLoader(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	q.Activation = token
	path := filepath.Join(dir, hash+".1.sqlite")
	session := attachLifecycle(t, s.Paging, path)
	claim := recoveryLease(t, s)
	s.Bind("host", hash, "storage", "c")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	unrelated := filepath.Join(dir, "unrelated.sqlite")
	if err := os.WriteFile(unrelated, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.discardSupersededHost(t.Context(), "host", claim); !errors.Is(err, ErrHostResetRequired) {
		t.Fatalf("attached recovery: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(before) != string(after) {
		t.Fatal("live database changed", err)
	}
	requireLifecycle(t, s.Paging, "open", path, "", "", 409)
	requireLifecycle(t, s.Paging, "detach", path, session, "", 204)
	if err := s.discardSupersededHost(t.Context(), "host", claim); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("superseded local cache retained", err)
	}
	if data, err := os.ReadFile(unrelated); err != nil || string(data) != "keep" {
		t.Fatal("unrelated file changed", err)
	}
	if len(s.pagingLoader.entries) != 0 {
		t.Fatal("admission leaked")
	}
	// Retirement stays closed until authoritative restore finishes.
	requireLifecycle(t, s.Paging, "open", path, "", "", 409)
}

func TestHostRecoveryWaitsForOrdinaryHostDatabase(t *testing.T) {
	s, _, hash, dir := loaderFixture(t)
	claim := recoveryLease(t, s)
	s.Bind("host", hash, "storage", "c")
	path := filepath.Join(dir, hash+".sqlite")
	if err := os.WriteFile(path, []byte("old metadata cache"), 0600); err != nil {
		t.Fatal(err)
	}
	session, err := s.attachOrdinaryLoader(path)
	if err != nil || session == "" {
		t.Fatal("ordinary host not tracked", err)
	}
	if err := s.discardSupersededHost(t.Context(), "host", claim); !errors.Is(err, ErrHostResetRequired) {
		t.Fatal(err)
	}
	if err := s.detachOrdinaryLoader(path, "wrong"); err == nil {
		t.Fatal("forged detach accepted")
	}
	if err := s.detachOrdinaryLoader(path, session); err != nil {
		t.Fatal(err)
	}
	if err := s.discardSupersededHost(t.Context(), "host", claim); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("old host metadata retained", err)
	}
}

func TestManagedHostRestoreIsIdempotentOnlyForLiveClaim(t *testing.T) {
	s, _, _, _ := loaderFixture(t)
	claim := recoveryLease(t, s)
	s.hostRecoveries = map[string]*hostRecovery{"host": {claim: claim, ready: true}}
	if _, err := s.managedHostRestore(t.Context(), "host", claim); err != nil {
		t.Fatal(err)
	}
	s.OwnerLeases.Forget(claim)
	if _, err := s.managedHostRestore(t.Context(), "host", claim); err == nil {
		t.Fatal("expired restore replay accepted")
	}
	if len(s.recoveringHosts) != 0 {
		t.Fatal("unverified owner changed admission")
	}
}

func TestManagedHostRestoreRejectsCrossScopeRecovery(t *testing.T) {
	s, _, _, _ := loaderFixture(t)
	claim := recoveryLease(t, s)
	old := claim
	old.Scope.ID = "other~c~shard0"
	s.hostRecoveries = map[string]*hostRecovery{"host": {claim: old, ready: true}}
	if _, err := s.managedHostRestore(t.Context(), "host", claim); err == nil {
		t.Fatal("different owner's host was reset")
	}
	if !s.hostRecoveries["host"].ready || len(s.recoveringHosts) != 0 {
		t.Fatal("rejected recovery changed host")
	}
}
