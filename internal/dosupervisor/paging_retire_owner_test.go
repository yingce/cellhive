package dosupervisor

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/sqlcapture"
)

func TestPagingRetirementKeepsActivationOwner(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "live"
		if expired {
			name = "expired"
		}
		t.Run(name, func(t *testing.T) {
			s, m, hash, dir := loaderFixture(t)
			q := addLoaderSource(t, s, m, hash, []string{"c/claimed"})
			claim := RecoveryClaim{Scope: cell.Scope{Namespace: "p", Class: "__do__", ID: "w~c~shard0"}, Node: "runtime-a", Epoch: 5}
			s.Segments = &checkpointRestoreSource{rep: m}
			q.OwnerScope = claim.Scope.String()
			q.OwnerNode = claim.Node
			q.OwnerEpoch = claim.Epoch
			leases := NewOwnerLeases(t.Context(), nil)
			defer leases.Close()
			lease := &ownerLease{claim: claim, cancel: func() {}, state: LeaseState{Node: claim.Node, Epoch: claim.Epoch, Expiry: time.Now().Add(time.Minute).UnixMilli()}}
			leases.entries[claim.Scope.String()] = lease
			s.OwnerLeases = leases
			token, err := s.AdmitPagingLoader(t.Context(), q)
			if err != nil {
				t.Fatal(err)
			}
			q.Activation = token
			path := filepath.Join(dir, hash+".1.sqlite")
			ctx := sqlcapture.WithDOOwner(t.Context(), sqlcapture.DOOwnerIdentity{Scope: claim.Scope, Node: claim.Node, Epoch: claim.Epoch})
			if err = s.syncFile(ctx, path); err != nil {
				t.Fatal(err)
			}
			if expired {
				lease.state.Expiry = time.Now().Add(-time.Second).UnixMilli()
			}
			// Retirement must use the registered activation, not caller-supplied owner fields.
			q.OwnerEpoch = 99
			q.OwnerNode = "untrusted"
			err = s.RetirePagingLoader(t.Context(), q)
			if expired {
				if err == nil {
					t.Fatal("retired expired owner")
				}
				if _, e := os.Stat(path); e != nil {
					t.Fatalf("lost unretired cache: %v", e)
				}
				return
			}
			if err != nil {
				t.Fatalf("retire claimed activation: %v", err)
			}
			if _, err = os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("cache not retired: %v", err)
			}
			if len(s.pagingLoader.entries) != 0 {
				t.Fatal("admission capacity not released")
			}
		})
	}
}

func TestClosingPagingLoaderAllowsNativeCheckpointCapture(t *testing.T) {
	s, m, hash, dir := loaderFixture(t)
	q := addLoaderSource(t, s, m, hash, []string{"c/closing"})
	token, err := s.AdmitPagingLoader(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	q.Activation = token
	path := filepath.Join(dir, hash+".1.sqlite")
	id := attachLifecycle(t, s.Paging, path)
	if err = s.RetirePagingLoader(t.Context(), q); err != ErrPagingBusy {
		t.Fatalf("retire native handle: %v", err)
	}
	requireLifecycle(t, s.Paging, "open", path, "", "", 409)
	if _, err = s.captureFile(path); err != ErrPagingRestoreRequired {
		t.Fatalf("ordinary capture while closing: %v", err)
	}
	fs, err := s.captureFileForCheckpoint(path, true)
	if err != nil {
		t.Fatalf("native close checkpoint capture: %v", err)
	}
	fs.mu.Unlock()
	requireLifecycle(t, s.Paging, "detach", path, id, "", 204)
	if _, err = s.captureFileForCheckpoint(path, true); err != ErrPagingRestoreRequired {
		t.Fatalf("checkpoint after native detach: %v", err)
	}
}
