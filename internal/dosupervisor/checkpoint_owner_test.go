package dosupervisor

import (
	"cellhive/internal/bucket"
	"cellhive/internal/cellstore"
	"cellhive/internal/replica"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/sqlcapture"
)

func TestCheckpointOwnerContext(t *testing.T) {
	claim := RecoveryClaim{Scope: cell.Scope{Namespace: "demo", Class: "__do__", ID: "w~C~shard0"}, Node: "do-b", Epoch: 3}
	for _, kind := range []string{"live", "expired", "replaced", "unmanaged", "no-manager", "administrative"} {
		t.Run(kind, func(t *testing.T) {
			leases := NewOwnerLeases(t.Context(), nil)
			defer leases.Close()
			entry := &ownerLease{claim: claim, cancel: func() {}, state: LeaseState{Node: claim.Node, Epoch: claim.Epoch, Expiry: time.Now().Add(time.Minute).UnixMilli()}}
			leases.entries[claim.Scope.String()] = entry
			s := &Supervisor{Epoch: 1, OwnerLeases: leases}
			switch kind {
			case "expired":
				entry.state.Expiry = time.Now().Add(-time.Second).UnixMilli()
			case "replaced":
				entry.claim.Epoch++
			case "unmanaged":
				delete(leases.entries, claim.Scope.String())
			case "no-manager":
				s.OwnerLeases = nil
			}
			input := claim
			if kind == "administrative" {
				input = RecoveryClaim{}
			}
			ctx, err := s.checkpointContext(context.Background(), input)
			if kind != "live" && kind != "administrative" {
				if err == nil {
					t.Fatal("invalid owner accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/internal/resolve" {
					_, _ = w.Write([]byte(`{"owned":true,"owner":{"epoch":7}}`))
					return
				}
				if kind == "live" {
					if r.Header.Get("x-cellhive-do-owner-scope") != claim.Scope.String() || r.Header.Get("x-cellhive-do-owner-node") != claim.Node || r.Header.Get("x-cellhive-do-owner-epoch") != "3" || r.Header.Get("x-cellhive-bucket-proof") != "" {
						t.Error("checkpoint lost exact fleet identity")
						http.Error(w, "bad identity", 400)
						return
					}
					_, _ = w.Write([]byte(`{"mode":"fleet"}`))
				} else {
					if r.Header.Get("x-cellhive-bucket-proof") != "1" {
						t.Error("administrative capture lost bucket proof")
					}
					_, _ = w.Write([]byte(`{"mode":"bucket"}`))
				}
			}))
			defer server.Close()
			c := sqlcapture.NewHTTPCommitter(server.URL, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", nil, nil)
			c.RequireDOOwner = true
			if err := c.Commit(ctx, cell.Scope{Namespace: "workerd", Class: "__do__", ID: "facet"}, s.captureDataEpoch(ctx), []byte("segment")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Exercise the actual claimed restore and checkpoint entry point, not just
// context construction: losing the claim at registration must fail this test.
func TestClaimedRestoreCheckpointCarriesFleetOwner(t *testing.T) {
	const hostID, hash = "owner/shard0", "deadbeef00"
	source := t.TempDir()
	rel := filepath.Join("cellhive-do-host", hash+".sqlite")
	if err := os.MkdirAll(filepath.Dir(filepath.Join(source, rel)), 0700); err != nil {
		t.Fatal(err)
	}
	mkSQLiteWithRow(t, filepath.Join(source, rel), 5)
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	original := &Supervisor{Dir: source, Epoch: 1, ScopePrefix: "workerd/__do__", Bucket: b, Committer: &bucketCommitter{rep: rep, epoch: 1}}
	original.Bind(hostID, hash, "storage", "Tenant")
	if _, err := original.SyncAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	claim := RecoveryClaim{Scope: cell.Scope{Namespace: "demo", Class: "__do__", ID: "w~C~shard0"}, Node: "do-b", Epoch: 3}
	leases := NewOwnerLeases(t.Context(), nil)
	defer leases.Close()
	leases.entries[claim.Scope.String()] = &ownerLease{claim: claim, cancel: func() {}, state: LeaseState{Node: claim.Node, Epoch: claim.Epoch, Expiry: time.Now().Add(time.Minute).UnixMilli()}}
	var commits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/internal/resolve" {
			_, _ = w.Write([]byte(`{"owned":true,"owner":{"epoch":7}}`))
			return
		}
		if r.URL.Path == "/v1/internal/claim" {
			_, _ = w.Write([]byte(`{"owner":{"epoch":7}}`))
			return
		}
		if r.Header.Get("x-cellhive-do-owner-scope") != claim.Scope.String() || r.Header.Get("x-cellhive-do-owner-node") != claim.Node || r.Header.Get("x-cellhive-do-owner-epoch") != "3" || r.Header.Get("x-cellhive-bucket-proof") != "" {
			t.Error("claimed restore lost checkpoint fleet owner")
			http.Error(w, "owner missing", 400)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		commits.Add(1)
		_, _ = w.Write([]byte(`{"mode":"fleet"}`))
	}))
	defer server.Close()
	c := sqlcapture.NewHTTPCommitter(server.URL, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", nil, nil)
	c.RequireDOOwner = true
	dest := t.TempDir()
	paging, err := NewPaging(dest, strings.Repeat("t", 32))
	if err != nil {
		t.Fatal(err)
	}
	defer paging.Close()
	segments := &checkpointRestoreSource{rep: rep}
	restored := &Supervisor{Dir: dest, Epoch: 1, ScopePrefix: "workerd/__do__", Bucket: b, Committer: c, Segments: segments, Paging: paging, OwnerLeases: leases}
	if n, err := restored.RestoreHostClaimed(t.Context(), dest, hostID, claim); err != nil || n != 1 {
		t.Fatalf("restore=%d %v", n, err)
	}
	if segments.claim != claim {
		t.Fatal("recovery lost shard identity")
	}
	path := filepath.Join(dest, rel)
	if err := paging.Hydrate(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE checkpoint_value(a); INSERT INTO checkpoint_value VALUES(42)"); err != nil {
		t.Fatal(err)
	}
	id, _, err := restored.prepareCheckpoint(t.Context(), path)
	if err != nil || id == 0 || commits.Load() == 0 {
		t.Fatalf("prepare=%d commits=%d err=%v", id, commits.Load(), err)
	}
}

type checkpointRestoreSource struct {
	rep   *replica.Manager
	claim RecoveryClaim
}

func (s *checkpointRestoreSource) RecoverScope(_ context.Context, _ cell.Scope, c RecoveryClaim) error {
	s.claim = c
	return nil
}
func (s *checkpointRestoreSource) PageFetcher(ctx context.Context, sc cell.Scope, epoch uint64) (*replica.PageFetcher, error) {
	return s.rep.NewPageFetcher(ctx, sc, epoch)
}
func (s *checkpointRestoreSource) Restore(context.Context, cell.Scope, uint64) ([][]byte, error) {
	return nil, errors.New("unexpected buffered restore")
}

func TestLostCheckpointOwnerRejectsBeforeMutationAndAllowsDetach(t *testing.T) {
	p, _, _, path := pagingFixture(t)
	v, err := p.lookup(path)
	if err != nil {
		t.Fatal(err)
	}
	claim := RecoveryClaim{Scope: cell.Scope{Namespace: "demo", Class: "__do__", ID: "w~C~shard0"}, Node: "old", Epoch: 1}
	v.claim = claim
	leases := NewOwnerLeases(t.Context(), nil)
	defer leases.Close()
	s := &Supervisor{Dir: p.root, Paging: p, OwnerLeases: leases}
	session := attachLifecycle(t, p, path)
	r := httptest.NewRequest("POST", "/internal/do-pages/checkpoint-begin?path="+url.QueryEscape(path)+"&activation="+session, nil)
	r.Header.Set("Authorization", "Bearer "+p.token)
	w := httptest.NewRecorder()
	s.PagingHandler().ServeHTTP(w, r)
	if w.Code != 412 {
		t.Fatalf("lost owner returned %d: %s", w.Code, w.Body.String())
	}
	if v.checkpoint.id != 0 || v.checkpoint.copying || v.activeWrites != 0 || v.preparations != 0 || v.writes != 0 {
		t.Fatal("rejected preparation mutated state")
	}
	requireLifecycle(t, p, "detach", path, session, "", 204)
}
