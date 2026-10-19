package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/owner"
	"cellhive/internal/scopedtoken"
)

// A scoped Workflow request must evict an expired peer's local copy before
// opening its in-flight gate. The claim inside the handler must not evict it
// again: Store.Forget would then wait for this request's own gate forever.
func TestWorkflowScopeClaimDoesNotForgetInsideRequest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := cellstore.New(filepath.Join(dir, "cells"))
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	sc := cell.Scope{Namespace: "acme", Class: "__workflow__", ID: "FLOW"}
	c, err := cs.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "stale", []byte("peer-copy"), nil); err != nil {
		t.Fatal(err)
	}
	path, err := cs.Path(sc)
	if err != nil {
		t.Fatal(err)
	}
	peer := &owner.Manager{B: b, NodeID: "b", Session: "sb", Advertise: "b:7001", OwnerTTL: time.Minute}
	if _, err := peer.Claim(ctx, sc, time.Now().Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	mine := &owner.Manager{B: b, NodeID: "a", Session: "sa", Advertise: "a:7001", OwnerTTL: time.Minute}
	srv := New(Deps{Cfg: config.Config{NodeID: "a", ScopeSecret: "scope-secret"}, Bucket: b, Store: cs, Owner: mine})
	tok, err := scopedtoken.Mint([]byte(srv.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "workflow", Name: "FLOW"})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.scopeAuth("workflow")(func(w http.ResponseWriter, r *http.Request) {
		if srv.forwardOrClaim(w, r, sc, nil) {
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/workflow/create?ns=acme&workflow=FLOW&id=i1", nil)
	req.Header.Set("x-cellhive-scope-token", tok)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { rr := httptest.NewRecorder(); h(rr, req); done <- rr }()
	select {
	case rr := <-done:
		if rr.Code != http.StatusOK {
			t.Fatalf("claim: %d %s", rr.Code, rr.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("workflow claim deadlocked inside its own request gate")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale local copy remains: %v", err)
	}
	o, _, err := mine.Resolve(ctx, sc)
	if err != nil || o.Node != "a" {
		t.Fatalf("owner after claim: %+v, %v", o, err)
	}

	// Internal Workflow step/wait routes use pinNSMW rather than scopeAuth.
	internal := cell.Scope{Namespace: "acme", Class: "__workflow__", ID: "INTERNAL"}
	stale, err := cs.Cell(ctx, internal)
	if err != nil {
		t.Fatal(err)
	}
	if err := stale.Put(ctx, "stale", []byte("peer-copy"), nil); err != nil {
		t.Fatal(err)
	}
	internalPath, err := cs.Path(internal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Claim(ctx, internal, time.Now().Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	internalHandler := srv.pinNSMW(func(w http.ResponseWriter, r *http.Request) {
		if srv.forwardOrClaim(w, r, internal, nil) {
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	internalReq := httptest.NewRequest(http.MethodPut, "/v1/internal/workflow/step?ns=acme&workflow=INTERNAL", nil)
	internalDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { rr := httptest.NewRecorder(); internalHandler(rr, internalReq); internalDone <- rr }()
	select {
	case rr := <-internalDone:
		if rr.Code != http.StatusOK {
			t.Fatalf("internal claim: %d %s", rr.Code, rr.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("internal workflow claim deadlocked inside its own request gate")
	}
	if _, err := os.Stat(internalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("internal stale copy remains: %v", err)
	}
}

// TestInvalidateStaleLocal covers celld's took_over rule: a local cell file is
// dropped when the scope is owned by a different (expired) node or by nobody,
// but kept for a scope this node owns (a clean same-node reload).
func TestInvalidateStaleLocal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	cs, err := cellstore.New(filepath.Join(dir, "cells"))
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	defer cs.Close()
	omA := &owner.Manager{B: b, NodeID: "a", Session: "sa", Advertise: "a:7001", OwnerTTL: time.Minute}
	srv := New(Deps{Cfg: config.Config{NodeID: "a", ScopeSecret: "s"}, Bucket: b, Store: cs, Owner: omA})

	seed := func(sc cell.Scope) string {
		t.Helper()
		c, cerr := cs.Cell(ctx, sc)
		if cerr != nil {
			t.Fatalf("cell %s: %v", sc, cerr)
		}
		if err := c.Put(ctx, "k", []byte("v"), nil); err != nil {
			t.Fatalf("put %s: %v", sc, err)
		}
		p, perr := cs.Path(sc)
		if perr != nil {
			t.Fatalf("path %s: %v", sc, perr)
		}
		if _, serr := os.Stat(p); serr != nil {
			t.Fatalf("seed %s: %v", sc, serr)
		}
		return p
	}
	gone := func(p string) bool {
		_, err := os.Stat(p)
		return errors.Is(err, os.ErrNotExist)
	}

	// A different node held the scope but its lease expired: the local copy is a
	// stale foreign lineage and must be dropped.
	foreign := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "foreign"}
	pForeign := seed(foreign)
	omB := &owner.Manager{B: b, NodeID: "b", Session: "sb", Advertise: "b:7001", OwnerTTL: time.Minute}
	if _, err := omB.Claim(ctx, foreign, time.Now().Add(-2*time.Minute)); err != nil {
		t.Fatalf("claim b: %v", err)
	}
	srv.invalidateStaleLocal(ctx, foreign)
	if !gone(pForeign) {
		t.Fatalf("expired foreign scope: local file not dropped")
	}

	// Nobody owns the scope: the local copy cannot be proven current.
	unowned := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "unowned"}
	pUnowned := seed(unowned)
	srv.invalidateStaleLocal(ctx, unowned)
	if !gone(pUnowned) {
		t.Fatalf("unowned scope: local file not dropped")
	}

	// This node owns the scope: keep the local file (clean reload).
	owned := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "owned"}
	pOwned := seed(owned)
	if _, err := omA.Claim(ctx, owned, time.Now()); err != nil {
		t.Fatalf("claim a: %v", err)
	}
	srv.invalidateStaleLocal(ctx, owned)
	if _, err := os.Stat(pOwned); err != nil {
		t.Fatalf("owned scope: local file dropped: %v", err)
	}
}
