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

func TestScopedBusyReplicaRefusesTakeoverAndRetries(t *testing.T) {
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
	end := cs.BeginRequest(sc.String())
	defer func() {
		if end != nil {
			end()
		}
	}()
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
		if rr.Code != http.StatusServiceUnavailable {
			o, _, _ := mine.Resolve(ctx, sc)
			_, statErr := os.Stat(path)
			t.Fatalf("claim: HTTP=%d owner=%s/%s stale_file_exists=%v body=%s", rr.Code, o.Node, o.Session, statErr == nil, rr.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("workflow claim deadlocked inside its own request gate")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("busy replica must remain intact: %v", err)
	}
	o, _, err := mine.Resolve(ctx, sc)
	if err != nil || o.Node != "b" {
		t.Fatalf("ownership changed after rejected cleanup: %+v %v", o, err)
	}
	end()
	end = nil
	rr := httptest.NewRecorder()
	h(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("retry: %d %s", rr.Code, rr.Body.String())
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retry kept stale replica: %v", err)
	}
}
