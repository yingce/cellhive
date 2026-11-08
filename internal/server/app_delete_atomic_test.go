package server

import (
	"context"
	"net/http"
	"testing"

	"cellhive/internal/control"
)

// App deletion must enqueue the resumable purge job and return without walking
// the cells prefix in the request. A canceled request or 502 must not leave a
// half-deleted cell (ADR-309).
func TestAppDeleteDoesNotSynchronouslyWalkCells(t *testing.T) {
	s, _ := newTestServer(t)
	s.Control = control.New(s.Store, nil)
	ctx := context.Background()
	key := "cells/acme/__kv__/main/ltx/e1/1.snapshot"
	if _, err := s.Bucket.Put(ctx, key, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	rr := do(t, s.AdminHandler(), http.MethodDelete, "/v1/control/app?namespace=acme", testRoleJWT(t, "admin"), nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("app delete status = %d: %s", rr.Code, rr.Body.String())
	}
	if _, _, err := s.Bucket.Get(ctx, key); err != nil {
		t.Fatalf("request synchronously deleted the cell payload: %v", err)
	}
}
