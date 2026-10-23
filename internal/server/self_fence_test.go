package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/lease"
	"cellhive/internal/owner"
)

func TestCapturedWriteSelfFencesWhenLeaseUnhealthy(t *testing.T) {
	s := &Server{Lease: &lease.Manager{TTL: time.Second}}
	called := false
	err := s.capturedWrite(context.Background(), cell.Scope{Namespace: "x", Class: "__kv__", ID: "y"}, func() error {
		called = true
		return nil
	})
	if !errors.Is(err, owner.ErrNotOwner) {
		t.Fatalf("capturedWrite err=%v, want ErrNotOwner", err)
	}
	if called {
		t.Fatal("write ran while lease was unhealthy")
	}
	if s.ackAllowed(time.Now()) {
		t.Fatal("unpublished lease considered healthy")
	}
}
