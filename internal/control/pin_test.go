package control

import (
	"context"
	"testing"
)

// TestDeployPinsServiceBindingVersion covers ADR-104: a service binding records
// the target's active version at deploy time and keeps it across target
// redeploys; a new deploy of the caller re-pins to the then-active version.
func TestDeployPinsServiceBindingVersion(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, false)
	if _, err := s.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := s.Deploy(ctx, "acme", "b", DeploySpec{BundleSHA: "shaB1"}, "test"); err != nil {
		t.Fatalf("deploy b v1: %v", err)
	}
	va, err := s.Deploy(ctx, "acme", "a", DeploySpec{
		BundleSHA: "shaA1",
		Bindings:  []Binding{{Type: "service", Name: "SVC", ID: "b", Entrypoint: "Api"}},
	}, "test")
	if err != nil {
		t.Fatalf("deploy a: %v", err)
	}
	if len(va.Bindings) != 1 || va.Bindings[0].Version != "shaB1" {
		t.Fatalf("binding version = %+v, want shaB1", va.Bindings)
	}

	// Redeploying the target does not change the caller's pinned version.
	if _, err := s.Deploy(ctx, "acme", "b", DeploySpec{BundleSHA: "shaB2"}, "test"); err != nil {
		t.Fatalf("deploy b v2: %v", err)
	}
	proj, err := s.Projection(ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	pa, ok := proj.WorkerFor("acme", "a")
	if !ok || len(pa.Version.Bindings) != 1 {
		t.Fatalf("projection a = %+v ok=%v", pa, ok)
	}
	if got := pa.Version.Bindings[0].Version; got != "shaB1" {
		t.Fatalf("pinned version after target redeploy = %q, want shaB1", got)
	}

	// A new deploy of the caller re-pins to the now-active target version.
	va2, err := s.Deploy(ctx, "acme", "a", DeploySpec{
		BundleSHA: "shaA2",
		Bindings:  []Binding{{Type: "service", Name: "SVC", ID: "b"}},
	}, "test")
	if err != nil {
		t.Fatalf("deploy a v2: %v", err)
	}
	if got := va2.Bindings[0].Version; got != "shaB2" {
		t.Fatalf("re-pinned version = %q, want shaB2", got)
	}
}
