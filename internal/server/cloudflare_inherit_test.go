package server

import (
	"cellhive/internal/control"
	"net/http"
	"testing"
)

func TestCloudflareUploadInheritsActiveBinding(t *testing.T) {
	s := newControlServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.CreateResource(ctx, "acme", "queue", "jobs", "acme/__queue__/jobs", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{BundleSHA: "old", Bindings: []control.Binding{{Type: "queue", Name: "JOBS", ID: "jobs"}}}, "ops"); err != nil {
		t.Fatal(err)
	}
	rr := reviewCFUpload(t, s, `{"main_module":"worker.js","bindings":[{"type":"inherit","name":"JOBS"}]}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("upload %d: %s", rr.Code, rr.Body.String())
	}
	v, ok, err := s.Control.VersionEnv(ctx, "acme", "api", 2)
	if err != nil || !ok || len(v.Bindings) != 1 || v.Bindings[0].Type != "queue" || v.Bindings[0].ID != "jobs" {
		t.Fatalf("inherited version: %+v %v", v, err)
	}
	active, _, _ := s.Control.WorkerEnv(ctx, "acme", "api")
	if active.Version != 1 {
		t.Fatal("upload changed active")
	}
	rr = reviewCFUpload(t, s, `{"main_module":"worker.js","bindings":[{"type":"inherit","name":"MISSING"}]}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing inherited binding: %d %s", rr.Code, rr.Body.String())
	}
}
