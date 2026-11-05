package dispatch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"cellhive/internal/control"
)

// A workflow instance with a captured target must not look up the mutable
// active definition on every run.
func TestWorkflowRunPinnedTargetSkipsActiveLookup(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	activeCalls, pinnedCalls := 0, 0
	d := &WorkflowDispatcher{
		URL: srv.URL, Token: jwtTestKey,
		Params: func(context.Context, string, string, string) ([]byte, error) { return nil, nil },
		Target: func(context.Context, string, string) (control.WorkflowTarget, bool) {
			activeCalls++
			return control.WorkflowTarget{Worker: "active", BundleSHA: "active-sha", Version: 1}, true
		},
		TargetForInstance: func(context.Context, string, string, string) (control.WorkflowTarget, bool, error) {
			pinnedCalls++
			return control.WorkflowTarget{Worker: "pinned", BundleSHA: "pinned-sha", Version: 9}, true, nil
		},
	}
	if err := d.Run(context.Background(), "acme", "wf", "i1", "tok"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if activeCalls != 0 {
		t.Fatalf("active target looked up %d times for a pinned instance", activeCalls)
	}
	if pinnedCalls != 1 {
		t.Fatalf("pinned target looked up %d times, want 1", pinnedCalls)
	}
}

func TestWorkflowRunUnpinnedFallsBackToActiveLookup(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	activeCalls, pinnedCalls := 0, 0
	d := &WorkflowDispatcher{
		URL: srv.URL, Token: jwtTestKey,
		Params: func(context.Context, string, string, string) ([]byte, error) { return nil, nil },
		Target: func(context.Context, string, string) (control.WorkflowTarget, bool) {
			activeCalls++
			return control.WorkflowTarget{Worker: "active", BundleSHA: "active-sha", Version: 2}, true
		},
		TargetForInstance: func(context.Context, string, string, string) (control.WorkflowTarget, bool, error) {
			pinnedCalls++
			return control.WorkflowTarget{}, false, nil
		},
	}
	if err := d.Run(context.Background(), "acme", "wf", "legacy", "tok"); err != nil {
		t.Fatalf("run: %v", err)
	}
	if pinnedCalls != 1 || activeCalls != 1 {
		t.Fatalf("pinned=%d active=%d, want 1/1", pinnedCalls, activeCalls)
	}
}
