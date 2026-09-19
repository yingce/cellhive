package main

import "testing"

// TestEnvTrue pins the CELLHIVE_DO_OBJECT_INDEX parsing to the same semantics as
// cell-agent's config.envBool (true/1), so the two sides cannot disagree
// (ADR-136).
func TestEnvTrue(t *testing.T) {
	for _, tc := range []struct {
		val  string
		want bool
	}{
		{"1", true}, {"true", true}, {"0", false}, {"false", false}, {"", false}, {"yes", false},
	} {
		t.Setenv("CELLHIVE_DO_OBJECT_INDEX", tc.val)
		if got := envTrue("CELLHIVE_DO_OBJECT_INDEX"); got != tc.want {
			t.Errorf("envTrue(%q) = %v, want %v", tc.val, got, tc.want)
		}
	}
}

// TestDoLeaseSeconds: CELLHIVE_DO_LEASE is a Go duration rendered as whole
// seconds for the host actor's DO_LEASE_S (ADR-137).
func TestDoLeaseSeconds(t *testing.T) {
	t.Setenv("CELLHIVE_DO_LEASE", "45s")
	if got := doLeaseSeconds(); got != "45" {
		t.Fatalf("doLeaseSeconds = %q, want 45", got)
	}
	t.Setenv("CELLHIVE_DO_LEASE", "")
	if got := doLeaseSeconds(); got != "30" {
		t.Fatalf("default doLeaseSeconds = %q, want 30", got)
	}
	t.Setenv("CELLHIVE_DO_LEASE", "100ms")
	if got := doLeaseSeconds(); got != "1" {
		t.Fatalf("sub-second lease = %q, want clamped 1", got)
	}
}

// TestRuntimeRoot: one scratch dir knob shared by every runtime (ADR-137).
func TestRuntimeRoot(t *testing.T) {
	t.Setenv("CELLHIVE_RUNTIME_DIR", "/var/tmp/cellhive")
	if got := runtimeRoot(); got != "/var/tmp/cellhive" {
		t.Fatalf("runtimeRoot = %q", got)
	}
}
