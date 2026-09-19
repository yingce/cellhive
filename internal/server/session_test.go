package server

import "testing"

// TestDeployRestartPolicy covers the DO restart decision (ADR-107).
func TestDeployRestartPolicy(t *testing.T) {
	cases := []struct {
		eager  bool
		policy string
		want   bool
	}{
		{false, "", false}, // default: lazy
		{true, "", true},   // configured eager
		{false, "restart", true},
		{true, "preserve", false},
		{true, "other", true}, // unknown -> default
	}
	for _, c := range cases {
		if got := deployRestartPolicy(c.eager, c.policy); got != c.want {
			t.Fatalf("deployRestartPolicy(%v,%q) = %v, want %v", c.eager, c.policy, got, c.want)
		}
	}
}
