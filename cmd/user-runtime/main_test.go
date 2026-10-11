package main

import "testing"

func TestTenantLogCapture(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
		ok    bool
	}{
		{"", true, true},
		{"on", true, true},
		{"ON", true, true},
		{"off", false, true},
		{"false", false, false},
		{"typo", false, false},
	} {
		t.Setenv("CELLHIVE_TENANT_LOG_CAPTURE", tc.value)
		got, err := tenantLogCapture()
		if (err == nil) != tc.ok || got != tc.want {
			t.Errorf("tenantLogCapture(%q) = %v, %v; want %v, ok=%v", tc.value, got, err, tc.want, tc.ok)
		}
	}
}
