package config

import (
	"os"
	"strings"
	"testing"
)

func clearCoordinationEnv(t *testing.T) {
	t.Helper()
	for _, key := range append([]string{"CELLHIVE_COORDINATION_BACKEND"}, removedCoordinationEnv...) {
		old, exists := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if exists {
				_ = os.Setenv(key, old)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
}

func TestSingleCoordinationSetting(t *testing.T) {
	clearCoordinationEnv(t)
	for _, tc := range []struct {
		value        string
		redis, valid bool
	}{
		{"", false, true}, {"object", false, true}, {"cos", false, true},
		{"redis://localhost:6379/2", true, true}, {"rediss://user:secret@localhost:6380/0", true, true},
		{"redis", false, false}, {"redis://", false, false}, {"http://localhost", false, false},
		{"redsi://user:secret@host", false, false}, {"redis://user:secret@host/no-db", false, false},
		{"redis://host/0?unknown=secret", false, false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("CELLHIVE_COORDINATION_BACKEND", tc.value)
			c := FromEnv()
			c.AllowInsecureDefaults = true
			err := c.Validate()
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			if err != nil {
				if strings.Contains(err.Error(), "secret") {
					t.Fatal("credentials leaked")
				}
				return
			}
			if tc.redis {
				if c.CoordinationBackend != "redis" || c.ScheduleMode != "shared" || c.CoordinationRedisURL != tc.value || c.ScheduleRedisURL != tc.value || c.CoordinationRegion == "" || c.ScheduleRegion != c.CoordinationRegion {
					t.Fatal("unified Redis settings not derived")
				}
			} else if c.CoordinationBackend != "object" || c.ScheduleMode != "cluster" || c.ScheduleRedisURL != "" {
				t.Fatal("object settings not selected")
			}
		})
	}
}

func TestRemovedCoordinationSettingsFailClosed(t *testing.T) {
	clearCoordinationEnv(t)
	for _, key := range removedCoordinationEnv {
		t.Run(key, func(t *testing.T) {
			t.Setenv("CELLHIVE_COORDINATION_BACKEND", "redis://localhost:6379/0")
			t.Setenv(key, "")
			c := FromEnv()
			c.AllowInsecureDefaults = true
			if err := c.Validate(); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatal("old setting silently ignored", err)
			}
		})
	}
}

func TestDerivedCoordinationNamespace(t *testing.T) {
	a := CoordinationNamespace("cos://bucket-one", "/node-a", "")
	if a != CoordinationNamespace("cos://bucket-one", "/node-b", "") {
		t.Fatal("node directory changed remote bucket namespace")
	}
	if a == CoordinationNamespace("cos://bucket-two", "/node-a", "") {
		t.Fatal("different buckets share namespace")
	}
	if CoordinationNamespace("s3://bucket", "", "https://one") == CoordinationNamespace("s3://bucket", "", "https://two") {
		t.Fatal("different endpoints share namespace")
	}
	if CoordinationNamespace("", "/tmp/shared/../bucket", "") != CoordinationNamespace("", "/tmp/bucket", "") {
		t.Fatal("FS namespace not canonical")
	}
	if CoordinationNamespace("", "/tmp/bucket", "") == CoordinationNamespace("", "/tmp/other", "") {
		t.Fatal("different FS buckets share namespace")
	}
}
