package config

import "testing"

func TestExplicitScheduleMode(t *testing.T) {
	for _, tc := range []struct {
		mode, url, region string
		ok                bool
	}{{"", "", "", true}, {"cluster", "", "", true}, {"local", "", "", false}, {"shared", "", "sg", false}, {"shared", "redis://localhost", "", false}, {"shared", "redis://localhost", "sg", true}, {"auto", "", "", false}} {
		t.Setenv("CELLHIVE_SCHEDULE_MODE", tc.mode)
		t.Setenv("CELLHIVE_SCHEDULE_REDIS_URL", tc.url)
		t.Setenv("CELLHIVE_SCHEDULE_REGION", tc.region)
		cfg := FromEnv()
		cfg.AllowInsecureDefaults = true
		if tc.mode == "" && cfg.ScheduleMode != "cluster" {
			t.Fatal(cfg.ScheduleMode)
		}
		if err := cfg.Validate(); (err == nil) != tc.ok {
			t.Fatalf("%s: %v", tc.mode, err)
		}
	}
}
