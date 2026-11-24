package config

import (
	"strings"
	"testing"
)

func TestCoordinationValidation(t *testing.T) {
	for _, tc := range []struct {
		backend, url, region string
		bad                  bool
	}{{"object", "", "", false}, {"redis", "", "sg", true}, {"redis", "redis://localhost", "", true}, {"redis", "redis://localhost", "sg", false}, {"redis", "redis://localhost", "{sg}", true}, {"auto", "", "", true}} {
		c := FromEnv()
		c.CoordinationBackend = tc.backend
		c.CoordinationRedisURL = tc.url
		c.CoordinationRegion = tc.region
		err := c.Validate()
		bad := err != nil && (strings.Contains(err.Error(), "coordination") || strings.Contains(err.Error(), "COORDINATION"))
		if bad != tc.bad {
			t.Fatalf("%+v: %v", tc, err)
		}
	}
}
