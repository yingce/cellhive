package config

import (
	"testing"
)

func TestAuthorityModes(t *testing.T) {
	for _, tc := range []struct {
		mode, bucket, ack string
		ok                bool
	}{{"", "", "", true}, {"legacy", "cos://test", "", true}, {"validate", "cos://test", "", true}, {"validate", "oss://test", "", true}, {"validate", "s3://test", "", false}, {"reclaim", "cos://test", "", false}, {"reclaim", "cos://test", "all-readers-validated", true}, {"oops", "cos://test", "", false}} {
		t.Run(tc.mode+tc.bucket+tc.ack, func(t *testing.T) {
			t.Setenv("CELLHIVE_AUTHORITY_MODE", tc.mode)
			t.Setenv("CELLHIVE_AUTHORITY_RECLAIM_ACK", tc.ack)
			t.Setenv("CELLHIVE_BUCKET", tc.bucket)
			c := FromEnv()
			c.AllowInsecureDefaults = true
			c.NodeID = "test"
			if c.AuthorityMode != tc.mode {
				t.Fatal("mode not loaded")
			}
			err := c.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("valid=%v err=%v", tc.ok, err)
			}
		})
	}
}
