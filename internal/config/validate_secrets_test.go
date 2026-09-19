package config

import (
	"testing"
	"time"
)

// TestValidateRootKey builds the production posture: one real root key derives
// every role credential. Missing/invalid/dev roots must be rejected (ADR-137).
func TestValidateRootKey(t *testing.T) {
	base := Config{
		NodeID: "n1", BucketDir: t.TempDir(), LeaseTTL: time.Minute, Durability: "bucket",
	}
	if err := base.Validate(); err == nil {
		t.Fatal("missing root key was accepted")
	}

	base.AllowInsecureDefaults = true
	base.RootKey = DevRootKey
	if err := base.Validate(); err != nil {
		t.Fatalf("explicit dev opt-out rejected: %v", err)
	}

	base.AllowInsecureDefaults = false
	if err := base.Validate(); err == nil {
		t.Fatal("dev root key was accepted without the opt-out")
	}

	const root = "00112233445566778899aabbccddeeff"
	c := DeriveCredentials(root)
	base.RootKey = root
	base.TokenPeer, base.TokenInternal, base.TokenDispatch = c.Peer, c.Internal, c.Dispatch
	base.TokenLog, base.AdminToken, base.ScopeSecret = c.Log, c.Admin, c.Scope
	if err := base.Validate(); err != nil {
		t.Fatalf("real root key rejected: %v", err)
	}

	// An undecodable root derives nothing, which Validate must catch.
	bad := Config{
		NodeID: "n1", BucketDir: t.TempDir(), LeaseTTL: time.Minute, Durability: "bucket",
		RootKey: "not-a-valid-key",
	}
	if err := bad.Validate(); err == nil {
		t.Fatal("undecodable root key was accepted")
	}
}
