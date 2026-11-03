package config

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"testing"

	"cellhive/internal/bearertoken"
)

func TestJWTSigningKeyDomains(t *testing.T) {
	const root = "00112233445566778899aabbccddeeff"
	c := Config{RootKey: root}
	raw, _ := hex.DecodeString(root)
	creds := DeriveCredentials(root)
	for role, encoded := range map[string]string{
		"internal": creds.Internal, "peer": creds.Peer, "dispatch": creds.Dispatch,
		"log": creds.Log, "admin": creds.Admin, "do-ticket": creds.DoTicket,
		"scope": creds.Binding,
	} {
		want, err := bearertoken.DeriveKey(raw, "token/"+role)
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.JWTKey("token/" + role)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s JWT key mismatch: %v", role, err)
		}
		if encoded != base64.RawURLEncoding.EncodeToString(want) {
			t.Fatalf("%s credential uses wrong derivation domain", role)
		}
	}
	if creds.Binding == creds.Scope {
		t.Fatal("JWT binding key must not reuse persisted legacy scope secret")
	}
	if _, err := (Config{RootKey: "bad"}).JWTKey("token/admin"); err == nil {
		t.Fatal("invalid root accepted")
	}
	if _, err := c.JWTKey("token/unknown"); err == nil {
		t.Fatal("unknown key purpose accepted")
	}
}

func TestJWTConfigDoesNotUseStaticAdminOverride(t *testing.T) {
	t.Setenv("CELLHIVE_ROOT_KEY", "00112233445566778899aabbccddeeff")
	t.Setenv("CELLHIVE_ADMIN_TOKEN", "obsolete-static-token")
	c := FromEnv()
	if c.AdminToken == "obsolete-static-token" {
		t.Fatal("legacy admin token replaced JWT signing key")
	}
	if c.BindingSigningKey != DeriveCredentials(c.RootKey).Binding {
		t.Fatal("binding JWT key not wired")
	}
}
