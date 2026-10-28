package scopedtoken

import (
	"errors"
	"testing"
	"time"
)

func TestPinnedClaims(t *testing.T) {
	secret := []byte("secret")
	base := Claims{Namespace: "acme", Kind: "kv", Name: "KV", Worker: "web", Version: 1, Resource: "data"}
	tok, err := Mint(secret, base)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Verify(secret, tok, time.Now())
	if err != nil || got != base {
		t.Fatalf("roundtrip: %+v %v", got, err)
	}
	for _, mutate := range []func(*Claims){func(c *Claims) { c.Iss = "tenant" }, func(c *Claims) { c.Version = 0 }, func(c *Claims) { c.Worker = "" }, func(c *Claims) { c.Kind = "d1" }, func(c *Claims) { c.Name = "*" }} {
		c := base
		mutate(&c)
		if _, err := Mint(secret, c); err == nil {
			t.Fatalf("accepted invalid fixed claims %+v", c)
		}
	}
}

// TestResourceRoutingHintClaims covers the platform-only resource routing hint:
// an ordinary platform KV token may carry Resource without worker/version, but a
// delegated issuer (ADR-302) and non-KV kinds must never carry it.
func TestResourceRoutingHintClaims(t *testing.T) {
	secret := []byte("secret")
	base := Claims{Namespace: "acme", Kind: "kv", Name: "KV", Resource: "data"}
	if _, err := Mint(secret, base); err != nil {
		t.Fatalf("platform resource hint rejected: %v", err)
	}
	for _, mutate := range []func(*Claims){
		func(c *Claims) { c.Iss = "tenant"; c.ExpiresMs = 1 },
		func(c *Claims) { c.Kind = "d1" },
		func(c *Claims) { c.Kind = "" },
		func(c *Claims) { c.Name = "*" },
		func(c *Claims) { c.Version = 1 },
	} {
		c := base
		mutate(&c)
		if _, err := Mint(secret, c); !errors.Is(err, ErrIncomplete) {
			t.Fatalf("accepted invalid resource claims %+v (err=%v)", c, err)
		}
	}
}
