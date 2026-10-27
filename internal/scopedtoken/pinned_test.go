package scopedtoken

import (
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
