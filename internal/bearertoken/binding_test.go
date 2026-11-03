package bearertoken

import (
	"encoding/json"
	"testing"
)

func TestBindingRequiresConcreteKindAndName(t *testing.T) {
	for _, kid := range []string{"token/scope", issuerID} {
		for _, field := range []string{"kind", "name"} {
			for _, value := range []string{"", "*", "cache*", "kv,r2", "a/b", "a\\b", ".", "..", "a..b", "a\x00b", "a\nb", "a\u007fb", "a\u0085b"} {
				t.Run(kid+"/"+field+"/"+value, func(t *testing.T) {
					c := baseClaims()
					c.Type = "binding"
					c.Namespace = "app"
					c.Kind = "kv"
					c.Name = "CACHE"
					if field == "kind" {
						c.Kind = value
					} else {
						c.Name = value
					}
					if _, err := Mint(testKey, kid, c); err == nil {
						t.Errorf("Mint accepted non-concrete %s=%q", field, value)
					}
					// Sign raw JSON independently of Mint so Verify must enforce shape too.
					payload := map[string]any{"ver": 1, "type": "binding", "sub": "test", "aud": "cellhive", "iat": testNow.Unix(), "exp": testNow.Unix() + 60, "ns": "app", "kind": c.Kind, "name": c.Name}
					h, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT", "kid": kid})
					p, _ := json.Marshal(payload)
					if _, _, err := Verify(testKey, signed(string(h), string(p)), testNow); err == nil {
						t.Errorf("Verify accepted signed non-concrete %s=%q", field, value)
					}
				})
			}
		}
	}
}

func TestBindingPreservesExactResourceIdentifiers(t *testing.T) {
	for _, kid := range []string{"token/scope", issuerID} {
		for _, name := range []string{"CACHE-v2", "cache.name_1", "name:version", "name with spaces", "\u8d44\u6e90"} {
			c := baseClaims()
			c.Type = "binding"
			c.Namespace = "app"
			c.Kind = "custom-kind"
			c.Name = name
			token, err := Mint(testKey, kid, c)
			if err != nil {
				t.Fatal(kid, name, err)
			}
			_, got, err := Verify(testKey, token, testNow)
			if err != nil || got.Name != name || got.Kind != c.Kind {
				t.Fatal(got, err)
			}
		}
	}
	c := baseClaims()
	c.Type = "binding"
	c.Namespace = "app"
	c.Kind = "kv"
	c.Name = "CACHE-v2"
	c.Worker = "worker-v2"
	c.Version = 2
	c.Resource = "resource-v2"
	if _, err := Mint(testKey, "token/scope", c); err != nil {
		t.Fatal("valid pinned KV rejected", err)
	}
	c = baseClaims()
	c.Type = "namespace"
	c.Namespaces = []string{"*"}
	if _, err := Mint(testKey, issuerID, c); err != nil {
		t.Fatal("namespace all-business grant rejected", err)
	}
}
