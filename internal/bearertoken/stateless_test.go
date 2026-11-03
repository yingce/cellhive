package bearertoken

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestStatelessIssuerKeyIDs(t *testing.T) {
	for _, pattern := range []string{"vwork-*", "vwork-42", "*"} {
		t.Run(pattern, func(t *testing.T) {
			kid := "issuer/" + pattern
			key, err := DeriveKey(bytes.Repeat([]byte{9}, 32), kid)
			if err != nil {
				t.Fatal(err)
			}
			c := Claims{Ver: 1, Type: "namespace", Subject: "test", Audience: "cellhive", IssuedAt: 1700000000, ExpiresAt: 1700000060, Namespaces: []string{"vwork-42"}}
			token, err := Mint(key, kid, c)
			if err != nil {
				t.Fatal(err)
			}
			got, _, err := Verify(key, token, time.Unix(1700000001, 0))
			if err != nil || got != kid {
				t.Fatalf("verify kid=%s err=%v", got, err)
			}
		})
	}
	for _, pattern := range []string{"", "00112233445566778899aabbccddeeff", "00112233445566778899AABBCCDDEEFF", "vwork/*", "v*work", "__platform__", "../vwork", "vwork-**", "vwork,other", "vwork-*/x", "**", "vwork\\app", ".", "a..b"} {
		if _, err := DeriveKey(bytes.Repeat([]byte{9}, 32), "issuer/"+pattern); err == nil {
			t.Errorf("accepted issuer pattern %q", pattern)
		}
		c := baseClaims()
		c.Type = "namespace"
		c.Namespaces = []string{"vwork-42"}
		kid := "issuer/" + pattern
		if _, err := Mint(testKey, kid, c); err == nil {
			t.Errorf("Mint accepted %q", kid)
		}
		h, _ := json.Marshal(header{Alg: "HS256", Typ: "JWT", Kid: kid})
		p, _ := json.Marshal(c)
		token := signed(string(h), string(p))
		if _, err := PeekKeyID(token); err == nil {
			t.Errorf("Peek accepted %q", kid)
		}
		if _, _, err := Verify(testKey, token, testNow); err == nil {
			t.Errorf("Verify accepted signed %q", kid)
		}
	}
}

func TestStatelessIssuerKeyDomainSeparation(t *testing.T) {
	seen := map[string]string{}
	for _, kid := range []string{"issuer/*", "issuer/vwork-*", "issuer/vwork-42", "issuer/name", "token/admin", "token/internal", "token/scope", "token/peer", "token/dispatch", "token/log", "token/assets", "token/tail", "token/do-ticket"} {
		key, err := DeriveKey(testKey, kid)
		if err != nil {
			t.Fatal(kid, err)
		}
		if prior, ok := seen[string(key)]; ok {
			t.Fatalf("%s and %s share key", prior, kid)
		}
		seen[string(key)] = kid
		again, err := DeriveKey(testKey, kid)
		if err != nil || !bytes.Equal(key, again) {
			t.Fatal("non-deterministic derivation", kid, err)
		}
	}
}
