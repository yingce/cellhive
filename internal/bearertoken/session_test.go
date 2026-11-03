package bearertoken

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func sessionClaims(typ string) Claims {
	c := baseClaims()
	c.Type = typ
	c.Namespace = "app"
	c.Worker = "worker"
	c.Session = "session-id"
	c.ParentIssuer = "token/admin"
	return c
}

func TestSessions(t *testing.T) {
	for _, typ := range []string{"assets", "tail"} {
		c := sessionClaims(typ)
		if typ == "assets" {
			c.WranglerSingleAssetUploads = true
		} else {
			c.Search = "error"
		}
		for _, parent := range []string{"token/admin", strings.TrimPrefix(issuerID, "issuer/")} {
			c.ParentIssuer = parent
			token, err := Mint(testKey, "token/"+typ, c)
			if err != nil {
				t.Fatal(err)
			}
			_, got, err := Verify(testKey, token, testNow)
			if err != nil || !reflect.DeepEqual(got, c) {
				t.Fatal(got, err)
			}
		}
		for _, mutate := range []func(*Claims){func(c *Claims) { c.ParentIssuer = "" }, func(c *Claims) { c.ParentIssuer = issuerID }, func(c *Claims) { c.Namespace = "*" }, func(c *Claims) { c.Worker = "../worker" }, func(c *Claims) { c.Session = "" }, func(c *Claims) { c.Session = "a\\b" }, func(c *Claims) { c.Kind = "kv" }, func(c *Claims) { c.StorageClass = "class" }, func(c *Claims) { c.Search = strings.Repeat("x", 257) }} {
			bad := c
			mutate(&bad)
			if _, err := Mint(testKey, "token/"+typ, bad); err == nil {
				t.Fatalf("accepted %+v", bad)
			}
		}
	}
	c := sessionClaims("assets")
	c.Search = "error"
	if _, err := Mint(testKey, "token/assets", c); err == nil {
		t.Fatal("assets search")
	}
	c = sessionClaims("tail")
	c.WranglerSingleAssetUploads = true
	if _, err := Mint(testKey, "token/tail", c); err == nil {
		t.Fatal("tail upload flag")
	}
	c = baseClaims()
	c.ParentIssuer = "token/admin"
	if _, err := Mint(testKey, "token/admin", c); err == nil {
		t.Fatal("admin session field")
	}
}

func TestDOTicket(t *testing.T) {
	shard := 0
	c := baseClaims()
	c.Type = "internal"
	c.Namespace = "app"
	c.Worker = "worker"
	c.StorageClass = "class"
	c.Shard = &shard
	c.Execution = `{"class":"class","bundle_sha":"bundle","storage_id":"incarnation","version":1,"limits":null}`
	c.ExpiresAt = c.IssuedAt + 30
	token, err := Mint(testKey, "token/do-ticket", c)
	if err != nil {
		t.Fatal(err)
	}
	_, got, err := Verify(testKey, token, testNow)
	if err != nil || !reflect.DeepEqual(got, c) {
		t.Fatal(got, err)
	}
	for _, mutate := range []func(*Claims){func(c *Claims) { c.Shard = nil }, func(c *Claims) { n := -1; c.Shard = &n }, func(c *Claims) { c.StorageClass = "" }, func(c *Claims) { c.Namespace = "*" }, func(c *Claims) { c.Worker = "" }, func(c *Claims) { c.Session = "id" }} {
		bad := c
		mutate(&bad)
		if _, err := Mint(testKey, "token/do-ticket", bad); err == nil {
			t.Fatal(bad)
		}
	}
	if _, err := Mint(testKey, "token/internal", c); err == nil {
		t.Fatal("ticket fields on transport")
	}
	for _, execution := range []string{"", `{}`, `{"class":"class","bundle_sha":"bundle","version":1,"limits":null}`, `{"class":"class","bundle_sha":"bundle","storage_id":"incarnation","version":-1,"limits":null}`} {
		bad := c
		bad.Execution = execution
		if _, err := Mint(testKey, "token/do-ticket", bad); err == nil {
			t.Fatalf("accepted execution %s", execution)
		}
	}
}

func TestSessionFieldPresence(t *testing.T) {
	for _, extra := range []string{`"session":""`, `"parent_issuer":""`, `"search":""`, `"wrangler_single_asset_uploads":false`, `"storage_class":""`, `"shard":0`} {
		p := `{"ver":1,"type":"admin","sub":"a","aud":"cellhive","iat":1700000000,"exp":1700000600,` + extra + `}`
		var c Claims
		if json.Unmarshal([]byte(p), &c) == nil {
			t.Fatal("unexpected field accepted", extra)
		}
	}
}
