package bearertoken

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

var testKey = []byte("0123456789abcdef0123456789abcdef")
var testNow = time.Unix(1700000000, 0)

const issuerID = "issuer/0123456789abcdef0123456789abcdef"

func baseClaims() Claims {
	return Claims{Ver: 1, Type: "admin", Subject: "bootstrap", Audience: "cellhive", IssuedAt: 1700000000, ExpiresAt: 1700000600}
}
func signed(h, p string) string {
	s := base64.RawURLEncoding.EncodeToString([]byte(h)) + "." + base64.RawURLEncoding.EncodeToString([]byte(p))
	m := hmac.New(sha256.New, testKey)
	m.Write([]byte(s))
	return s + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func TestGolden(t *testing.T) {
	// Independently computed with Node crypto.createHmac and crypto.hkdfSync.
	const golden = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRva2VuL2FkbWluIn0.eyJ2ZXIiOjEsInR5cGUiOiJhZG1pbiIsInN1YiI6ImJvb3RzdHJhcCIsImF1ZCI6ImNlbGxoaXZlIiwiaWF0IjoxNzAwMDAwMDAwLCJleHAiOjE3MDAwMDA2MDB9.CQHugbHG_pWab_r5JBXWGBTOjoSYfWd45hdk1HSDOnM"
	token, err := Mint(testKey, "token/admin", baseClaims())
	if err != nil || token != golden {
		t.Fatalf("mint %s: %v", token, err)
	}
	kid, c, err := Verify(testKey, golden, testNow)
	if err != nil || kid != "token/admin" || !reflect.DeepEqual(c, baseClaims()) {
		t.Fatalf("verify %s %+v %v", kid, c, err)
	}
	if kid, err := PeekKeyID(golden); err != nil || kid != "token/admin" {
		t.Fatal(kid, err)
	}
	k, err := DeriveKey(testKey, "token/admin")
	if err != nil || hex.EncodeToString(k) != "543ec4212e9acc5f0bb34150e0f4093871599b8b1ca3c220c174ef0fc62ae8e4" {
		t.Fatalf("derive %x %v", k, err)
	}
}

func TestClaimsAndKeyDomains(t *testing.T) {
	for _, tt := range []struct{ kid, typ string }{{issuerID, "namespace"}, {issuerID, "binding"}, {"token/scope", "binding"}, {"token/admin", "admin"}, {"token/internal", "internal"}, {"token/peer", "internal"}, {"token/dispatch", "internal"}, {"token/log", "internal"}, {"token/do-ticket", "internal"}, {"token/assets", "assets"}, {"token/tail", "tail"}} {
		c := baseClaims()
		c.Type = tt.typ
		if c.Type == "assets" || c.Type == "tail" {
			c = sessionClaims(c.Type)
		}
		if tt.kid == "token/do-ticket" {
			n := 0
			c.Namespace = "app"
			c.Worker = "worker"
			c.StorageClass = "class"
			c.Execution = `{"class":"class","bundle_sha":"bundle","storage_id":"incarnation","version":1,"limits":null}`
			c.ExpiresAt = c.IssuedAt + 30
			c.Shard = &n
		}
		if c.Type == "namespace" {
			c.Namespaces = []string{"vwork-*"}
		}
		if c.Type == "binding" {
			c.Namespace = "vwork-app"
			c.Kind = "kv"
			c.Name = "CACHE"
		}
		token, err := Mint(testKey, tt.kid, c)
		if err != nil {
			t.Fatal(tt, err)
		}
		_, got, err := Verify(testKey, token, testNow)
		if err != nil || !reflect.DeepEqual(c, got) {
			t.Fatal(tt, got, err)
		}
	}
	for _, mutate := range []func(*Claims){func(c *Claims) { c.Ver = 0 }, func(c *Claims) { c.Subject = "" }, func(c *Claims) { c.Audience = "other" }, func(c *Claims) { c.ExpiresAt = c.IssuedAt }, func(c *Claims) { c.Namespaces = []string{"*"} }, func(c *Claims) { c.Kind = "kv" }, func(c *Claims) { c.Type = "unknown" }} {
		c := baseClaims()
		mutate(&c)
		if _, err := Mint(testKey, "token/admin", c); err == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
	for _, kid := range []string{"token/scope", issuerID, "token/peer", "token/assets", "token/tail"} {
		if _, err := Mint(testKey, kid, baseClaims()); err == nil {
			t.Fatal("domain confusion", kid)
		}
	}
	c := baseClaims()
	c.Type = "binding"
	c.Namespace = "app"
	c.Kind = "kv"
	c.Name = "CACHE"
	c.Worker = "worker"
	c.Version = 2
	c.Resource = "resource"
	if _, err := Mint(testKey, "token/scope", c); err != nil {
		t.Fatal(err)
	}
	if _, err := Mint(testKey, issuerID, c); err == nil {
		t.Fatal("issuer pinned binding")
	}
	c.Namespace = "app*"
	if _, err := Mint(testKey, "token/scope", c); err == nil {
		t.Fatal("wildcard binding")
	}
	c.Namespace = "app"
	c.Resource = ""
	if _, err := Mint(testKey, "token/scope", c); err == nil {
		t.Fatal("partial pin")
	}
	for _, kid := range []string{"", "token/../admin", "token/unknown", "issuer/name", "issuer/0123456789ABCDEF0123456789abcdef", issuerID + "/x"} {
		if _, err := DeriveKey(testKey, kid); err == nil {
			t.Fatal(kid)
		}
	}
}

func TestRejectMalformedJWT(t *testing.T) {
	h := `{"alg":"HS256","typ":"JWT","kid":"token/admin"}`
	p := `{"ver":1,"type":"admin","sub":"bootstrap","aud":"cellhive","iat":1700000000,"exp":1700000600}`
	for _, bad := range []string{strings.Replace(h, "HS256", "none", 1), strings.Replace(h, "JWT", "jwt", 1), strings.Replace(h, `"alg":"HS256"`, `"alg":"HS256","alg":"HS256"`, 1), strings.Replace(h, `"kid"`, `"KID"`, 1), strings.Replace(h, `"typ":"JWT"`, `"typ":null`, 1), h + `{}`, `[]`} {
		tok := signed(bad, p)
		if _, _, err := Verify(testKey, tok, testNow); err == nil {
			t.Fatal(bad)
		}
		if _, err := PeekKeyID(tok); err == nil {
			t.Fatal("peek", bad)
		}
	}
	for _, bad := range []string{strings.Replace(p, `"exp":1700000600`, `"exp":1700000600,"exp":1700000600`, 1), strings.Replace(p, `"exp":1700000600`, `"exp":"1700000600"`, 1), strings.Replace(p, `"exp":1700000600`, `"exp":null`, 1), strings.Replace(p, `,"exp":1700000600`, "", 1), strings.Replace(p, `,"iat":1700000000`, "", 1), strings.Replace(p, `1700000600`, `1700000600.0`, 1), strings.Replace(p, `"ver":1`, `"Ver":1`, 1), strings.Replace(p, `"ver":1`, `"ver":1,"actions":[]`, 1), strings.Replace(p, `"ver":1`, `"ver":1,"ns":[]`, 1), p + `{}`, `null`, `{"ver":`} {
		if _, _, err := Verify(testKey, signed(h, bad), testNow); err == nil {
			t.Fatal(bad)
		}
	}
	valid := signed(h, p)
	for _, tok := range []string{"", valid + ".x", strings.Repeat("a", 16385), valid[:len(valid)-1] + "X"} {
		if _, _, err := Verify(testKey, tok, testNow); err == nil {
			t.Fatal("accepted malformed token")
		}
	}
	if _, _, err := Verify([]byte("wrong"), valid, testNow); err == nil {
		t.Fatal("wrong key")
	}
	for _, sec := range []int64{1700000600, 1700000700, 1699999969} {
		if _, _, err := Verify(testKey, valid, time.Unix(sec, 0)); err == nil {
			t.Fatal(sec)
		}
	}
	if _, _, err := Verify(testKey, valid, time.Unix(1699999970, 0)); err != nil {
		t.Fatal(err)
	}
}

func TestStrictNamespaceJSON(t *testing.T) {
	for _, s := range []string{`{"type":"namespace","ns":"app"}`, `{"type":"binding","ns":["app"]}`, `{"type":"namespace","ns":[null]}`, `{"type":"namespace","ns":null}`, `{"type":"namespace","ns":[1]}`, `{"ns":["app"],"ns":["app"]}`} {
		var c Claims
		if json.Unmarshal([]byte(s), &c) == nil {
			t.Fatal(s)
		}
	}
	for _, fragment := range []string{`"type":"namespace","ns":"app"`, `"type":"binding","ns":["app"]`, `"type":"namespace","ns":[null]`, `"type":"namespace","ns":null`, `"type":"namespace","ns":[1]`} {
		data := `{"ver":1,"sub":"test","aud":"cellhive","iat":1700000000,"exp":1700000600,` + fragment + `}`
		var c Claims
		if err := json.Unmarshal([]byte(data), &c); err == nil {
			t.Fatal("accepted wrong ns shape", data)
		}
	}
}

func TestCrossClientBindingGolden(t *testing.T) {
	// Node crypto vector shared with the TypeScript/Rust issuer implementations.
	const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Imlzc3Vlci8wMDExMjIzMzQ0NTU2Njc3ODg5OWFhYmJjY2RkZWVmZiJ9.eyJ2ZXIiOjEsInR5cGUiOiJiaW5kaW5nIiwic3ViIjoiZ29sZGVuIiwiYXVkIjoiY2VsbGhpdmUiLCJpYXQiOjE3MDAwMDAwMDAsImV4cCI6MTcwMDAwMDA2MCwibnMiOiJ2d29yay00MiIsImtpbmQiOiJrdiIsIm5hbWUiOiJDQUNIRSJ9.u4LalcrCTsL_xfWoXcvxoWZYkpSdiCnILnT5QPUaWh8"
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	kid, c, err := Verify(key, token, testNow)
	want := Claims{Ver: 1, Type: "binding", Subject: "golden", Audience: "cellhive", IssuedAt: 1700000000, ExpiresAt: 1700000060, Namespace: "vwork-42", Kind: "kv", Name: "CACHE"}
	if err != nil || kid != "issuer/00112233445566778899aabbccddeeff" || !reflect.DeepEqual(c, want) {
		t.Fatalf("%s %+v %v", kid, c, err)
	}
}

func TestNamespacePatterns(t *testing.T) {
	for _, s := range []string{"app", "vwork-*", "*", "a_b.c-1", "a*"} {
		if err := ValidatePattern(s); err != nil {
			t.Fatal(s, err)
		}
	}
	for _, s := range []string{"", "a**", "*a", "a*b", "__sys", "__*", "a..b", "a/b", "a b", "é", "a\n"} {
		if ValidatePattern(s) == nil {
			t.Fatal(s)
		}
	}
	for _, tt := range []struct {
		patterns []string
		ns       string
		want     bool
	}{{[]string{"vwork-*"}, "vwork-app", true}, {[]string{"vwork-*"}, "vwork", false}, {[]string{"*"}, "__sys", false}, {[]string{"*"}, "a*", false}, {[]string{"app"}, "apple", false}} {
		if got := Allows(tt.patterns, tt.ns); got != tt.want {
			t.Fatal(tt, got)
		}
	}
	for _, tt := range []struct {
		a, b []string
		want bool
	}{{[]string{"vwork-*"}, []string{"vwork-app", "vwork-a*"}, true}, {[]string{"vwork-*"}, []string{"*"}, false}, {[]string{"app"}, []string{"app*"}, false}, {[]string{"app*"}, []string{"ap*"}, false}, {[]string{"*"}, []string{"app*"}, true}, {[]string{"app*"}, []string{"app*"}, true}, {[]string{"*"}, []string{"__sys"}, false}} {
		if got := Contains(tt.a, tt.b); got != tt.want {
			t.Fatal(tt, got)
		}
	}
}

func TestBearer(t *testing.T) {
	for _, s := range []string{"Bearer abc.def.ghi", "bearer abc.def.ghi", "BEARER abc.def.ghi"} {
		got, err := Bearer(http.Header{"Authorization": {s}})
		if err != nil || got != "abc.def.ghi" {
			t.Fatal(s, got, err)
		}
	}
	for _, h := range []http.Header{nil, {"Authorization": {}}, {"Authorization": {"Bearer a", "Bearer b"}}, {"Authorization": {"Bearer a"}, "authorization": {"Bearer b"}}, {"Authorization": {"Basic a"}}, {"Authorization": {"Bearer a,b"}}, {"Authorization": {"Bearer a b"}}, {"Authorization": {"Bearer a\tb"}}, {"Authorization": {"Bearer "}}, {"Authorization": {"Bearer  a"}}, {"Authorization": {" Bearer a"}}} {
		if _, err := Bearer(h); err == nil {
			t.Fatal(h)
		}
	}
}
