package main

import (
	"cellhive/internal/bearertoken"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGetBodyValidation(t *testing.T) {
	for _, body := range []string{"xxxx", "xxx", "xxxxx", "xxxy"} {
		t.Run(body, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer srv.Close()
			cfg := conf{addr: srv.URL, mode: "get", nsList: []string{"bench"}, tokens: []string{"test"}, body: []byte("xxxx"), verifyGet: true}
			code, err := call(srv.Client(), cfg, "k0", 0)
			if code != 200 || (err == nil) != (body == "xxxx") {
				t.Fatalf("code=%d err=%v body=%q", code, err, body)
			}
		})
	}
}

func TestBindingAuthorization(t *testing.T) {
	c := conf{signingKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", nsList: []string{"bench"}, tokens: []string{""}, kind: "kv", name: "KV"}
	token, err := c.authorization(0)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := bearertoken.DecodeKey(c.signingKey)
	kid, claims, err := bearertoken.Verify(key, token, time.Now())
	if err != nil || kid != "token/scope" || claims.Type != "binding" || claims.Namespace != "bench" || claims.ExpiresAt-claims.IssuedAt != 300 {
		t.Fatalf("kid=%s claims=%+v err=%v", kid, claims, err)
	}
	if _, _, err := bearertoken.Verify(key, token, time.Unix(claims.ExpiresAt, 0)); err == nil {
		t.Fatal("expired benchmark credential accepted")
	}
	c.tokens[0] = "supplied-jwt"
	if token, err := c.authorization(0); err != nil || token != "supplied-jwt" {
		t.Fatalf("explicit credential changed: %s %v", token, err)
	}
}
