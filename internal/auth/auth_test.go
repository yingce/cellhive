package auth

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func signRS256(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	head, _ := json.Marshal(map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid})
	payload, _ := json.Marshal(claims)
	signing := b64(head) + "." + b64(payload)
	h := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, h[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signing + "." + b64(sig)
}

func jwksServer(t *testing.T, kid string, key *rsa.PublicKey) *httptest.Server {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "RSA", "kid": kid, "alg": "RS256",
		"n": b64(key.N.Bytes()), "e": b64(big.NewInt(int64(key.E)).Bytes()),
	}}})
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write(body)
	}))
}

func TestStaticToken(t *testing.T) {
	a := StaticToken{Token: "ops"}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if a.Authenticate(r) {
		t.Fatalf("empty header accepted")
	}
	r.Header.Set("x-cellhive-admin-token", "nope")
	if a.Authenticate(r) {
		t.Fatalf("wrong token accepted")
	}
	r.Header.Set("x-cellhive-admin-token", "ops")
	if !a.Authenticate(r) {
		t.Fatalf("correct token rejected")
	}
}

func TestJWTBearerRS256(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	srv := jwksServer(t, "k1", &key.PublicKey)
	defer srv.Close()
	now := time.Unix(1_700_000_000, 0)
	j := &JWTBearer{
		Issuer: "https://issuer", Audience: "cellhive",
		Keys: &JWKS{URL: srv.URL, Now: func() time.Time { return now }},
		Now:  func() time.Time { return now },
	}

	good := signRS256(t, key, "k1", map[string]any{
		"iss": "https://issuer", "aud": "cellhive", "exp": now.Add(time.Minute).Unix(),
	})
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer "+good)
	if !j.Authenticate(r) {
		t.Fatalf("valid jwt rejected")
	}

	// Wrong audience.
	badAud := signRS256(t, key, "k1", map[string]any{"iss": "https://issuer", "aud": "other", "exp": now.Add(time.Minute).Unix()})
	r.Header.Set("Authorization", "Bearer "+badAud)
	if j.Authenticate(r) {
		t.Fatalf("bad audience accepted")
	}
	// Expired.
	exp := signRS256(t, key, "k1", map[string]any{"iss": "https://issuer", "aud": "cellhive", "exp": now.Add(-time.Minute).Unix()})
	r.Header.Set("Authorization", "Bearer "+exp)
	if j.Authenticate(r) {
		t.Fatalf("expired jwt accepted")
	}
	// Tampered payload (changes the signed input, so the signature no longer matches).
	parts := strings.Split(good, ".")
	tampered := parts[0] + "." + parts[1] + "A." + parts[2]
	r.Header.Set("Authorization", "Bearer "+tampered)
	if j.Authenticate(r) {
		t.Fatalf("tampered jwt accepted")
	}
	// Wrong issuer.
	badIss := signRS256(t, key, "k1", map[string]any{"iss": "evil", "aud": "cellhive", "exp": now.Add(time.Minute).Unix()})
	r.Header.Set("Authorization", "Bearer "+badIss)
	if j.Authenticate(r) {
		t.Fatalf("bad issuer accepted")
	}
	// A different key cannot sign for kid k1.
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	forged := signRS256(t, other, "k1", map[string]any{"iss": "https://issuer", "aud": "cellhive", "exp": now.Add(time.Minute).Unix()})
	r.Header.Set("Authorization", "Bearer "+forged)
	if j.Authenticate(r) {
		t.Fatalf("forged signature accepted")
	}
}

func TestAnyFallsBackToStatic(t *testing.T) {
	a := Any{&JWTBearer{Keys: &JWKS{URL: "http://127.0.0.1:1"}, Issuer: "x"}, StaticToken{Token: "ops"}}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("x-cellhive-admin-token", "ops")
	if !a.Authenticate(r) {
		t.Fatalf("static fallback failed")
	}
}

// TestJWTWithoutExpiryRejected covers ADR-135: a bearer token without an `exp`
// claim must not be accepted (it would never expire).
func TestJWTWithoutExpiryRejected(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	jwks := jwksServer(t, "k1", &key.PublicKey)
	defer jwks.Close()
	j := &JWTBearer{Issuer: "https://issuer", Audience: "cellhive", Keys: &JWKS{URL: jwks.URL, HTTP: &http.Client{}}}
	tok := signRS256(t, key, "k1", map[string]any{"iss": "https://issuer", "aud": "cellhive"})
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	if j.Authenticate(r) {
		t.Fatal("JWT without exp was accepted")
	}
}

// TestJWKSServesStaleDuringRefresh covers ADR-135: a slow JWKS refresh must not
// block verifications that can be served from the cached key set.
func TestJWKSServesStaleDuringRefresh(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	body, _ := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "RSA", "kid": "k1", "alg": "RS256",
		"n": b64(key.N.Bytes()), "e": b64(big.NewInt(int64(key.E)).Bytes()),
	}}})
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) > 1 {
			entered <- struct{}{}
			<-release // hold the refresh
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	j := &JWKS{URL: srv.URL, HTTP: &http.Client{}, TTL: 10 * time.Millisecond}
	if _, err := j.Key("k1"); err != nil { // prime the cache
		t.Fatalf("prime: %v", err)
	}
	time.Sleep(20 * time.Millisecond)  // let the TTL expire
	go func() { _, _ = j.Key("k1") }() // starts the (blocked) refresh
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh never started")
	}
	done := make(chan error, 1)
	go func() {
		_, err := j.Key("k1")
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Key during refresh: %v", err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("Key blocked on an in-flight refresh instead of serving the cached key")
	}
	close(release)
}
