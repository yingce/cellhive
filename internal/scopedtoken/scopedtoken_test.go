package scopedtoken

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestMintVerifyRoundTrip(t *testing.T) {
	secret := []byte("s3cret")
	now := time.Unix(1_700_000_000, 0)
	c := Claims{Namespace: "acme", Kind: "kv", Name: "main", ExpiresMs: now.Add(time.Minute).UnixMilli()}
	tok, err := Mint(secret, c)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	got, err := Verify(secret, tok, now)
	if err != nil || got != c {
		t.Fatalf("verify = %+v, %v", got, err)
	}
}

func TestVerifyRejectsTamperedExpiredAndWrongKey(t *testing.T) {
	secret := []byte("s3cret")
	now := time.Unix(1_700_000_000, 0)
	c := Claims{Namespace: "acme", Kind: "kv", Name: "main", ExpiresMs: now.Add(time.Minute).UnixMilli()}
	tok, _ := Mint(secret, c)

	// Tampered payload (flip a character in the payload part).
	bad := "x" + tok[1:]
	if _, err := Verify(secret, bad, now); err == nil {
		t.Fatalf("tampered token accepted")
	}
	// Wrong secret.
	if _, err := Verify([]byte("other"), tok, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("wrong key err = %v", err)
	}
	// Expired.
	if _, err := Verify(secret, tok, now.Add(2*time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired err = %v", err)
	}
	// Malformed.
	for _, m := range []string{"", "nodot", "a.", ".b", "!!!"} {
		if _, err := Verify(secret, m, now); err == nil {
			t.Fatalf("malformed %q accepted", m)
		}
	}
}

func TestMintRejectsIncompleteClaims(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	for _, c := range []Claims{
		{Kind: "kv", Name: "main", ExpiresMs: now.UnixMilli() + 1000},
		{Namespace: "acme", Name: "main", ExpiresMs: now.UnixMilli() + 1000},
		{Namespace: "acme", Kind: "kv", ExpiresMs: now.UnixMilli() + 1000},
	} {
		if _, err := Mint([]byte("k"), c); !errors.Is(err, ErrIncomplete) {
			t.Fatalf("incomplete claims accepted: %+v", c)
		}
	}
	if _, err := Mint(nil, Claims{Namespace: "a", Kind: "kv", Name: "n", ExpiresMs: 1}); err == nil {
		t.Fatalf("empty secret accepted")
	}
}

// TestNonExpiringToken covers ADR-074: ExpiresMs==0 means no expiry; revocation
// is via binding registration + secret rotation.
func TestNonExpiringToken(t *testing.T) {
	secret := []byte("s3cret")
	c := Claims{Namespace: "acme", Kind: "kv", Name: "main"}
	tok, err := Mint(secret, c)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	// Still valid far in the future.
	got, err := Verify(secret, tok, time.Unix(4_000_000_000, 0))
	if err != nil || got != c || got.ExpiresMs != 0 {
		t.Fatalf("verify = %+v, %v", got, err)
	}
}

// TestCanonicalPayloadFormat pins the signed JSON bytes so the platform JS
// loader reproduces them exactly (ADR-074 cross-language contract).
func TestCanonicalPayloadFormat(t *testing.T) {
	b, err := canonical(Claims{Namespace: "acme", Kind: "kv", Name: "main"})
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	if want := `{"ns":"acme","kind":"kv","name":"main"}`; string(b) != want {
		t.Fatalf("canonical = %s, want %s", b, want)
	}
}

// TestMatchGlob covers segment-level matching (ADR-181): "*" any, "pre*" prefix,
// else exact, and no cross-prefix confusion ("acme" must not match "acmex").
func TestMatchGlob(t *testing.T) {
	cases := []struct {
		pattern, value string
		want           bool
	}{
		{"*", "anything", true},
		{"acme", "acme", true},
		{"acme", "acmex", false},
		{"user*", "user42", true},
		{"user*", "user", true},
		{"user*", "admin", false},
		{"", "", false},
	}
	for _, tc := range cases {
		if got := matchOne(tc.pattern, tc.value); got != tc.want {
			t.Fatalf("matchOne(%q,%q)=%v, want %v", tc.pattern, tc.value, got, tc.want)
		}
	}
}

// TestIssuerKeyAndVerifyIssuer covers delegated issuer tokens (ADR-181): the
// issuer key is derived from the scope secret, an issuer token is rejected by
// the platform key and by a different issuer, and an empty Iss uses the scope
// key directly.
func TestIssuerKeyAndVerifyIssuer(t *testing.T) {
	scope := []byte("scope-secret")
	now := time.Unix(1_700_000_000, 0)
	key, err := IssuerKey(scope, "vwork")
	if err != nil {
		t.Fatalf("issuer key: %v", err)
	}
	if bytes.Equal(key, scope) {
		t.Fatal("issuer key equals the scope secret")
	}
	c := Claims{Namespace: "acme", Kind: "d1", Name: "user*", Iss: "vwork", ExpiresMs: now.Add(time.Minute).UnixMilli()}
	tok, err := Mint(key, c)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	got, err := VerifyIssuer(scope, tok, now)
	if err != nil || got != c {
		t.Fatalf("verify issuer = %+v, %v", got, err)
	}
	if _, err := Verify(scope, tok, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("platform key accepted issuer token: %v", err)
	}
	other, _ := IssuerKey(scope, "other")
	if _, err := Verify(other, tok, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("wrong issuer key accepted: %v", err)
	}
	plat, _ := Mint(scope, Claims{Namespace: "acme", Kind: "kv", Name: "main"})
	if _, err := VerifyIssuer(scope, plat, now); err != nil {
		t.Fatalf("platform token via VerifyIssuer: %v", err)
	}
}

// TestCanonicalPayloadFormatWithIssuer pins the field order ns,kind,name,iss,exp_ms.
func TestCanonicalPayloadFormatWithIssuer(t *testing.T) {
	b, err := canonical(Claims{Namespace: "acme", Kind: "d1", Name: "user*", Iss: "vwork", ExpiresMs: 123})
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	if want := `{"ns":"acme","kind":"d1","name":"user*","iss":"vwork","exp_ms":123}`; string(b) != want {
		t.Fatalf("canonical = %s, want %s", b, want)
	}
}
