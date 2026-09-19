package scopedtoken

import (
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
