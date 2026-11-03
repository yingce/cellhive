package doticket

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTicketMintVerify(t *testing.T) {
	secret := bytes.Repeat([]byte{7}, 32)
	now := time.Now().Truncate(time.Second)
	c := Claims{NS: "acme", Worker: "web", StorageClass: "Room", Class: "RoomV2", BundleSHA: "bundle", StorageID: "incarnation", Version: 2, Limits: "null", Shard: 7, Exp: now.Add(30 * time.Second).UnixMilli()}
	tok, err := Mint(secret, c)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	got, err := Verify(secret, tok, now)
	if err != nil || got != c {
		t.Fatalf("verify = %+v err=%v, want %+v", got, err, c)
	}
	if _, err := Verify(bytes.Repeat([]byte{8}, 32), tok, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("wrong secret err = %v, want ErrSignature", err)
	}
	if _, err := Verify(secret, tok, now.Add(2*time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired err = %v, want ErrExpired", err)
	}
	if _, err := Verify(secret, "garbage", now); !errors.Is(err, ErrMalformed) {
		t.Fatalf("malformed err = %v, want ErrMalformed", err)
	}
}

func TestTicketJWTWire(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	token, err := Mint(bytes.Repeat([]byte{7}, 32), Claims{NS: "acme", Worker: "web", StorageClass: "Room", Class: "Room", BundleSHA: "bundle", StorageID: "incarnation", Version: 1, Limits: "null", Shard: 0, Exp: now.Add(30 * time.Second).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("ticket must be JWT, got %d segments", len(parts))
	}
	raw, _ := base64.RawURLEncoding.DecodeString(parts[0])
	var header map[string]any
	if err := json.Unmarshal(raw, &header); err != nil {
		t.Fatal(err)
	}
	if header["kid"] != "token/do-ticket" || header["alg"] != "HS256" || header["typ"] != "JWT" {
		t.Fatal(header)
	}
	raw, _ = base64.RawURLEncoding.DecodeString(parts[1])
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	if claims["exp"] != float64(now.Add(30*time.Second).Unix()) || claims["aud"] != "cellhive" || claims["type"] != "internal" {
		t.Fatal(claims)
	}
}

func TestTicketRequiresExecutionIdentity(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	c := Claims{NS: "acme", Worker: "web", StorageClass: "Room", Shard: 0, Exp: time.Now().Add(30 * time.Second).UnixMilli()}
	if _, err := Mint(key, c); err == nil {
		t.Fatal("accepted unsigned execution identity")
	}
}

func TestTicketRejectsNonJWTKeyAndOutOfRangeShard(t *testing.T) {
	c := Claims{NS: "acme", Worker: "web", StorageClass: "Room", Shard: 0, Exp: time.Now().Add(time.Minute).UnixMilli()}
	if _, err := Mint([]byte("legacy-secret"), c); err == nil {
		t.Fatal("accepted legacy key")
	}
	c.Shard = 16
	if _, err := Mint(bytes.Repeat([]byte{7}, 32), c); err == nil {
		t.Fatal("accepted shard outside runtime shard count")
	}
}
