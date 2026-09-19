package doticket

import (
	"errors"
	"testing"
	"time"
)

func TestTicketMintVerify(t *testing.T) {
	secret := []byte("s3cret")
	now := time.Now()
	c := Claims{NS: "acme", Worker: "web", StorageClass: "Room", Shard: 7, Exp: now.Add(time.Minute).UnixMilli()}
	tok, err := Mint(secret, c)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	got, err := Verify(secret, tok, now)
	if err != nil || got != c {
		t.Fatalf("verify = %+v err=%v, want %+v", got, err, c)
	}
	if _, err := Verify([]byte("other"), tok, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("wrong secret err = %v, want ErrSignature", err)
	}
	if _, err := Verify(secret, tok, now.Add(2*time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired err = %v, want ErrExpired", err)
	}
	if _, err := Verify(secret, "garbage", now); !errors.Is(err, ErrMalformed) {
		t.Fatalf("malformed err = %v, want ErrMalformed", err)
	}
}
