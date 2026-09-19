package control

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

// Envelope does envelope encryption: a random per-secret data key (DEK)
// encrypts the value, and the root key (which lives outside the cell, e.g.
// env/KMS) wraps the DEK. Only the wrapped DEK and ciphertext are stored.
type Envelope struct {
	root cipher.AEAD
}

// ParseRootKey decodes a 32-byte root key from base64 (standard or URL) or hex.
func ParseRootKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("control: empty root key")
	}
	for _, dec := range []func(string) ([]byte, error){
		base64.StdEncoding.DecodeString,
		base64.RawStdEncoding.DecodeString,
		base64.URLEncoding.DecodeString,
		hex.DecodeString,
	} {
		if b, err := dec(s); err == nil && len(b) == 32 {
			return b, nil
		}
	}
	return nil, fmt.Errorf("control: root key must decode to 32 bytes")
}

// NewEnvelope builds an envelope cipher from a 32-byte root key.
func NewEnvelope(rootKey []byte) (*Envelope, error) {
	if len(rootKey) != 32 {
		return nil, fmt.Errorf("control: root key must be 32 bytes")
	}
	block, err := aes.NewCipher(rootKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Envelope{root: gcm}, nil
}

func sealWith(aead cipher.AEAD, plaintext []byte) ([]byte, error) {
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return append(nonce, aead.Seal(nil, nonce, plaintext, nil)...), nil
}

func openWith(aead cipher.AEAD, blob []byte) ([]byte, error) {
	ns := aead.NonceSize()
	if len(blob) < ns {
		return nil, fmt.Errorf("control: ciphertext too short")
	}
	return aead.Open(nil, blob[:ns], blob[ns:], nil)
}

// Seal encrypts a value, returning the wrapped DEK and the value ciphertext
// (each a nonce-prefixed AEAD blob).
func (e *Envelope) Seal(value []byte) (wrappedDEK, ciphertext []byte, err error) {
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		return nil, nil, err
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, nil, err
	}
	valueAEAD, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}
	ciphertext, err = sealWith(valueAEAD, value)
	if err != nil {
		return nil, nil, err
	}
	wrappedDEK, err = sealWith(e.root, dek)
	if err != nil {
		return nil, nil, err
	}
	return wrappedDEK, ciphertext, nil
}

// Open decrypts a value sealed by Seal.
func (e *Envelope) Open(wrappedDEK, ciphertext []byte) ([]byte, error) {
	dek, err := openWith(e.root, wrappedDEK)
	if err != nil {
		return nil, fmt.Errorf("control: unwrap data key: %w", err)
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	valueAEAD, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return openWith(valueAEAD, ciphertext)
}
