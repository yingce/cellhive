package config

import (
	"fmt"

	"cellhive/internal/bearertoken"
)

// JWTKey derives a purpose-separated JWT key without changing persisted
// encryption or database credential derivations.
func (c Config) JWTKey(kid string) ([]byte, error) {
	root, ok := decodeRootKey(c.RootKey)
	if !ok {
		return nil, fmt.Errorf("config: valid CELLHIVE_ROOT_KEY required for JWT signing")
	}
	return bearertoken.DeriveKey(root, kid)
}
