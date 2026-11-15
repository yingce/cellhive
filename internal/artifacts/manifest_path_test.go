package artifacts

import (
	"errors"
	"testing"
)

func TestManifestTokenRejectsDelimiterInPath(t *testing.T) {
	a, b := Hash([]byte("A")), Hash([]byte("B"))
	full := map[string]File{"a": {SHA256: a, Size: 1}, "b": {SHA256: b, Size: 1}}
	path := "a\x00" + a + "\nb"
	subset := map[string]File{path: {SHA256: b, Size: 1}}
	if token := ManifestToken(subset); token != "" {
		t.Fatalf("invalid path produced token %q (original=%q)", token, ManifestToken(full))
	}
	if _, err := AssetKey("acme", "web", ManifestToken(full), path); !errors.Is(err, ErrBadPath) {
		t.Fatalf("NUL asset path error=%v", err)
	}
	if ManifestToken(full) == "" {
		t.Fatal("valid commitment rejected")
	}
}
