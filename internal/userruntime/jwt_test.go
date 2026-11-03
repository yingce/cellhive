package userruntime

import (
	"strings"
	"testing"
)

func TestJWTTrustedModules(t *testing.T) {
	if strings.Count(capnpTemplate, `(name = "jwt.js", esModule = embed "jwt.js")`) != 2 {
		t.Fatal("both trusted workers need JWT module")
	}
	if strings.Count(capnpTemplate, `(name = "BINDING_SIGNING_KEY", fromEnvironment = "CELLHIVE_HOST_BINDING_SIGNING_KEY")`) != 2 {
		t.Fatal("both trusted workers need purpose-isolated binding key")
	}
}
