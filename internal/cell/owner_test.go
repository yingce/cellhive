package cell

import "testing"

func TestSupportedProtoVersion(t *testing.T) {
	if !SupportedProtoVersion("") || !SupportedProtoVersion("v1") {
		t.Fatal("legacy/v1 must be readable")
	}
	if SupportedProtoVersion("v2") || SupportedProtoVersion("garbage") {
		t.Fatal("unknown versions must be rejected")
	}
}
