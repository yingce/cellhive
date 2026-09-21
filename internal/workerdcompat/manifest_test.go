package workerdcompat

import (
	"testing"

	"cellhive/internal/workerdbin"
)

func TestManifestMatchesPinnedWorkerd(t *testing.T) {
	m := Manifest()
	if m.WorkerdVersion != workerdbin.PinnedVersion {
		t.Fatalf("manifest=%s pin=%s", m.WorkerdVersion, workerdbin.PinnedVersion)
	}
	if m.WorkerdVersion != "1.20260916.1" {
		t.Fatalf("unexpected pin %s", m.WorkerdVersion)
	}
	if m.UpstreamRevision != "adda2635656d09e541b0feeea796da9d2a8bc10e" {
		t.Fatalf("unexpected upstream revision %q", m.UpstreamRevision)
	}
	if m.SourceSHA256 == "" {
		t.Fatal("manifest lacks source identity")
	}
	if m.MaxCompatibilityDate != "2026-09-23" {
		t.Fatalf("max compatibility date = %q", m.MaxCompatibilityDate)
	}
	for i := 1; i < len(m.Flags); i++ {
		if m.Flags[i-1].Name >= m.Flags[i].Name {
			t.Fatalf("flags are not sorted and unique at %q, %q", m.Flags[i-1].Name, m.Flags[i].Name)
		}
	}
	if !Allowed("nodejs_compat") {
		t.Fatal("stable upstream flag nodejs_compat is not allowed")
	}
	if Allowed("durable_object_rename") {
		t.Fatal("experimental upstream flag durable_object_rename is allowed")
	}
}
