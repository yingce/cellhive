package main

import (
	"strings"
	"testing"
)

func TestParseCompatibilityFlagsSortsAndMarksExperimentalFields(t *testing.T) {
	source := []byte(`
struct CompatibilityFlags @0x1 {
  stableFeature @0 :Bool
      $compatDisableFlag("z_disable")
      $compatEnableFlag("a_enable");
  experimentalFeature @1 :Bool
      $compatEnableFlag("experimental_enable")
      $experimental;
}
`)
	flags, err := parseCompatibilityFlags(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(flags) != 3 {
		t.Fatalf("flags=%+v", flags)
	}
	if flags[0].Name != "a_enable" || flags[0].Experimental || !flags[0].CellHiveAllowed {
		t.Fatalf("stable enable flag=%+v", flags[0])
	}
	if flags[1].Name != "experimental_enable" || !flags[1].Experimental || flags[1].CellHiveAllowed {
		t.Fatalf("experimental enable flag=%+v", flags[1])
	}
	if flags[2].Name != "z_disable" || flags[2].Experimental || !flags[2].CellHiveAllowed {
		t.Fatalf("stable disable flag=%+v", flags[2])
	}
}

func TestRenderTypeScriptIsDeterministic(t *testing.T) {
	data := manifestData{
		WorkerdVersion: "1.test", MaxCompatibilityDate: "2026-01-02",
		Flags: []manifestFlag{{Name: "z", CellHiveAllowed: false}, {Name: "a", CellHiveAllowed: true}},
	}
	got := string(renderTypeScript(data))
	if !strings.Contains(got, `PINNED_WORKERD = "1.test"`) ||
		!strings.Contains(got, `COMPAT_DATE_MAX = "2026-01-02"`) {
		t.Fatalf("generated constants missing:\n%s", got)
	}
	if strings.Contains(got, `"z"`) || !strings.Contains(got, `"a"`) {
		t.Fatalf("generated allowed flags wrong:\n%s", got)
	}
}

func TestParseCompatibilityFlagsAppliesExplicitUnsupportedPolicy(t *testing.T) {
	source := []byte(`
struct CompatibilityFlags @0x1 {
  registry @0 :Bool $compatEnableFlag("new_module_registry");
}
`)
	flags, err := parseCompatibilityFlags(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(flags) != 1 || flags[0].CellHiveAllowed {
		t.Fatalf("new_module_registry must fail closed: %+v", flags)
	}
}
