package wranglercompat

import (
	"os"
	"regexp"
	"testing"
)

func codes(fs []Finding) map[string]string {
	m := map[string]string{}
	for _, f := range fs {
		m[f.Code] = f.FieldPath
	}
	return m
}

func TestValidateOK(t *testing.T) {
	res := Validate(Input{
		BundleSHA:          "abc",
		CompatibilityDate:  "2026-06-15",
		CompatibilityFlags: []string{"nodejs_compat"},
		Bindings:           []Binding{{Type: "kv", Name: "KV"}, {Type: "do", Name: "ROOM"}},
		IsRegistered:       func(kind, name string) bool { return true },
	})
	if !res.OK() {
		t.Fatalf("expected OK, got %+v", res.Errors)
	}
}

// TestFormerPlatformEnvNamesAllowed ensures that env names formerly reserved
// for platform transport are wholly user-owned (ADR-185).
func TestFormerPlatformEnvNamesAllowed(t *testing.T) {
	res := Validate(Input{
		BundleSHA:         "abc",
		CompatibilityDate: "2026-06-15",
		Bindings: []Binding{
			{Type: "kv", Name: "CH_PLATFORM"},
			{Type: "do", Name: "CELL_URL"},
			{Type: "kv", Name: "__cellhive_test"},
		},
		Vars: map[string]string{
			"PLATFORM":  "x",
			"LOG_TOKEN": "y",
			"WF_ID":     "z",
		},
		IsRegistered: func(kind, name string) bool { return true },
	})
	for _, f := range append(res.Errors, res.Warnings...) {
		if f.Code == "reserved_env_name" {
			t.Fatalf("former platform name rejected: %+v", f)
		}
	}
}

func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name  string
		in    Input
		code  string
		field string
	}{
		{
			name:  "missing bundle",
			in:    Input{},
			code:  "missing_bundle",
			field: "bundle_sha",
		},
		{
			name:  "compat date too new",
			in:    Input{BundleSHA: "x", CompatibilityDate: "2026-08-01"},
			code:  "compat_date_too_new",
			field: "compatibility_date",
		},
		{
			name:  "unknown flag",
			in:    Input{BundleSHA: "x", CompatibilityFlags: []string{"nope"}},
			code:  "unknown_flag",
			field: "compatibility_flags",
		},
		{
			name:  "rejected binding images",
			in:    Input{BundleSHA: "x", Bindings: []Binding{{Type: "images", Name: "IMAGES"}}},
			code:  "unsupported_binding",
			field: "bindings[0].type",
		},
		{
			name:  "unknown binding type",
			in:    Input{BundleSHA: "x", Bindings: []Binding{{Type: "wat", Name: "W"}}},
			code:  "unsupported_binding",
			field: "bindings[0].type",
		},
		{
			name: "unregistered resource",
			in: Input{
				BundleSHA:    "x",
				Bindings:     []Binding{{Type: "kv", Name: "KV"}},
				IsRegistered: func(kind, name string) bool { return false },
			},
			code:  "binding_unregistered",
			field: "bindings[0].name",
		},
		{
			name:  "unknown field",
			in:    Input{BundleSHA: "x", UnknownFields: []string{"containers"}},
			code:  "unknown_field",
			field: "containers",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := Validate(tc.in)
			got := codes(res.Errors)
			field, ok := got[tc.code]
			if !ok {
				t.Fatalf("missing code %q in %+v", tc.code, res.Errors)
			}
			if field != tc.field {
				t.Fatalf("code %q field_path = %q, want %q", tc.code, field, tc.field)
			}
		})
	}
}

func TestValidateServiceAndDOExemptFromRegistration(t *testing.T) {
	res := Validate(Input{
		BundleSHA:    "x",
		Bindings:     []Binding{{Type: "do", Name: "ROOM"}, {Type: "service", Name: "SVC"}},
		IsRegistered: func(kind, name string) bool { return false },
	})
	if !res.OK() {
		t.Fatalf("do/service should not require registration, got %+v", res.Errors)
	}
}

func TestValidateMaxDateBoundary(t *testing.T) {
	if res := Validate(Input{BundleSHA: "x", CompatibilityDate: MaxCompatibilityDate}); !res.OK() {
		t.Fatalf("exactly max date should pass, got %+v", res.Errors)
	}
}

func TestValidateMigrations(t *testing.T) {
	allowed := []map[string]any{
		{"tag": "v1", "new_sqlite_classes": []any{"Room"}},
		{"tag": "v2", "renamed_classes": []any{map[string]any{"from": "A", "to": "B"}}},
		{"tag": "v3", "deleted_classes": []any{"Old"}},
		{"tag": "v4", "transferred_classes": []any{map[string]any{"from": "A", "to": "B"}}},
	}
	if res := ValidateMigrations(allowed); len(res) != 0 {
		t.Fatalf("allowed migrations rejected: %+v", res)
	}
	// Cross-worker transfer (script_name) is rejected.
	res := ValidateMigrations([]map[string]any{{"tag": "v6", "transferred_classes": []any{map[string]any{"from": "A", "to": "B", "script_name": "other"}}}})
	if len(res) != 1 || res[0].Code != "invalid_migration" {
		t.Fatalf("cross-worker transfer -> %+v", res)
	}
	// unknown field.
	res = ValidateMigrations([]map[string]any{{"tag": "v5", "something": 1}})
	if len(res) != 1 || res[0].Code != "unknown_migration_field" {
		t.Fatalf("unknown field -> %+v", res)
	}
	// malformed rename shape.
	res = ValidateMigrations([]map[string]any{{"tag": "v6", "renamed_classes": []any{map[string]any{"from": "A"}}}})
	if len(res) != 1 || res[0].Code != "invalid_migration" {
		t.Fatalf("bad shape -> %+v", res)
	}
}

func TestValidateCrons(t *testing.T) {
	if res := ValidateCrons([]string{"*/5 * * * *", "0 0 * * *", "30 2 * * 1-5"}); len(res) != 0 {
		t.Fatalf("valid crons rejected: %+v", res)
	}
	res := ValidateCrons([]string{"not a cron", "*/5 * * *"})
	if len(res) != 2 || res[0].Code != "invalid_cron" || res[1].Code != "invalid_cron" {
		t.Fatalf("invalid crons -> %+v", res)
	}
}

func TestWorkflowBindingRequiresClassName(t *testing.T) {
	base := Input{Namespace: "acme", Worker: "api", BundleSHA: "sha",
		IsRegistered: func(kind, name string) bool { return true }}
	base.Bindings = []Binding{{Type: "workflow", Name: "WF"}}
	if res := Validate(base); len(res.Errors) == 0 || res.Errors[0].Code != "invalid_binding" {
		t.Fatalf("missing class_name -> %+v", res.Errors)
	}
	ok := base
	ok.Bindings = []Binding{{Type: "workflow", Name: "WF", ClassName: "MyWF"}}
	if res := Validate(ok); len(res.Errors) != 0 {
		t.Fatalf("workflow with class_name rejected: %+v", res.Errors)
	}
}

// TestVendorMatrixContract pins the supported binding kinds and migration keys so
// docs/compatibility-matrix.md cannot drift silently (P5 regression guard).
func TestVendorMatrixContract(t *testing.T) {
	// vectorize is supported as of ADR-158 (registered resource + exact KNN).
	wantBindings := []string{"kv", "d1", "r2", "queue", "workflow", "do", "service", "ai", "hyperdrive", "vectorize"}
	if len(SupportedBindingKinds) != len(wantBindings) {
		t.Fatalf("supported bindings = %v", SupportedBindingKinds)
	}
	for _, k := range wantBindings {
		if !SupportedBindingKinds[k] {
			t.Fatalf("missing supported binding kind %q", k)
		}
	}
	for _, rejected := range []string{"browser", "images", "send_email"} {
		if SupportedBindingKinds[rejected] {
			t.Fatalf("binding %q must not be supported", rejected)
		}
	}
	// The vectorize resource is referenced by index_name (Binding.ID), not the
	// binding name (ADR-158).
	reg := Input{Namespace: "acme", Worker: "api", BundleSHA: "sha",
		IsRegistered: func(kind, name string) bool { return kind == "vectorize" && name == "docs" }}
	reg.Bindings = []Binding{{Type: "vectorize", Name: "INDEX", ID: "docs"}}
	if res := Validate(reg); len(res.Errors) != 0 {
		t.Fatalf("vectorize binding with a registered index rejected: %+v", res.Errors)
	}
	reg.Bindings = []Binding{{Type: "vectorize", Name: "INDEX", ID: "other"}}
	if res := Validate(reg); len(res.Errors) == 0 || res.Errors[0].Code != "binding_unregistered" {
		t.Fatalf("unregistered vectorize index accepted: %+v", res.Errors)
	}
	for _, k := range []string{"renamed_classes", "deleted_classes", "transferred_classes"} {
		if !SupportedMigrationKeys[k] {
			t.Fatalf("migration %q must be supported (ADR-082/084)", k)
		}
	}
}

// TestKnownFlagsMatchDevCLI: the Go validator and the Bun dev CLI must accept
// exactly the same compatibility flags (ADR-153); a one-sided edit fails here.
func TestKnownFlagsMatchDevCLI(t *testing.T) {
	raw, err := os.ReadFile("../../cli/src/validate.ts")
	if err != nil {
		t.Fatalf("read cli validator: %v", err)
	}
	re := regexp.MustCompile(`(?s)KNOWN_COMPAT_FLAGS = new Set<string>\(\[(.*?)\]\)`)
	m := re.FindSubmatch(raw)
	if m == nil {
		t.Fatal("KNOWN_COMPAT_FLAGS not found in cli/src/validate.ts")
	}
	cli := map[string]bool{}
	for _, q := range regexp.MustCompile(`"([a-z0-9_]+)"`).FindAllStringSubmatch(string(m[1]), -1) {
		cli[q[1]] = true
	}
	if len(cli) == 0 {
		t.Fatal("parsed no flags from the CLI list")
	}
	for f := range KnownCompatibilityFlags {
		if !cli[f] {
			t.Errorf("flag %q is accepted by the server but missing from the dev CLI", f)
		}
	}
	for f := range cli {
		if !KnownCompatibilityFlags[f] {
			t.Errorf("flag %q is accepted by the dev CLI but not the server", f)
		}
	}
}

// TestPinPairsWithCompatibilityDate: the workerd pin and the compatibility
// ceiling are one decision; bumping one without the other fails (ADR-153).
func TestPinPairsWithCompatibilityDate(t *testing.T) {
	if PinnedWorkerdVersion != "1.20260615.1" {
		t.Fatalf("pinned workerd changed to %q: re-verify MaxCompatibilityDate (%s) against the new release and update this test", PinnedWorkerdVersion, MaxCompatibilityDate)
	}
	if MaxCompatibilityDate != "2026-06-22" {
		t.Fatalf("compatibility ceiling changed to %q: re-verify against pinned workerd %s", MaxCompatibilityDate, PinnedWorkerdVersion)
	}
}
