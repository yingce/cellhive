// Package wranglercompat holds the platform's authoritative Wrangler
// compatibility gate (ADR-014, ADR-065). It is used by the control-plane
// deploy endpoint to reject unsupported bindings/fields/flags server-side,
// so a permissive local dev runtime (Miniflare) can never smuggle a
// deployment the platform cannot run. The CLI mirrors these rules for fast
// preflight feedback, but the server is authoritative.
package wranglercompat

import (
	"strconv"
	"strings"

	"cellhive/internal/cron"
	"cellhive/internal/workerdbin"
)

// PinnedWorkerdVersion re-exports the stock workerd pin this validator targets
// (ADR-153), so a bump on one side without the other fails a test.
const PinnedWorkerdVersion = workerdbin.PinnedVersion

// MaxCompatibilityDate is the newest compatibility date the pinned workerd
// supports (verified 2026-06-22). Deploys above it are rejected.
const MaxCompatibilityDate = "2026-06-22"

// SupportedBindingKinds are the binding types the platform can actually serve
// (docs/bindings.md). Anything else is rejected.
var SupportedBindingKinds = map[string]bool{
	"kv":       true,
	"d1":       true,
	"r2":       true,
	"queue":    true,
	"workflow": true,
	"do":       true,
	"service":  true,
	"ai":       true,
	// hyperdrive carries a connection string only; the platform does not pool
	// connections (ADR-125).
	"hyperdrive": true,
	// vectorize indexes are registered resources with an immutable
	// dimensions/metric config (ADR-158).
	"vectorize": true,
}

// ResourceKinds are the binding kinds that must reference a registered
// control-plane resource (no auto-provisioning, ADR-014).
var ResourceKinds = map[string]bool{
	"kv":       true,
	"d1":       true,
	"r2":       true,
	"queue":    true,
	"workflow": true,
	// hyperdrive must reference a registered resource (the origin URL lives in
	// its scope; no auto-provisioning).
	"hyperdrive": true,
	"vectorize":  true,
}

// RejectedBindingHints explains bindings that Miniflare may emulate locally but
// the platform does not support (ADR-014). Kept for actionable error messages.
var RejectedBindingHints = map[string]string{
	"images":              "not supported",
	"ai-search":           "not supported",
	"browser":             "not supported",
	"browser-rendering":   "not supported",
	"send_email":          "not supported",
	"dispatch_namespaces": "not supported",
	"secrets_store":       "not supported",
	"containers":          "not supported",
	"cache":               "not supported",
	"analytics_engine":    "not supported",
	"python":              "not supported",
	"flagship":            "not supported",
	"pipelines":           "not supported",
}

var KnownCompatibilityFlags = map[string]bool{
	"nodejs_compat":                               true,
	"nodejs_compat_v2":                            true,
	"nodejs_compat_populate_process_env":          true,
	"no_handle_cross_request_promise_resolution":  true,
	"global_fetch_strictly_public":                true,
	"disable_fetch_stream_teeing":                 true,
	"streams_enable_constructors":                 true,
	"transformstream_enable_standard_constructor": true,
	"export_commonjs_default":                     true,
	"export_commonjs_namespace":                   true,
	"disable_nodejs_process_v2":                   true,
	"enable_ctx_exports":                          true,
	"deployment_id_header":                        true,
	"require_custom_ports_development":            true,
}

// Severity is a finding level.
type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

// Finding is one validation result with a stable code and field path.
type Finding struct {
	Severity  Severity `json:"severity"`
	Code      string   `json:"code"`
	FieldPath string   `json:"field_path,omitempty"`
	Message   string   `json:"message"`
}

// Binding is a deploy-time binding reference.
type Binding struct {
	Type string `json:"type"`
	Name string `json:"name"`
	ID   string `json:"id"`
	// ClassName is the exported WorkflowEntrypoint class for workflow bindings.
	ClassName string `json:"class_name,omitempty"`
	// Entrypoint is the target worker's named entrypoint for service bindings.
	Entrypoint string `json:"entrypoint,omitempty"`
}

// Input is a deploy request as seen by the compatibility gate.
type Input struct {
	Namespace          string
	Worker             string
	BundleSHA          string
	AssetsSHA          string
	CompatibilityDate  string
	CompatibilityFlags []string
	Bindings           []Binding
	// Vars are the worker's plain-text env vars (binding-name namespace too).
	Vars map[string]string
	// UnknownFields are top-level config keys the platform does not recognise.
	UnknownFields []string
	// IsRegistered reports whether a resource binding references a
	// control-plane-registered resource. Nil skips the check.
	IsRegistered func(kind, name string) bool
}

// Result is the validation outcome.
type Result struct {
	Errors   []Finding
	Warnings []Finding
}

// OK reports whether there are no errors.
func (r Result) OK() bool { return len(r.Errors) == 0 }

// Validate applies the platform compatibility gate.
func Validate(in Input) Result {
	var res Result

	if strings.TrimSpace(in.BundleSHA) == "" {
		res.Errors = append(res.Errors, Finding{
			Severity: SeverityError, Code: "missing_bundle", FieldPath: "bundle_sha",
			Message: "bundle_sha is required",
		})
	}

	if in.CompatibilityDate != "" && in.CompatibilityDate > MaxCompatibilityDate {
		res.Errors = append(res.Errors, Finding{
			Severity: SeverityError, Code: "compat_date_too_new", FieldPath: "compatibility_date",
			Message: "compatibility_date " + in.CompatibilityDate + " is newer than the platform maximum " + MaxCompatibilityDate,
		})
	}

	for _, f := range in.CompatibilityFlags {
		if !KnownCompatibilityFlags[f] {
			res.Errors = append(res.Errors, Finding{
				Severity: SeverityError, Code: "unknown_flag", FieldPath: "compatibility_flags",
				Message: "unknown compatibility flag " + f,
			})
		}
	}

	for i, b := range in.Bindings {
		path := "bindings[" + strconv.Itoa(i) + "].type"
		if !SupportedBindingKinds[b.Type] {
			msg := "binding type " + b.Type + " is not supported by the platform"
			if hint, ok := RejectedBindingHints[b.Type]; ok {
				msg += " (" + hint + ")"
			}
			res.Errors = append(res.Errors, Finding{
				Severity: SeverityError, Code: "unsupported_binding", FieldPath: path, Message: msg,
			})
			continue
		}
		// The registered resource is normally the binding name; for vectorize,
		// Cloudflare's index_name (our Binding.ID) is the resource while the
		// binding name is just the env key (ADR-158).
		ref := b.Name
		if b.Type == "vectorize" && b.ID != "" {
			ref = b.ID
		}
		if ResourceKinds[b.Type] && in.IsRegistered != nil && !in.IsRegistered(b.Type, ref) {
			res.Errors = append(res.Errors, Finding{
				Severity: SeverityError, Code: "binding_unregistered", FieldPath: "bindings[" + strconv.Itoa(i) + "].name",
				Message: "no registered " + b.Type + " resource named " + ref + " (create it first; no auto-provisioning)",
			})
		}
		if b.Type == "workflow" && b.ClassName == "" {
			res.Errors = append(res.Errors, Finding{
				Severity: SeverityError, Code: "invalid_binding", FieldPath: "bindings[" + strconv.Itoa(i) + "].class_name",
				Message: "workflow binding " + b.Name + " requires class_name (the exported WorkflowEntrypoint class)",
			})
		}
	}

	for _, f := range in.UnknownFields {
		res.Errors = append(res.Errors, Finding{
			Severity: SeverityError, Code: "unknown_field", FieldPath: f,
			Message: "unknown configuration field " + f,
		})
	}

	return res
}

// AllowedMigrationKeys are the DO migration fields the platform supports
// (ADR-014): only class creation. Renames/deletes/transfers change storage
// identity and are rejected.
var AllowedMigrationKeys = map[string]bool{
	"tag":                true,
	"new_classes":        true,
	"new_sqlite_classes": true,
}

// RejectedMigrationKeys change Durable Object storage identity in ways the
// platform cannot represent. Empty today; kept as the explicit fail-closed list.
var RejectedMigrationKeys = map[string]bool{}

// SupportedMigrationKeys are accepted (creation + rename + delete + same-worker
// transfer, ADR-082/084).
var SupportedMigrationKeys = map[string]bool{
	"renamed_classes":     true,
	"deleted_classes":     true,
	"transferred_classes": true,
}

// ValidateMigrations checks wrangler `migrations` entries (ADR-081). Each entry
// is a map so unknown keys are visible rather than silently dropped.
func ValidateMigrations(entries []map[string]any) []Finding {
	var out []Finding
	for i, e := range entries {
		for k := range e {
			path := "migrations[" + strconv.Itoa(i) + "]." + k
			switch {
			case AllowedMigrationKeys[k]:
			case SupportedMigrationKeys[k]:
				if err := checkMigrationShape(k, e[k], path); err != "" {
					out = append(out, Finding{Severity: SeverityError, Code: "invalid_migration", FieldPath: path, Message: err})
				}
			case RejectedMigrationKeys[k]:
				out = append(out, Finding{
					Severity: SeverityError, Code: "unsupported_migration", FieldPath: path,
					Message: "migration " + k + " is not supported: it changes Durable Object storage identity",
				})
			default:
				out = append(out, Finding{
					Severity: SeverityError, Code: "unknown_migration_field", FieldPath: path,
					Message: "unknown migration field " + k,
				})
			}
		}
	}
	return out
}

// ValidateCrons checks that every cron expression parses as a 5-field UTC
// expression (ADR-070/076). Without this an invalid expression would be accepted
// at deploy and then silently skipped by the scheduler.
func ValidateCrons(exprs []string) []Finding {
	var out []Finding
	for i, expr := range exprs {
		if _, err := cron.Parse(strings.TrimSpace(expr)); err != nil {
			out = append(out, Finding{
				Severity:  SeverityError,
				Code:      "invalid_cron",
				FieldPath: "crons[" + strconv.Itoa(i) + "]",
				Message:   "invalid cron expression " + strconv.Quote(expr) + ": " + err.Error(),
			})
		}
	}
	return out
}

// checkMigrationShape returns a non-empty message when a supported migration's
// value is malformed (ADR-082).
func checkMigrationShape(key string, v any, path string) string {
	list, ok := v.([]any)
	if !ok {
		return key + " must be an array"
	}
	switch key {
	case "renamed_classes":
		for i, item := range list {
			m, ok := item.(map[string]any)
			if !ok || m["from"] == nil || m["to"] == nil {
				return "renamed_classes[" + strconv.Itoa(i) + "] must be {from,to}"
			}
		}
	case "deleted_classes":
		for i, item := range list {
			if _, ok := item.(string); !ok {
				return "deleted_classes[" + strconv.Itoa(i) + "] must be a string"
			}
		}
	case "transferred_classes":
		for i, item := range list {
			m, ok := item.(map[string]any)
			if !ok || m["from"] == nil || m["to"] == nil {
				return "transferred_classes[" + strconv.Itoa(i) + "] must be {from,to}"
			}
			if sn, _ := m["script_name"].(string); sn != "" {
				return "transferred_classes[" + strconv.Itoa(i) + "] with script_name (cross-worker) is not supported; transfer within the same worker only"
			}
		}
	}
	return ""
}
