import type { WranglerConfig } from "./config.ts";
import { COMPAT_DATE_MAX, KNOWN_COMPAT_FLAGS, PINNED_WORKERD } from "./workerd-compat.generated.ts";

export { COMPAT_DATE_MAX, KNOWN_COMPAT_FLAGS, PINNED_WORKERD } from "./workerd-compat.generated.ts";

// NOTE: the authoritative gate lives server-side in
// internal/wranglercompat (ADR-065). This file mirrors it for fast preflight;
// keep codes/flags in sync. The server re-validates and can reject even if dev
// allowed it (e.g. Miniflare emulating a platform-unsupported binding).
// Bindings the platform rejects (ADR-014). Miniflare may support these locally,
// so `dev` must warn and `deploy` must reject server-side (ADR-065). `ai`
// (BYO OpenAI-compatible) and `workflows` (self-built engine) ARE supported and
// are therefore not listed here.
export const UNSUPPORTED_BINDINGS: { key: string; kind: string }[] = [
  { key: "images", kind: "images" },
  { key: "browser", kind: "browser-rendering" },
  { key: "send_email", kind: "send_email" },
  { key: "secrets_store_secrets", kind: "secrets_store" },
  { key: "dispatch_namespaces", kind: "dispatch_namespaces" },
  { key: "mtls_certificates", kind: "mtls" },
  { key: "pipelines", kind: "pipelines" },
  { key: "analytics_engine_datasets", kind: "analytics_engine" },

  { key: "vpc_services", kind: "vpc" },
  { key: "vpc_networks", kind: "vpc" },
  { key: "artifacts", kind: "artifacts" },
];

export type Diagnostic = { severity: "error" | "warning"; code: string; field_path: string; message: string };

// Bindings the platform supports but `cellhive dev` cannot emulate: Miniflare
// only parses the `vectorize` binding (workerd has no vectorize service), so
// env.INDEX would be missing at runtime. Fail loudly instead of pretending
// (ADR-158).
export const DEV_UNSUPPORTED_BINDINGS: { key: string; kind: string; hint: string }[] = [
  {
    key: "vectorize",
    kind: "vectorize",
    hint:
      "deploy to a namespace and use `cellhive vectorize insert|query|info` (dev has no local index implementation)",
  },
];

export function devUnsupportedBindings(
  raw: Record<string, unknown>,
): { key: string; kind: string; hint: string }[] {
  return DEV_UNSUPPORTED_BINDINGS.filter((b) => b.key in raw);
}

export function isUnsupportedBinding(key: string): boolean {
  return UNSUPPORTED_BINDINGS.some((b) => b.key === key);
}

export function unsupportedBindingKeys(raw: Record<string, unknown>): string[] {
  return UNSUPPORTED_BINDINGS.filter((b) => b.key in raw).map((b) => b.key);
}

export function validate(cfg: WranglerConfig): Diagnostic[] {
  const d: Diagnostic[] = [];

  if (cfg.compatibilityDate && cfg.compatibilityDate > COMPAT_DATE_MAX) {
    d.push({
      severity: "error",
      code: "compat_date_too_new",
      field_path: "compatibility_date",
      message: `compatibility_date ${cfg.compatibilityDate} is newer than the platform maximum ${COMPAT_DATE_MAX}`,
    });
  }

  for (const flag of cfg.compatibilityFlags) {
    if (!KNOWN_COMPAT_FLAGS.has(flag)) {
      d.push({
        severity: "error",
        code: "unknown_flag",
        field_path: "compatibility_flags",
        message: `unknown compatibility flag "${flag}" for pinned workerd ${PINNED_WORKERD}`,
      });
    }
  }

  for (const b of UNSUPPORTED_BINDINGS) {
    if (b.key in cfg.raw) {
      d.push({
        severity: "warning",
        code: "unsupported_binding",
        field_path: b.key,
        message: `binding "${b.kind}" is not supported by the CellHive platform; deploy will be rejected (dev may still work locally)`,
      });
    }
  }

  return d;
}
