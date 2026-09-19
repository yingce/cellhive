import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";

export type WranglerConfig = {
  path: string;
  name: string;
  main: string;
  compatibilityDate?: string;
  compatibilityFlags: string[];
  vars: Record<string, unknown>;
  kvNamespaces: string[];
  d1Databases: string[];
  r2Buckets: string[];
  hyperdrives: { binding: string; id?: string; localConnectionString?: string }[];
  workflows: { binding: string; name: string; className: string }[];
  ai?: { binding: string };
  durableObjects: { name: string; className: string }[];
  queueProducers: Record<string, string>;
  queueConsumers: { queue: string; maxBatchSize?: number; maxBatchTimeout?: number }[];
  rules: { type: string; globs: string[]; fallthrough?: boolean }[];
  assets?: {
    directory: string;
    binding?: string;
    // run_worker_first: true (worker before assets for every path) or a path list
    // (worker first only for those paths). Assets still fall back to the worker
    // on a 404 (worker+assets interop).
    invokeUserWorkerAhead?: boolean;
    runWorkerFirstPaths?: string[];
    // not_found_handling: "none" | "404-page" | "single-page-application".
    notFoundHandling?: string;
  };
  envName?: string;
  raw: Record<string, unknown>;
};

const CONFIG_FILES = ["wrangler.jsonc", "wrangler.json", "wrangler.toml"];

function stripJsonComments(input: string): string {
  let out = "";
  let inString = false;
  let inLine = false;
  let inBlock = false;
  for (let i = 0; i < input.length; i++) {
    const c = input[i];
    const n = input[i + 1];
    if (inLine) {
      if (c === "\n") { inLine = false; out += c; }
      continue;
    }
    if (inBlock) {
      if (c === "*" && n === "/") { inBlock = false; i++; }
      continue;
    }
    if (inString) {
      out += c;
      if (c === "\\") { out += input[++i] ?? ""; continue; }
      if (c === '"') inString = false;
      continue;
    }
    if (c === '"') { inString = true; out += c; continue; }
    if (c === "/" && n === "/") { inLine = true; i++; continue; }
    if (c === "/" && n === "*") { inBlock = true; i++; continue; }
    out += c;
  }
  return out.replace(/,(\s*[}\]])/g, "$1");
}

function parseConfigFile(path: string): Record<string, unknown> {
  const text = readFileSync(path, "utf8");
  if (path.endsWith(".toml")) {
    return (Bun as unknown as { TOML: { parse(s: string): Record<string, unknown> } }).TOML.parse(text);
  }
  return JSON.parse(stripJsonComments(text)) as Record<string, unknown>;
}

function pickEnv(raw: Record<string, unknown>, envName?: string): Record<string, unknown> {
  if (!envName) return raw;
  const envs = raw.env as Record<string, Record<string, unknown>> | undefined;
  const e = envs?.[envName];
  if (!e) throw new Error(`environment "${envName}" not found in config`);
  return { ...raw, ...e };
}

function bindingNames(entry: unknown, key: string): string[] {
  if (!Array.isArray(entry)) return [];
  const names: string[] = [];
  for (const item of entry) {
    if (item && typeof item === "object") {
      const v = (item as Record<string, unknown>)[key];
      if (typeof v === "string") names.push(v);
    }
  }
  return names;
}

export function findConfigFile(dir: string): string | undefined {
  for (const f of CONFIG_FILES) {
    const p = join(dir, f);
    if (existsSync(p)) return p;
  }
  return undefined;
}

export function loadConfig(dir: string, envName?: string): WranglerConfig {
  const path = findConfigFile(dir);
  if (!path) throw new Error(`no wrangler config found in ${dir} (looked for ${CONFIG_FILES.join(", ")})`);
  const root = parseConfigFile(path);
  const raw = pickEnv(root, envName);

  const main = raw.main;
  if (typeof main !== "string" || !main) throw new Error(`config ${path}: "main" is required`);

  return {
    path,
    name: typeof raw.name === "string" ? raw.name : "worker",
    main,
    compatibilityDate: typeof raw.compatibility_date === "string" ? raw.compatibility_date : undefined,
    compatibilityFlags: Array.isArray(raw.compatibility_flags) ? (raw.compatibility_flags as string[]) : [],
    vars: (raw.vars as Record<string, unknown>) ?? {},
    kvNamespaces: bindingNames(raw.kv_namespaces, "binding"),
    d1Databases: bindingNames(raw.d1_databases, "binding"),
    r2Buckets: bindingNames(raw.r2_buckets, "binding"),
    hyperdrives: parseHyperdrive(raw),
    workflows: parseWorkflows(raw),
    ai: parseAI(raw),
    durableObjects: (((raw.durable_objects as { bindings?: unknown[] } | undefined)?.bindings ?? []) as unknown[])
      .filter((b): b is { name: string; class_name: string } =>
        !!b && typeof b === "object" && typeof (b as Record<string, unknown>).name === "string",
      )
      .map((b) => ({ name: b.name, className: String(b.class_name ?? b.name) })),
    queueProducers: parseQueueProducers(raw.queues),
    queueConsumers: parseQueueConsumers(raw.queues),
    rules: parseRules(raw.rules),
    assets: parseAssets(raw),
    envName,
    raw,
  };
}

function parseAssets(raw: Record<string, unknown>): WranglerConfig["assets"] {
  const a = raw.assets as Record<string, unknown> | undefined;
  if (a && typeof a.directory === "string") {
    const rw = a.run_worker_first;
    const paths = Array.isArray(rw) ? rw.filter((x): x is string => typeof x === "string") : undefined;
    const nfh = a.not_found_handling;
    return {
      directory: a.directory,
      binding: typeof a.binding === "string" ? a.binding : undefined,
      invokeUserWorkerAhead: typeof rw === "boolean" ? rw : paths !== undefined ? paths.length > 0 : undefined,
      runWorkerFirstPaths: paths && paths.length > 0 ? paths : undefined,
      notFoundHandling: nfh === "none" || nfh === "404-page" || nfh === "single-page-application" ? nfh : undefined,
    };
  }
  // Legacy [site] bucket = "./public"
  const site = raw.site as { bucket?: unknown } | undefined;
  if (site && typeof site.bucket === "string") {
    return { directory: site.bucket };
  }
  return undefined;
}

// parseHyperdrive reads `hyperdrive: [{binding, id, localConnectionString}]`. dev
// connects to localConnectionString when present (Cloudflare's local mode), else
// the id, which the platform resolves from the registered resource.
// parseHyperdrive keeps both fields: `id` names a registered hyperdrive
// resource (its origin URL lives sealed in the control plane, ADR-129), while
// `localConnectionString` is the dev-only local override. Deploy parses this in
// Go; dev resolves it locally (see dev.ts).
function parseHyperdrive(raw: Record<string, unknown>): WranglerConfig["hyperdrives"] {
  const out: WranglerConfig["hyperdrives"] = [];
  const list = (raw.hyperdrive as unknown[]) || [];
  for (const h of list) {
    if (!h || typeof h !== "object") continue;
    const o = h as Record<string, unknown>;
    const binding = typeof o.binding === "string" ? o.binding : "";
    if (!binding) continue;
    out.push({
      binding,
      id: typeof o.id === "string" ? o.id : undefined,
      localConnectionString: typeof o.localConnectionString === "string" ? o.localConnectionString : undefined,
    });
  }
  return out;
}

// parseWorkflows maps `workflows: [{binding, name, class_name}]` for dev parity.
function parseWorkflows(raw: Record<string, unknown>): WranglerConfig["workflows"] {
  const out: WranglerConfig["workflows"] = [];
  for (const w of (raw.workflows as unknown[]) || []) {
    if (!w || typeof w !== "object") continue;
    const o = w as Record<string, unknown>;
    const binding = typeof o.binding === "string" ? o.binding : "";
    const name = typeof o.name === "string" ? o.name : binding;
    const className = typeof o.class_name === "string" ? o.class_name : name;
    if (binding && name && className) out.push({ binding, name, className });
  }
  return out;
}

// parseAI maps `ai: {binding}` for dev parity.
function parseAI(raw: Record<string, unknown>): WranglerConfig["ai"] {
  const a = raw.ai as Record<string, unknown> | undefined;
  const binding = a && typeof a.binding === "string" ? a.binding : "";
  return binding ? { binding } : undefined;
}

function parseQueueProducers(queues: unknown): Record<string, string> {
  const out: Record<string, string> = {};
  const producers = (queues as { producers?: unknown[] } | undefined)?.producers;
  if (!Array.isArray(producers)) return out;
  for (const p of producers) {
    if (p && typeof p === "object") {
      const o = p as Record<string, unknown>;
      if (typeof o.binding === "string" && typeof o.queue === "string") out[o.binding] = o.queue;
    }
  }
  return out;
}

function parseQueueConsumers(queues: unknown): { queue: string; maxBatchSize?: number; maxBatchTimeout?: number }[] {
  const consumers = (queues as { consumers?: unknown[] } | undefined)?.consumers;
  if (!Array.isArray(consumers)) return [];
  const out: { queue: string; maxBatchSize?: number; maxBatchTimeout?: number }[] = [];
  for (const c of consumers) {
    if (c && typeof c === "object") {
      const o = c as Record<string, unknown>;
      if (typeof o.queue === "string") {
        out.push({
          queue: o.queue,
          maxBatchSize: typeof o.max_batch_size === "number" ? o.max_batch_size : undefined,
          maxBatchTimeout: typeof o.max_batch_timeout === "number" ? o.max_batch_timeout : undefined,
        });
      }
    }
  }
  return out;
}

function parseRules(rules: unknown): { type: string; globs: string[]; fallthrough?: boolean }[] {
  if (!Array.isArray(rules)) return [];
  const out: { type: string; globs: string[]; fallthrough?: boolean }[] = [];
  for (const r of rules) {
    if (r && typeof r === "object") {
      const o = r as Record<string, unknown>;
      if (typeof o.type === "string" && Array.isArray(o.globs)) {
        out.push({
          type: o.type,
          globs: o.globs as string[],
          fallthrough: typeof o.fallthrough === "boolean" ? o.fallthrough : undefined,
        });
      }
    }
  }
  return out;
}
