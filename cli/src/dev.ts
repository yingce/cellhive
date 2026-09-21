import { existsSync, mkdirSync, rmSync, watch } from "node:fs";
import { join, resolve, extname, relative } from "node:path";
import {
  Miniflare,
  convertV4MiniflareOptions,
  type MiniflareOptions,
  type V4ModuleDefinition,
  type V4MiniflareOptions,
  type V4SourceOptions,
  type V4WorkerOptions,
} from "miniflare";
import { loadConfig, type WranglerConfig } from "./config.ts";
import { validate, devUnsupportedBindings, PINNED_WORKERD, COMPAT_DATE_MAX, type Diagnostic } from "./validate.ts";

type Args = {
  projectDir: string;
  port: number;
  dataDir: string;
  env?: string;
  vars: Record<string, string>;
  clean: boolean;
  strictBuild: boolean;
  hotReload: boolean;
};

function parseArgs(argv: string[]): Args {
  const a: Args = {
    projectDir: process.cwd(),
    port: 8787,
    dataDir: "",
    vars: {},
    clean: false,
    strictBuild: false,
    hotReload: true,
  };
  const rest: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const x = argv[i];
    if (x === "--port") a.port = Number(argv[++i]);
    else if (x === "--data-dir") a.dataDir = argv[++i];
    else if (x === "--env") a.env = argv[++i];
    else if (x === "--clean") a.clean = true;
    else if (x === "--strict-build") a.strictBuild = true;
    else if (x === "--no-hot-reload") a.hotReload = false;
    else if (x === "--var") {
      const kv = argv[++i] ?? "";
      const eq = kv.indexOf("=");
      if (eq < 0) throw new Error(`--var expects KEY=VALUE, got "${kv}"`);
      a.vars[kv.slice(0, eq)] = kv.slice(eq + 1);
    } else if (x.startsWith("-")) throw new Error(`unknown flag ${x}`);
    else rest.push(x);
  }
  if (rest[0]) a.projectDir = resolve(rest[0]);
  if (!a.dataDir) a.dataDir = join(a.projectDir, ".cellhive-dev");
  return a;
}

function printDiagnostics(diags: Diagnostic[]): boolean {
  let fatal = false;
  for (const d of diags) {
    const tag = d.severity === "error" ? "error" : "warning";
    console.error(`  ${tag}: [${d.code}] ${d.field_path}: ${d.message}`);
    if (d.severity === "error") fatal = true;
  }
  return fatal;
}

// assetsOptions configures the native Miniflare asset service only. User-worker
// ordering is deliberately handled by devAssetWorkers' entry router because
// Miniflare 5 cannot express ADR-071 with its built-in router (ADR-114).
export function assetsOptions(
  a: WranglerConfig["assets"],
  projectDir: string,
): V4WorkerOptions["assets"] | undefined {
  if (!a) return undefined;
  const assetConfig: Record<string, unknown> = {};
  if (a.notFoundHandling) assetConfig.not_found_handling = a.notFoundHandling;
  return {
    directory: resolve(projectDir, a.directory),
    binding: a.binding,
    routerConfig: { has_user_worker: false },
    assetConfig,
  };
}

const DEV_ENTRY_WORKER = "__cellhive_dev_entry";
const DEV_ASSET_WORKER = "__cellhive_dev_assets";
const DEV_NATIVE_ASSET_BINDING = "ASSETS";

// Miniflare 5's built-in router couples user-worker fallback to not-found
// handling. This tiny dev-only worker keeps the native asset service but makes
// the ADR-071 order explicit. It runs in the same workerd instance and adds no
// listener, process, credentials, or persistent state.
const DEV_ASSET_ROUTER_SOURCE = `
function workerFirst(config, pathname) {
  if (config.runWorkerFirst) return true;
  for (const pattern of config.runWorkerFirstPaths || []) {
    if (!pattern) continue;
    if (pattern.endsWith("*") ? pathname.startsWith(pattern.slice(0, -1))
      : pathname === pattern || pathname.startsWith(pattern + "/")) return true;
  }
  return false;
}

function asset404IsFinal(config) {
  return config.notFoundHandling === "404-page" && config.has404Page;
}

export default {
  async fetch(request, env) {
    if (request.method !== "GET" && request.method !== "HEAD") {
      return env.USER_WORKER.fetch(request);
    }
    const pathname = new URL(request.url).pathname;
    if (workerFirst(env.ROUTING, pathname)) {
      const workerResponse = await env.USER_WORKER.fetch(request);
      if (workerResponse.status !== 404) return workerResponse;
      const assetResponse = await env.ASSET_WORKER.fetch(request);
      if (assetResponse.status !== 404 || asset404IsFinal(env.ROUTING)) return assetResponse;
      return workerResponse;
    }
    const assetResponse = await env.ASSET_WORKER.fetch(request);
    if (assetResponse.status !== 404 || asset404IsFinal(env.ROUTING)) return assetResponse;
    return env.USER_WORKER.fetch(request);
  }
};
`;

const DEV_ASSET_SERVICE_SOURCE = `
export default { fetch(request, env) { return env.${DEV_NATIVE_ASSET_BINDING}.fetch(request); } };
`;

export function devAssetWorkers(
  userWorker: V4WorkerOptions,
  assets: NonNullable<WranglerConfig["assets"]>,
  projectDir: string,
): V4WorkerOptions[] {
  const userName = userWorker.name || "worker";
  const compatibilityDate = userWorker.compatibilityDate ?? COMPAT_DATE_MAX;
  const assetBinding = assets.binding;
  const user = assetBinding
    ? {
        ...userWorker,
        serviceBindings: {
          ...userWorker.serviceBindings,
          [assetBinding]: DEV_ASSET_WORKER,
        },
      }
    : userWorker;
  const routing = {
    runWorkerFirst: Boolean(assets.invokeUserWorkerAhead && !assets.runWorkerFirstPaths?.length),
    runWorkerFirstPaths: assets.runWorkerFirstPaths ?? [],
    notFoundHandling: assets.notFoundHandling ?? "none",
    has404Page: existsSync(join(resolve(projectDir, assets.directory), "404.html")),
  };
  return [
    {
      name: DEV_ENTRY_WORKER,
      modules: true,
      script: DEV_ASSET_ROUTER_SOURCE,
      compatibilityDate,
      bindings: { ROUTING: routing },
      serviceBindings: { ASSET_WORKER: DEV_ASSET_WORKER, USER_WORKER: userName },
    },
    user,
    {
      name: DEV_ASSET_WORKER,
      modules: true,
      script: DEV_ASSET_SERVICE_SOURCE,
      compatibilityDate,
      assets: {
        ...assetsOptions(assets, projectDir)!,
        binding: DEV_NATIVE_ASSET_BINDING,
      },
    },
  ];
}

function describeBindings(cfg: WranglerConfig, extraVars: Record<string, string>): string {
  return [
    ...cfg.kvNamespaces.map((n) => `KV(${n})`),
    ...cfg.d1Databases.map((n) => `D1(${n})`),
    ...cfg.r2Buckets.map((n) => `R2(${n})`),
    ...cfg.hyperdrives.map((h) => `Hyperdrive(${h.binding})`),
    ...cfg.durableObjects.map((n) => `DO(${n.name})`),
    ...Object.keys(cfg.queueProducers).map((n) => `Queue(${n}->${cfg.queueProducers[n]})`),
    ...cfg.queueConsumers.map((c) => `consumer(${c.queue})`),
    ...(cfg.assets ? [`assets(${cfg.assets.directory})`] : []),
    ...Object.keys({ ...cfg.vars, ...extraVars }).map((n) => `var(${n})`),
  ].join(" ");
}

// devHyperdrives resolves hyperdrive bindings for local dev: an explicit
// localConnectionString wins; otherwise an inline URL id is used. A resource id
// (origin URL sealed in the control plane) cannot be resolved offline, so the
// binding is skipped in dev rather than pointed at a non-URL.
function devHyperdrives(list: WranglerConfig["hyperdrives"]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const h of list) {
    const cs = h.localConnectionString || (h.id && h.id.includes("://") ? h.id : "");
    if (cs) {
      out[h.binding] = cs;
    } else {
      console.warn(
        `hyperdrive(${h.binding}): no localConnectionString; binding skipped in dev ` +
          "(the registered resource's origin URL lives in the control plane)",
      );
    }
  }
  return out;
}

async function bundleIfNeeded(
  cfg: WranglerConfig,
  projectDir: string,
  strictBuild: boolean,
): Promise<{ scriptPath?: string; script?: string }> {
  const abs = resolve(projectDir, cfg.main);
  const ext = extname(cfg.main).toLowerCase();
  if (strictBuild) return bundleStrict(cfg, projectDir, abs);
  if (ext === ".js" || ext === ".mjs" || ext === ".cjs") {
    // workerd requires the entry inside the working dir (no ".."), so dev runs from projectDir.
    return { scriptPath: cfg.main };
  }
  const built = await Bun.build({ entrypoints: [abs], target: "browser", format: "esm" });
  if (!built.success) {
    throw new Error("bundling failed:\n" + built.logs.map((l) => String(l)).join("\n"));
  }
  return { script: await built.outputs[0].text() };
}

async function sourceOptions(
  entry: { scriptPath?: string; script?: string },
  cfg: WranglerConfig,
  projectDir: string,
): Promise<V4SourceOptions> {
  if (cfg.rules.length === 0) return { modules: true, ...entry };

  // Miniflare 5's v4 compatibility converter cannot translate modulesRules.
  // Materialise the declared additional modules instead, preserving their
  // type and keeping the entry module first (which defines mainModule).
  const entryPath = entry.scriptPath ?? "__cellhive_dev_entry.mjs";
  const modules: V4ModuleDefinition[] = [
    { type: "ESModule", path: entryPath, ...(entry.script === undefined ? {} : { contents: entry.script }) },
  ];
  const seen = new Set([resolve(projectDir, entryPath)]);
  for (const rule of cfg.rules) {
    for (const pattern of rule.globs) {
      const glob = new Bun.Glob(pattern);
      for await (const path of glob.scan({ cwd: projectDir, onlyFiles: true })) {
        const absolute = resolve(projectDir, path);
        if (seen.has(absolute)) continue;
        seen.add(absolute);
        modules.push({ type: rule.type as V4ModuleDefinition["type"], path });
      }
    }
  }
  return { modules, modulesRoot: projectDir };
}

// bundleStrict uses the platform Go+esbuild bundler (ADR-005) so dev and deploy
// produce the same artifact. Requires the `cellhive` Go binary.
async function bundleStrict(
  cfg: WranglerConfig,
  projectDir: string,
  entry: string,
): Promise<{ scriptPath: string }> {
  const bin =
    process.env.CELLHIVE_BIN ||
    Bun.which("cellhive") ||
    join(import.meta.dir, "..", "..", "bin", "cellhive");
  if (!bin || !existsSync(bin)) {
    throw new Error(
      "--strict-build needs the platform bundler: set CELLHIVE_BIN or build it (" +
        "`make build` produces bin/cellhive)",
    );
  }
  const out = join(projectDir, ".cellhive-dev", "strict-bundle.js");
  mkdirSync(join(projectDir, ".cellhive-dev"), { recursive: true });
  const cmd = [bin, "bundle", "build", entry, "--out", out];
  if (cfg.compatibilityFlags.includes("nodejs_compat") || cfg.compatibilityFlags.includes("nodejs_compat_v2")) {
    cmd.push("--nodejs-compat");
  }
  for (const r of cfg.rules) for (const g of r.globs) cmd.push("--rule", `${r.type}:${g}`);
  const res = Bun.spawnSync(cmd, { stdout: "pipe", stderr: "pipe" });
  if (res.exitCode !== 0) {
    throw new Error(`strict bundle failed: ${res.stderr.toString().trim() || res.stdout.toString().trim()}`);
  }
  return { scriptPath: relative(projectDir, out) };
}

export async function buildOptions(args: Args): Promise<{ options: MiniflareOptions; cfg: WranglerConfig }> {
  const cfg = loadConfig(args.projectDir, args.env);

  if (cfg.assets) {
    const dir = resolve(args.projectDir, cfg.assets.directory);
    if (!existsSync(dir)) throw new Error(`assets directory not found: ${dir}`);
    console.error(
      "  assets: serving " +
        dir +
        " with _headers/_redirects + not_found_handling and worker fallback on asset misses (ADR-069/071 parity)",
    );
  }
  const entry = await bundleIfNeeded(cfg, args.projectDir, args.strictBuild);
  const source = await sourceOptions(entry, cfg, args.projectDir);
  const userWorker: V4WorkerOptions = {
    name: cfg.name,
    rootPath: args.projectDir,
    ...source,
    compatibilityDate: cfg.compatibilityDate ?? COMPAT_DATE_MAX,
    compatibilityFlags: cfg.compatibilityFlags,
    bindings: { ...cfg.vars, ...args.vars },
    kvNamespaces: cfg.kvNamespaces,
    d1Databases: cfg.d1Databases,
    r2Buckets: cfg.r2Buckets,
    hyperdrives: devHyperdrives(cfg.hyperdrives),
    workflows: Object.fromEntries(cfg.workflows.map((w) => [w.binding, { name: w.name, className: w.className }])),
    ...(cfg.ai ? { ai: cfg.ai } : {}),
    durableObjects: Object.fromEntries(
      // useSQLite mirrors production (ADR-077/081 facet SQLite storage).
      cfg.durableObjects.map((d) => [d.name, { className: d.className, useSQLite: true }]),
    ),
    queueProducers: cfg.queueProducers,
    queueConsumers: Object.fromEntries(
      cfg.queueConsumers.map((c) => [
        c.queue,
        { maxBatchSize: c.maxBatchSize, maxBatchTimeout: c.maxBatchTimeout },
      ]),
    ),
  };
  const legacyOptions: V4MiniflareOptions = {
    rootPath: args.projectDir,
    port: args.port,
    resourcePersistencePath: join(args.dataDir, "miniflare"),
    workers: cfg.assets ? devAssetWorkers(userWorker, cfg.assets, args.projectDir) : [userWorker],
  };
  const options = convertV4MiniflareOptions(legacyOptions);
  return { options, cfg };
}

export async function runDev(argv: string[]): Promise<void> {
  const args = parseArgs(argv);
  if (args.clean) rmSync(args.dataDir, { recursive: true, force: true });
  process.chdir(args.projectDir);
  mkdirSync(join(args.dataDir, "miniflare"), { recursive: true });

  console.log("cellhive dev — DEV (Bun + Miniflare; single worker, local simulation)");

  const { options, cfg } = await buildOptions(args);
  console.log(`worker: ${cfg.name}    namespace: ${cfg.name}    env: ${args.env ?? "default"}`);
  console.log(`bindings: ${describeBindings(cfg, args.vars) || "(none)"}`);
  if (printDiagnostics(validate(cfg))) {
    console.error("cellhive dev: refusing to start (see errors above)");
    process.exit(1);
  }
  const devUnsupported = devUnsupportedBindings(cfg.raw);
  if (devUnsupported.length > 0) {
    for (const b of devUnsupported) {
      console.error(`cellhive dev: binding "${b.kind}" is not available in dev — ${b.hint}`);
    }
    console.error("cellhive dev: refusing to start (see errors above)");
    process.exit(1);
  }

  const mf = new Miniflare(options);
  await mf.ready;
  console.log(`url:      http://localhost:${args.port}/`);
  console.log(`workerd:  pinned ${PINNED_WORKERD} (Miniflare bundled version overridden)`);
  console.log(`persist:  ${join(args.dataDir, "miniflare")}`);

  if (args.hotReload) {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const IGNORE = ["/.cellhive-dev/", "/node_modules/", "/.git/"];
    watch(args.projectDir, { recursive: true }, (_event, filename) => {
      const f = String(filename ?? "").replaceAll("\\", "/");
      if (!f) return;
      // recursive watch reports full paths on Linux; ignore our own persist/output dirs
      const full = f.startsWith("/") ? f : `${args.projectDir.replaceAll("\\", "/")}/${f}`;
      if (IGNORE.some((p) => full.includes(p))) return;
      clearTimeout(timer);
      timer = setTimeout(async () => {
        try {
          const next = await buildOptions(args);
          const diags = validate(next.cfg);
          printDiagnostics(diags);
          if (diags.some((d) => d.severity === "error")) {
            console.error("cellhive dev: reload skipped (config errors)");
            return;
          }
          await mf.setOptions(next.options);
          console.log(`reloaded ${full.replace(args.projectDir.replaceAll("\\", "/") + "/", "")} (${new Date().toLocaleTimeString()})`);
        } catch (e) {
          console.error(`reload failed: ${e instanceof Error ? e.message : String(e)}`);
        }
      }, 150);
    });
    console.log("watching: project (hot reload on change)");
  }

  const shutdown = async () => {
    await mf.dispose();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}
