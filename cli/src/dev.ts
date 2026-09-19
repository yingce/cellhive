import { existsSync, mkdirSync, rmSync, watch } from "node:fs";
import { join, resolve, extname, relative } from "node:path";
import { Miniflare, type MiniflareOptions } from "miniflare";
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

// assetsOptions maps the wrangler `assets` config to Miniflare's asset plugin so
// dev behaves like the production loader (ADR-069/071): `_headers`/`_redirects`
// are read from the directory, `not_found_handling` is applied, and asset misses
// fall back to the user worker (has_user_worker). `run_worker_first` (bool or
// path list) routes those requests to the worker first.
export function assetsOptions(
  a: WranglerConfig["assets"],
  projectDir: string,
): NonNullable<MiniflareOptions["assets"]> | undefined {
  if (!a) return undefined;
  const routerConfig: Record<string, unknown> = { has_user_worker: true };
  if (a.runWorkerFirstPaths && a.runWorkerFirstPaths.length > 0) {
    // Path list: only those paths run the worker first; all others are served
    // from assets (falling back to the worker on a miss). Setting the global
    // invoke_user_worker_ahead_of_assets here would be wrong.
    routerConfig.static_routing = { user_worker: a.runWorkerFirstPaths };
  } else if (a.invokeUserWorkerAhead) {
    routerConfig.invoke_user_worker_ahead_of_assets = true;
  }
  const assetConfig: Record<string, unknown> = {};
  if (a.notFoundHandling) assetConfig.not_found_handling = a.notFoundHandling;
  return {
    directory: resolve(projectDir, a.directory),
    binding: a.binding,
    routerConfig: routerConfig as NonNullable<MiniflareOptions["assets"]>["routerConfig"],
    assetConfig: assetConfig as NonNullable<MiniflareOptions["assets"]>["assetConfig"],
  };
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

async function buildOptions(args: Args): Promise<{ options: MiniflareOptions; cfg: WranglerConfig }> {
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
  const options: MiniflareOptions = {
    modules: true,
    ...entry,
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
    modulesRules: cfg.rules.map((r) => ({
      type: r.type as "ESModule" | "CommonJS" | "Text" | "Data" | "CompiledWasm",
      include: r.globs,
      fallthrough: r.fallthrough,
    })),
    ...(assetsOptions(cfg.assets, args.projectDir)
      ? { assets: assetsOptions(cfg.assets, args.projectDir) }
      : {}),
    port: args.port,
    defaultPersistRoot: join(args.dataDir, "miniflare"),
  };
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
