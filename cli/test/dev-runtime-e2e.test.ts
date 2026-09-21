import { expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Miniflare } from "miniflare";
import { buildOptions } from "../src/dev";

test("Miniflare 5 runs module fetch plus KV, D1, and R2 through translated config", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cellhive-dev-runtime-"));
  writeFileSync(
    join(dir, "wrangler.jsonc"),
    JSON.stringify({
      name: "runtime-smoke",
      main: "index.js",
      compatibility_date: "2026-09-21",
      kv_namespaces: [{ binding: "KV", id: "kv" }],
      d1_databases: [{ binding: "DB", database_id: "db" }],
      r2_buckets: [{ binding: "BUCKET", bucket_name: "bucket" }],
    }),
  );
  writeFileSync(
    join(dir, "index.js"),
    `export default { async fetch(request, env) {
      const path = new URL(request.url).pathname;
      if (path === "/module") return new Response("module-ok");
      if (path === "/kv") { await env.KV.put("k", "kv-ok"); return new Response(await env.KV.get("k")); }
      if (path === "/d1") {
        await env.DB.exec("CREATE TABLE IF NOT EXISTS t (v TEXT)");
        await env.DB.prepare("INSERT INTO t(v) VALUES (?)").bind("d1-ok").run();
        return Response.json(await env.DB.prepare("SELECT v FROM t ORDER BY rowid DESC LIMIT 1").first());
      }
      if (path === "/r2") { await env.BUCKET.put("k", "r2-ok"); return new Response(await (await env.BUCKET.get("k")).text()); }
      return new Response("missing", { status: 404 });
    } };`,
  );
  const { options } = await buildOptions({
    projectDir: dir,
    port: 0,
    dataDir: join(dir, ".cellhive-dev"),
    vars: {},
    clean: false,
    strictBuild: false,
    hotReload: false,
  });
  const mf = new Miniflare(options);
  try {
    expect(await (await mf.dispatchFetch("http://dev.test/module")).text()).toBe("module-ok");
    expect(await (await mf.dispatchFetch("http://dev.test/kv")).text()).toBe("kv-ok");
    expect(await (await mf.dispatchFetch("http://dev.test/d1")).json()).toEqual({ v: "d1-ok" });
    expect(await (await mf.dispatchFetch("http://dev.test/r2")).text()).toBe("r2-ok");
  } finally {
    await mf.dispose();
  }
});

test("Miniflare 5 materialises wrangler text module rules", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cellhive-dev-rules-"));
  writeFileSync(join(dir, "message.txt"), "rules-ok");
  writeFileSync(
    join(dir, "index.js"),
    `import message from "./message.txt"; export default { fetch() { return new Response(message); } };`,
  );
  writeFileSync(
    join(dir, "wrangler.jsonc"),
    JSON.stringify({
      name: "rules-smoke",
      main: "index.js",
      compatibility_date: "2026-09-21",
      rules: [{ type: "Text", globs: ["**/*.txt"] }],
    }),
  );
  const { options } = await buildOptions({
    projectDir: dir,
    port: 0,
    dataDir: join(dir, ".cellhive-dev"),
    vars: {},
    clean: false,
    strictBuild: false,
    hotReload: false,
  });
  const mf = new Miniflare(options);
  try {
    expect(await (await mf.dispatchFetch("http://dev.test/")).text()).toBe("rules-ok");
  } finally {
    await mf.dispose();
  }
});

test("setOptions hot reload keeps the dev-only asset router topology", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cellhive-dev-hot-assets-"));
  const publicDir = join(dir, "public");
  mkdirSync(publicDir);
  writeFileSync(join(publicDir, "index.html"), "asset");
  writeFileSync(
    join(dir, "wrangler.jsonc"),
    JSON.stringify({
      name: "hot-assets",
      main: "index.js",
      compatibility_date: "2026-09-21",
      assets: { directory: "public", run_worker_first: ["/api/*"] },
    }),
  );
  const writeWorker = (version: string) =>
    writeFileSync(
      join(dir, "index.js"),
      `export default { fetch() { return new Response(${JSON.stringify(version)}); } };`,
    );
  const args = {
    projectDir: dir,
    port: 0,
    dataDir: join(dir, ".cellhive-dev"),
    vars: {},
    clean: false,
    strictBuild: false,
    hotReload: false,
  };
  writeWorker("v1");
  const mf = new Miniflare((await buildOptions(args)).options);
  try {
    expect(await (await mf.dispatchFetch("http://dev.test/api/value")).text()).toBe("v1");
    writeWorker("v2");
    await mf.setOptions((await buildOptions(args)).options);
    expect(await (await mf.dispatchFetch("http://dev.test/api/value")).text()).toBe("v2");
  } finally {
    await mf.dispose();
  }
});
