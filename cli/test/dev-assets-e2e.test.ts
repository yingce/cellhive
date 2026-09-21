import { expect, test } from "bun:test";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Miniflare, convertV4MiniflareOptions, type V4WorkerOptions } from "miniflare";
import { devAssetWorkers } from "../src/dev";

function userWorker(dir: string, source: string): V4WorkerOptions {
  return {
    name: "user",
    rootPath: dir,
    modules: true,
    script: source,
    compatibilityDate: "2026-09-21",
    compatibilityFlags: [],
  };
}

function assetsMiniflare(
  dir: string,
  source: string,
  assets: Parameters<typeof devAssetWorkers>[1],
): Miniflare {
  return new Miniflare(
    convertV4MiniflareOptions({ workers: devAssetWorkers(userWorker(dir, source), assets, dir) }),
  );
}

// End-to-end check that `cellhive dev`'s asset options reproduce the production
// loader behavior (ADR-069/071): _headers, _redirects, not_found_handling and
// run_worker_first paths all apply when serving through Miniflare.
test("dev assets pipeline matches the production loader", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cellhive-dev-assets-"));
  const pub = join(dir, "public");
  mkdirSync(pub);
  writeFileSync(join(pub, "index.html"), "<h1>index</h1>");
  writeFileSync(join(pub, "new.html"), "<h1>new</h1>");
  writeFileSync(join(pub, "404.html"), "<h1>missing</h1>");
  writeFileSync(join(pub, "asset.txt"), "asset-body");
  writeFileSync(join(pub, "_headers"), "/asset.txt\n  X-Custom: yes\n");
  writeFileSync(join(pub, "_redirects"), "/old /new.html 302\n");
  mkdirSync(join(pub, "api"));
  writeFileSync(join(pub, "api", "existing.txt"), "worker-404-asset");
  const source = `export default { fetch(req, env) {
    const path = new URL(req.url).pathname;
    if (path === "/api/existing.txt") return new Response("worker-404", { status: 404 });
    if (path === "/binding") return env.STATIC.fetch(new URL("/asset.txt", req.url));
    return Response.json({ worker: true, path });
  } };`;

  const mf = assetsMiniflare(dir, source, {
    directory: "public",
    binding: "STATIC",
    notFoundHandling: "404-page",
    runWorkerFirstPaths: ["/api/*", "/binding"],
    invokeUserWorkerAhead: true,
  });
  try {
    // redirect: "manual" so a _redirects 302 is observed as-is (fetch follows it).
    const get = (path: string) => mf.dispatchFetch("http://dev.test" + path, { redirect: "manual" });

    // Assets win when the file exists.
    const home = await get("/");
    expect(home.status).toBe(200);
    expect(await home.text()).toBe("<h1>index</h1>");

    // _headers applies to asset responses.
    const asset = await get("/asset.txt");
    expect(asset.status).toBe(200);
    expect(asset.headers.get("x-custom")).toBe("yes");
    expect(await asset.text()).toBe("asset-body");

    // _redirects returns the configured status + location.
    const red = await get("/old");
    expect(red.status).toBe(302);
    expect(red.headers.get("location")).toBe("/new.html");

    // not_found_handling: 404-page serves 404.html with status 404.
    const nf = await get("/nope");
    expect(nf.status).toBe(404);
    expect(await nf.text()).toBe("<h1>missing</h1>");

    // run_worker_first paths go to the worker.
    const api = await get("/api/hi");
    expect(api.status).toBe(200);
    expect(await api.json()).toEqual({ worker: true, path: "/api/hi" });

    // Production runs the worker first, then falls back to an existing asset
    // when the worker explicitly returns 404.
    const worker404 = await get("/api/existing.txt");
    expect(worker404.status).toBe(200);
    expect(await worker404.text()).toBe("worker-404-asset");

    const binding = await get("/binding");
    expect(binding.status).toBe(200);
    expect(await binding.text()).toBe("asset-body");
  } finally {
    await mf.dispose();
  }
});

test("asset miss falls back to the user worker when not_found_handling is none", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cellhive-dev-assets-none-"));
  mkdirSync(join(dir, "public"));
  const mf = assetsMiniflare(
    dir,
    `export default { fetch(req) { return new Response("worker:" + new URL(req.url).pathname); } };`,
    { directory: "public", notFoundHandling: "none" },
  );
  try {
    const res = await mf.dispatchFetch("http://dev.test/missing");
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("worker:/missing");
  } finally {
    await mf.dispose();
  }
});

test("single-page-application serves index instead of falling back to the worker", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cellhive-dev-assets-spa-"));
  const pub = join(dir, "public");
  mkdirSync(pub);
  writeFileSync(join(pub, "index.html"), "spa-index");
  const mf = assetsMiniflare(
    dir,
    `export default { fetch() { return new Response("worker"); } };`,
    { directory: "public", notFoundHandling: "single-page-application" },
  );
  try {
    const res = await mf.dispatchFetch("http://dev.test/client/route");
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("spa-index");
  } finally {
    await mf.dispose();
  }
});

test("run_worker_first true runs the worker before an existing asset", async () => {
  const dir = mkdtempSync(join(tmpdir(), "cellhive-dev-assets-worker-first-"));
  const pub = join(dir, "public");
  mkdirSync(pub);
  writeFileSync(join(pub, "index.html"), "asset-index");
  const mf = assetsMiniflare(
    dir,
    `export default { fetch() { return new Response("worker-first"); } };`,
    { directory: "public", invokeUserWorkerAhead: true },
  );
  try {
    const res = await mf.dispatchFetch("http://dev.test/");
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("worker-first");
  } finally {
    await mf.dispose();
  }
});
