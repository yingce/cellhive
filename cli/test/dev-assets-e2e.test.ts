import { expect, test } from "bun:test";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Miniflare } from "miniflare";
import { assetsOptions } from "../src/dev";

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
  const worker = join(dir, "worker.mjs");
  writeFileSync(
    worker,
    "export default { fetch(req) { return Response.json({ worker: true, path: new URL(req.url).pathname }); } };\n",
  );

  const mf = new Miniflare({
    modules: true,
    modulesRoot: dir,
    scriptPath: worker,
    compatibilityDate: "2026-06-22",
    compatibilityFlags: [],
    assets: assetsOptions(
      {
        directory: "public",
        notFoundHandling: "404-page",
        runWorkerFirstPaths: ["/api/*"],
        invokeUserWorkerAhead: true,
      },
      dir,
    ),
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
  } finally {
    await mf.dispose();
  }
});
