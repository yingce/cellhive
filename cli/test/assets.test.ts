import { expect, test } from "bun:test";
import { assetsOptions, devAssetWorkers } from "../src/dev";

// These assert the wrangler assets → Miniflare mapping that makes `cellhive dev`
// behave like the production loader (ADR-069/071).

test("native assets config delegates worker ordering to the entry router", () => {
  const o = assetsOptions({ directory: "public" }, "/p");
  expect(o).toEqual({
    directory: "/p/public",
    binding: undefined,
    routerConfig: { has_user_worker: false },
    assetConfig: {},
  });
});

test("run_worker_first paths do not leak into Miniflare's incompatible router", () => {
  const o = assetsOptions(
    { directory: "public", runWorkerFirstPaths: ["/api/*"], invokeUserWorkerAhead: true },
    "/p",
  );
  expect(o!.routerConfig).toEqual({ has_user_worker: false });
});

test("run_worker_first true is also owned by the entry router", () => {
  const o = assetsOptions({ directory: "public", invokeUserWorkerAhead: true }, "/p");
  expect(o!.routerConfig).toEqual({ has_user_worker: false });
});

test("binding and not_found_handling pass through", () => {
  const o = assetsOptions(
    { directory: "public", binding: "ASSETS", notFoundHandling: "single-page-application" },
    "/p",
  );
  expect(o!.binding).toBe("ASSETS");
  expect(o!.assetConfig).toEqual({ not_found_handling: "single-page-application" });
});

test("no assets config returns undefined", () => {
  expect(assetsOptions(undefined, "/p")).toBeUndefined();
});

test("Miniflare 5 assets use an in-process entry router and two service bindings", () => {
  const user = {
    name: "app",
    modules: true as const,
    script: "export default { fetch() { return new Response('ok') } }",
    compatibilityDate: "2026-09-21",
  };
  const workers = devAssetWorkers(
    user,
    { directory: "public", runWorkerFirstPaths: ["/api/*"], notFoundHandling: "404-page" },
    "/project",
  );
  expect(workers).toHaveLength(3);
  expect(workers[0].serviceBindings).toEqual({
    ASSET_WORKER: "__cellhive_dev_assets",
    USER_WORKER: "app",
  });
  expect(workers[0].bindings).toEqual({
    ROUTING: {
      runWorkerFirst: false,
      runWorkerFirstPaths: ["/api/*"],
      notFoundHandling: "404-page",
      has404Page: false,
    },
  });
  expect(workers[1]).toBe(user);
  expect(workers[2].assets).toMatchObject({
    directory: "/project/public",
    binding: "ASSETS",
    routerConfig: { has_user_worker: false },
  });
});

test("a configured assets binding is exposed to user code through the native asset service", () => {
  const user = {
    name: "app",
    modules: true as const,
    script: "export default { fetch() { return new Response('ok') } }",
    compatibilityDate: "2026-09-21",
  };
  const workers = devAssetWorkers(user, { directory: "public", binding: "STATIC" }, "/project");
  expect(workers[1].serviceBindings).toEqual({ STATIC: "__cellhive_dev_assets" });
});
