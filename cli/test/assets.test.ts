import { expect, test } from "bun:test";
import { assetsOptions } from "../src/dev";

// These assert the wrangler assets → Miniflare mapping that makes `cellhive dev`
// behave like the production loader (ADR-069/071).

test("assets-only config keeps worker fallback enabled", () => {
  const o = assetsOptions({ directory: "public" }, "/p");
  expect(o).toEqual({
    directory: "/p/public",
    binding: undefined,
    routerConfig: { has_user_worker: true },
    assetConfig: {},
  });
});

test("run_worker_first paths become static routing (not a global worker-first)", () => {
  const o = assetsOptions(
    { directory: "public", runWorkerFirstPaths: ["/api/*"], invokeUserWorkerAhead: true },
    "/p",
  );
  expect(o!.routerConfig).toEqual({
    has_user_worker: true,
    static_routing: { user_worker: ["/api/*"] },
  });
});

test("run_worker_first: true runs the worker ahead of assets", () => {
  const o = assetsOptions({ directory: "public", invokeUserWorkerAhead: true }, "/p");
  expect(o!.routerConfig).toEqual({ has_user_worker: true, invoke_user_worker_ahead_of_assets: true });
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
