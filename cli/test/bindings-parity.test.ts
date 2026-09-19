import { expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig } from "../src/config";

// Dev-mode binding parity: workflows/ai declared in wrangler.jsonc must reach
// Miniflare instead of being silently dropped (ADR-134).

function configWith(body: object): string {
  const dir = mkdtempSync(join(tmpdir(), "cellhive-dev-cfg-"));
  writeFileSync(join(dir, "wrangler.jsonc"), JSON.stringify(body));
  return dir;
}

test("workflows and ai are parsed for dev", () => {
  const dir = configWith({
    name: "app",
    main: "index.js",
    workflows: [{ binding: "WF", name: "my-wf", class_name: "MyWorkflow" }],
    ai: { binding: "AI" },
  });
  const cfg = loadConfig(dir, undefined);
  expect(cfg.workflows).toEqual([{ binding: "WF", name: "my-wf", className: "MyWorkflow" }]);
  expect(cfg.ai).toEqual({ binding: "AI" });
});

test("workflows/ai are absent by default", () => {
  const cfg = loadConfig(configWith({ name: "app", main: "index.js" }), undefined);
  expect(cfg.workflows).toEqual([]);
  expect(cfg.ai).toBeUndefined();
});

// Vectorize (ADR-158): the platform supports it, but `cellhive dev` has no
// local index implementation, so dev must refuse rather than start with a
// broken env.INDEX.
test("vectorize is a dev-unsupported binding", async () => {
  const { devUnsupportedBindings, validate } = await import("../src/validate");
  const raw = { name: "app", main: "index.js", vectorize: [{ binding: "INDEX", index_name: "docs" }] };
  const cfg = loadConfig(configWith(raw), undefined);
  expect(devUnsupportedBindings(cfg.raw).map((b) => b.kind)).toEqual(["vectorize"]);
  // It is no longer a platform-rejected binding.
  expect(validate(cfg).some((d) => d.code === "unsupported_binding")).toBe(false);
  expect(devUnsupportedBindings({ name: "app" })).toEqual([]);
});
