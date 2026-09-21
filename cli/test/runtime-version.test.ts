import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const cliRoot = join(import.meta.dir, "..");

test("dev runtime is pinned to the approved same-date pair", () => {
  const pkg = JSON.parse(readFileSync(join(cliRoot, "package.json"), "utf8"));
  const installedWorkerd = JSON.parse(
    readFileSync(join(cliRoot, "node_modules", "workerd", "package.json"), "utf8"),
  );

  expect(pkg.dependencies.miniflare).toBe("5.20260916.0-alpha");
  expect(pkg.overrides.workerd).toBe("1.20260916.1");
  expect(installedWorkerd.version).toBe("1.20260916.1");
});
