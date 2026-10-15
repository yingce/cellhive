import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";

test("AI docs catalog and Skill bundle satisfy the local contract", () => {
  const output = execFileSync(process.execPath, ["scripts/verify-ai-docs.mjs"], {
    cwd: fileURLToPath(new URL("../..", import.meta.url)),
    encoding: "utf8"
  });
  assert.match(output, /AI docs contract: PASS/);
});
