import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = await readFile(new URL("./budget.js", import.meta.url), "utf8");
const budget = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);
const vectors = JSON.parse(await readFile(
  new URL("../../internal/workerbudget/testdata/vectors.json", import.meta.url),
  "utf8",
));

test("shared env vectors match Go estimates byte for byte", () => {
  for (const vector of vectors.env) {
    const value = vector.repeat
      ? { [vector.repeat.key]: vector.repeat.char.repeat(vector.repeat.count) }
      : vector.value;
    assert.equal(budget.estimateEnv(value), vector.want, vector.name);
    if (vector.overLimit) {
      assert.throws(
        () => budget.checkEnv(value),
        (error) => error instanceof budget.LimitError &&
          error.code === "worker_env_too_large" &&
          error.actual === vector.want && error.max === 1016 * 1024,
        vector.name,
      );
    } else {
      assert.doesNotThrow(() => budget.checkEnv(value), vector.name);
    }
  }
});

test("shared code vectors match Go estimates and boundaries", () => {
  for (const vector of vectors.code) {
    const input = {
      ...vector,
      modules: (vector.modules || []).map((module) => ({
        name: module.name,
        text: module.text || "",
        data: module.dataBase64 ? Buffer.from(module.dataBase64, "base64") : undefined,
      })),
    };
    assert.equal(budget.estimateCode(input), vector.want, vector.name);
    if (vector.ok) {
      assert.doesNotThrow(() => budget.checkCode(input), vector.name);
    } else {
      assert.throws(
        () => budget.checkCode(input),
        (error) => error instanceof budget.LimitError &&
          error.code === "worker_code_too_large" &&
          error.actual === vector.want && error.max === 64 * 1024 * 1024,
        vector.name,
      );
    }
  }
});

test("env rejects unsupported and cyclic values", () => {
  const cyclic = {};
  cyclic.self = cyclic;
  for (const value of [{ bad: () => {} }, { bad: Infinity }, cyclic]) {
    assert.throws(() => budget.estimateEnv(value));
  }
});

test("binding env estimate preserves user names and complete props", () => {
  const vars = { CELL_URL: "user-owned", plain: "value" };
  const specs = {
    KV: { kind: "kv", ns: "acme", name: "KV", token: "scope-token" },
  };
  const estimated = budget.estimatedWorkerEnv(vars, specs);
  assert.deepEqual(estimated, {
    CELL_URL: "user-owned",
    plain: "value",
    KV: {
      __cellhiveBinding: "kv",
      props: { kind: "kv", ns: "acme", name: "KV", token: "scope-token" },
    },
  });
  assert.equal(budget.assertEstimatedWorkerEnvBudget(vars, specs), budget.estimateEnv(estimated));
});

test("checkedWorkerGet never invokes loader after a code or env budget failure", () => {
  let calls = 0;
  const loader = { get() { calls++; throw new Error("loader callback must remain unreachable"); } };
  const oversizedCode = {
    mainModule: "tenant.js",
    modules: { "tenant.js": new Uint8Array(64 * 1024 * 1024) },
    env: {},
  };
  assert.throws(
    () => budget.checkedWorkerGet(loader, "code", oversizedCode, {}, {}, "user"),
    (error) => error.code === "worker_code_too_large",
  );
  const oversizedEnv = { huge: "x".repeat(1016 * 1024) };
  const smallCode = { mainModule: "tenant.js", modules: { "tenant.js": "export default {}" }, env: oversizedEnv };
  assert.throws(
    () => budget.checkedWorkerGet(loader, "env", smallCode, oversizedEnv, {}, "do"),
    (error) => error.code === "worker_env_too_large",
  );
  assert.equal(calls, 0);
  assert.deepEqual(budget.budgetFailureCounts(), {
    user: { code: 1, env: 0 },
    do: { code: 0, env: 1 },
  });
});

test("budget errors expose only bounded aggregate fields", () => {
  const error = new budget.LimitError("worker_env_too_large", 1040385, 1040384);
  assert.deepEqual(budget.budgetErrorBody(error), {
    error: "worker_env_too_large",
    actual_bytes: 1040385,
    max_bytes: 1040384,
  });
  assert.deepEqual(
    budget.budgetErrorBody({
      toString() {
        return "LimitError: worker_env_too_large: 1040395 bytes exceeds 1040384-byte limit";
      },
    }),
    {
      error: "worker_env_too_large",
      actual_bytes: 1040395,
      max_bytes: 1040384,
    },
  );
  assert.equal(budget.budgetErrorBody(new Error("unrelated")), null);
});
