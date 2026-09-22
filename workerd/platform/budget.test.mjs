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
