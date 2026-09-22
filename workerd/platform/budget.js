const encoder = new TextEncoder();

export const CODE_MAX_BYTES = 64 * 1024 * 1024;
export const ENV_UPSTREAM_MAX_BYTES = 1024 * 1024;
export const ENV_HEADROOM_BYTES = 8 * 1024;
export const ENV_MAX_BYTES = ENV_UPSTREAM_MAX_BYTES - ENV_HEADROOM_BYTES;

export const CODE_TOO_LARGE = "worker_code_too_large";
export const ENV_TOO_LARGE = "worker_env_too_large";

export class LimitError extends Error {
  constructor(code, actual, max) {
    super(`${code}: ${actual} bytes exceeds ${max}-byte limit`);
    this.name = "LimitError";
    this.code = code;
    this.actual = actual;
    this.max = max;
  }
}

function byteLength(value) {
  return encoder.encode(value).byteLength;
}

function addSize(total, size) {
  if (!Number.isFinite(size) || size > Number.MAX_SAFE_INTEGER - total) {
    return Number.MAX_SAFE_INTEGER;
  }
  return size > 0 ? total + size : total;
}

export function estimateCode(input) {
  let total = 0;
  total = addSize(total, byteLength(input.mainModule || ""));
  total = addSize(total, input.moduleBytes || 0);
  total = addSize(total, byteLength(input.generatedWrapper || ""));
  for (const module of input.modules || []) {
    total = addSize(total, byteLength(module.name || ""));
    total = addSize(total, byteLength(module.text || ""));
    if (module.data != null) {
      const size = ArrayBuffer.isView(module.data)
        ? module.data.byteLength
        : module.data instanceof ArrayBuffer
          ? module.data.byteLength
          : NaN;
      total = addSize(total, size);
    }
  }
  for (const source of input.fixedInjectedSources || []) {
    total = addSize(total, byteLength(source));
  }
  return total;
}

export function checkCode(input) {
  const actual = estimateCode(input);
  if (actual > CODE_MAX_BYTES) {
    throw new LimitError(CODE_TOO_LARGE, actual, CODE_MAX_BYTES);
  }
  return actual;
}

function moduleInput(name, value) {
  if (typeof value === "string") return { name, text: value };
  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
    return { name, data: value };
  }
  if (!value || typeof value !== "object") {
    throw new TypeError(`unsupported WorkerCode module ${name}`);
  }
  if (typeof value.js === "string") return { name, text: value.js };
  if (typeof value.cjs === "string") return { name, text: value.cjs };
  if (typeof value.text === "string") return { name, text: value.text };
  if (value.data instanceof ArrayBuffer || ArrayBuffer.isView(value.data)) {
    return { name, data: value.data };
  }
  if (value.wasm instanceof ArrayBuffer || ArrayBuffer.isView(value.wasm)) {
    return { name, data: value.wasm };
  }
  if (Object.hasOwn(value, "json")) {
    return { name, text: JSON.stringify(value.json) };
  }
  throw new TypeError(`unsupported WorkerCode module ${name}`);
}

export function estimateWorkerCode(workerCode) {
  if (!workerCode || typeof workerCode !== "object") {
    throw new TypeError("WorkerCode must be an object");
  }
  return estimateCode({
    mainModule: workerCode.mainModule,
    modules: Object.entries(workerCode.modules || {}).map(([name, value]) => moduleInput(name, value)),
  });
}

export function assertWorkerCodeBudget(workerCode) {
  const actual = estimateWorkerCode(workerCode);
  if (actual > CODE_MAX_BYTES) {
    throw new LimitError(CODE_TOO_LARGE, actual, CODE_MAX_BYTES);
  }
  return actual;
}

function validateJSONValue(value, ancestors) {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (Number.isFinite(value)) return;
    throw new TypeError("worker env contains a non-finite number");
  }
  if (typeof value !== "object") {
    throw new TypeError(`worker env contains unsupported ${typeof value}`);
  }
  if (ancestors.has(value)) throw new TypeError("worker env contains a cycle");
  if (Object.getOwnPropertySymbols(value).length > 0) {
    throw new TypeError("worker env contains a symbol key");
  }
  ancestors.add(value);
  for (const key of Object.keys(value)) validateJSONValue(value[key], ancestors);
  ancestors.delete(value);
}

const nonLatin1 = /[\u0100-\uffff]/;

function twoByteStringPenalty(value) {
  if (!nonLatin1.test(value)) return 0;
  return Math.max(0, (2 * value.length) - byteLength(value));
}

function envStringPenalty(value) {
  if (typeof value === "string") return twoByteStringPenalty(value);
  if (!value || typeof value !== "object") return 0;
  let total = 0;
  for (const key of Object.keys(value)) {
    total += twoByteStringPenalty(key);
    total += envStringPenalty(value[key]);
  }
  return total;
}

export function estimateEnv(value) {
  validateJSONValue(value, new WeakSet());
  const json = JSON.stringify(value);
  if (json === undefined) throw new TypeError("worker env is not JSON serializable");
  return byteLength(json) + envStringPenalty(value);
}

export function checkEnv(value) {
  const actual = estimateEnv(value);
  if (actual > ENV_MAX_BYTES) {
    throw new LimitError(ENV_TOO_LARGE, actual, ENV_MAX_BYTES);
  }
  return actual;
}

export const assertWorkerEnvBudget = checkEnv;
