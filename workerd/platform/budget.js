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
  total = addSize(total, input.fixedInjectedBytes || 0);
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

function v8StringExtraBytes(value) {
  let wide = false;
  for (let i = 0; i < value.length; i++) {
    if (value.charCodeAt(i) > 0xff) {
      wide = true;
      break;
    }
  }
  return wide ? Math.max(0, (value.length * 2) - byteLength(value)) : 0;
}

// Walk iteratively so validation and V8 string accounting are one explicit
// pass. Exit markers make the WeakSet track only ancestors, allowing repeated
// references while still rejecting cycles before JSON.stringify sees them.
function inspectEnv(value) {
  let extraBytes = 0;
  const ancestors = new WeakSet();
  const stack = [{ value, exit: false }];
  while (stack.length > 0) {
    const frame = stack.pop();
    const current = frame.value;
    if (frame.exit) {
      ancestors.delete(current);
      continue;
    }
    if (current === null || typeof current === "boolean") continue;
    if (typeof current === "string") {
      extraBytes += v8StringExtraBytes(current);
      continue;
    }
    if (typeof current === "number") {
      if (!Number.isFinite(current)) throw new TypeError("worker env contains a non-finite number");
      continue;
    }
    if (typeof current !== "object") {
      throw new TypeError(`worker env contains unsupported ${typeof current}`);
    }
    if (ancestors.has(current)) throw new TypeError("worker env contains a cycle");
    if (Object.getOwnPropertySymbols(current).length > 0) {
      throw new TypeError("worker env contains a symbol key");
    }
    ancestors.add(current);
    stack.push({ value: current, exit: true });
    const keys = Object.keys(current);
    for (let i = keys.length - 1; i >= 0; i--) {
      const key = keys[i];
      extraBytes += v8StringExtraBytes(key);
      stack.push({ value: current[key], exit: false });
    }
  }
  return extraBytes;
}

export function estimateEnv(value) {
  const extraBytes = inspectEnv(value);
  const json = JSON.stringify(value);
  if (json === undefined) throw new TypeError("worker env is not JSON serializable");
  return byteLength(json) + extraBytes;
}

export function checkEnv(value) {
  const actual = estimateEnv(value);
  if (actual > ENV_MAX_BYTES) {
    throw new LimitError(ENV_TOO_LARGE, actual, ENV_MAX_BYTES);
  }
  return actual;
}

export const assertWorkerEnvBudget = checkEnv;

export function estimatedWorkerEnv(vars, specs) {
  const env = Object.assign({}, vars || {});
  for (const [name, spec] of Object.entries(specs || {})) {
    if (!spec || typeof spec !== "object") continue;
    env[name] = {
      __cellhiveBinding: typeof spec.kind === "string" ? spec.kind : "",
      props: Object.assign({}, spec),
    };
  }
  return env;
}

export function assertEstimatedWorkerEnvBudget(vars, specs) {
  return assertWorkerEnvBudget(estimatedWorkerEnv(vars, specs));
}

const failureCounts = {
  user: { code: 0, env: 0 },
  do: { code: 0, env: 0 },
};

export function checkedWorkerGet(loader, id, workerCode, vars, specs, surface = "user") {
  const target = surface === "do" ? failureCounts.do : failureCounts.user;
  try {
    assertWorkerCodeBudget(workerCode);
  } catch (error) {
    if (error instanceof LimitError) target.code++;
    throw error;
  }
  try {
    assertEstimatedWorkerEnvBudget(vars, specs);
  } catch (error) {
    if (error instanceof LimitError) target.env++;
    throw error;
  }
  return loader.get(id, () => workerCode);
}

export function budgetFailureCounts() {
  return {
    user: { ...failureCounts.user },
    do: { ...failureCounts.do },
  };
}

export function budgetErrorBody(error) {
  if (error instanceof LimitError) {
    return {
      error: error.code,
      actual_bytes: error.actual,
      max_bytes: error.max,
    };
  }

  // workerd preserves the exact error string, but not custom Error fields,
  // when a facet-construction failure crosses the Durable Object boundary.
  // Accept only our canonical LimitError form and revalidate every number so
  // arbitrary tenant errors cannot add fields to the platform response.
  const match = /^LimitError: (worker_(code|env)_too_large): ([0-9]+) bytes exceeds ([0-9]+)-byte limit$/.exec(String(error));
  if (!match) return null;
  const actual = Number(match[3]);
  const max = Number(match[4]);
  const expectedMax = match[2] === "code" ? CODE_MAX_BYTES : ENV_MAX_BYTES;
  if (!Number.isSafeInteger(actual) || actual <= max || max !== expectedMax) return null;
  return {
    error: match[1],
    actual_bytes: actual,
    max_bytes: max,
  };
}
