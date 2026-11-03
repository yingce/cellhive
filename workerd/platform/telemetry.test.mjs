import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { verifyJWT } from "./jwt.js";

const source = await readFile(new URL("./telemetry.js", import.meta.url), "utf8");
const resolved = source.replace('from "jwt.js"', `from "${new URL('./jwt.js', import.meta.url).href}"`);
const telemetry = await import(`data:text/javascript;base64,${Buffer.from(resolved).toString("base64")}`);

test("nested platform span uses the supplied parent span", () => {
  globalThis.__cellhiveTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
  const invoke = telemetry.startSpan("do.invoke");
  const gate = telemetry.startSpan("do.gate", {}, invoke);

  assert.equal(gate.trace_id, invoke.trace_id);
  assert.equal(gate.parent_span_id, invoke.span_id);
  assert.equal(
    telemetry.traceparentForSpan(gate),
    `00-${gate.trace_id}-${gate.span_id}-01`,
  );
});

test("telemetry signs fresh internal JWT per flush and never sends signing key", async () => {
  const key = Buffer.alloc(32, 7).toString("base64url");
  const requests = [];
  const originalFetch = globalThis.fetch;
  const originalNow = Date.now;
  let now = 1700000000000;
  globalThis.fetch = async (url, init) => {
    requests.push({ url, init });
    return new Response(null, { status: 204 });
  };
  Date.now = () => now;
  try {
    for (let i = 0; i < 2; i++) {
      telemetry.endSpan({ name: "test-span" });
      await telemetry.flushSpans({ CELL_URL: "http://cell/", CELL_TOKEN: key });
      now += 600000;
    }
    assert.equal(requests.length, 2);
    for (let i = 0; i < requests.length; i++) {
      const { url, init } = requests[i];
      const headers = new Headers(init.headers);
      assert.equal(url, "http://cell/v1/internal/telemetry/spans");
      assert.equal(headers.has("x-cellhive-internal-token"), false);
      assert.equal(JSON.stringify(init).includes(key), false);
      const claims = await verifyJWT(key, headers.get("authorization")?.slice(7), "token/internal", "internal", 1700000000 + i * 600);
      assert.ok(claims);
      assert.equal(claims.iat, 1700000000 + i * 600);
      assert.equal(claims.exp - claims.iat, 300);
      assert.equal(JSON.parse(init.body).spans[0].name, "test-span");
    }
    telemetry.endSpan({ name: "bad-key" });
    await telemetry.flushSpans({ CELL_URL: "http://cell", CELL_TOKEN: "legacy-secret" });
    assert.equal(requests.length, 2, "invalid signing keys must not be transmitted");
  } finally {
    globalThis.fetch = originalFetch;
    Date.now = originalNow;
  }
});
