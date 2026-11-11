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

 test("telemetry consumes the collector response", async () => {
 const old = globalThis.fetch;
 let consumed = false;
 globalThis.fetch = async () => ({ body: {getReader: () => ({read: async () => { consumed = true; return {done:true}; }, releaseLock(){}, cancel:async()=>{}})} });
 try {
  telemetry.endSpan({name: "release"});
  await telemetry.flushSpans({CELL_URL: "http://cell", CELL_TOKEN: Buffer.alloc(32, 7).toString("base64url")});
  assert.equal(consumed, true);
 } finally { globalThis.fetch = old; }
});

test('collector saturation drops telemetry without queuing network requests', async () => {
  const old = globalThis.fetch;
  const releases = [];
  globalThis.fetch = () => new Promise(resolve => releases.push(() => resolve(new Response(null, {status:204}))));
  try {
    const pending = Array.from({length: 16}, () => telemetry.postTelemetry('http://collector', {}));
    assert.equal(releases.length, 16);
    assert.equal(await telemetry.postTelemetry('http://collector', {}), null);
    assert.equal(releases.length, 16);
    releases.forEach(release => release());
    await Promise.all(pending);
    globalThis.fetch = async () => new Response(null, {status:204});
    assert.equal((await telemetry.postTelemetry('http://collector', {})).status, 204);
  } finally { releases.forEach(release => release()); globalThis.fetch = old; }
});

test('collector oversized response is cancelled and releases export admission', async () => {
  const old = globalThis.fetch;
  let cancelled = false;
  globalThis.fetch = async () => new Response(new ReadableStream({
    start(c) { c.enqueue(new Uint8Array(65537)); },
    cancel() { cancelled = true; },
  }));
  try {
    await assert.rejects(telemetry.postTelemetry('http://collector', {}), /response too large/);
    assert.equal(cancelled, true);
  } finally { globalThis.fetch = old; }
});

test('collector deadline covers both missing headers and stalled response bodies', async () => {
  const old = globalThis.fetch;
  try {
    for (const stage of ['headers', 'body']) {
      let aborted = false;
      globalThis.fetch = async (_url, {signal}) => {
        if (stage === 'headers') return new Promise((resolve, reject) => {
          signal.addEventListener('abort', () => { aborted = true; reject(signal.reason); }, {once:true});
        });
        return new Response(new ReadableStream({start(c) {
          signal.addEventListener('abort', () => { aborted = true; c.error(signal.reason); }, {once:true});
        }}));
      };
      await assert.rejects(telemetry.postTelemetry('http://collector', {}), /timeout/);
      assert.equal(aborted, true, stage);
    }
  } finally { globalThis.fetch = old; }
});

test('file log framing preserves attribution without network export', () => {
 const old=console.log,lines=[];console.log=s=>lines.push(s);
 try {
  assert.equal(telemetry.fileLog({}, {}, 'ignored'), false);
  assert.equal(telemetry.fileLog({FILE_LOG_TOKEN:'host-secret'}, {'cellhive.namespace':'n'}, 'hello\nworld','info',123), true);
  const prefix='@cellhive-log:host-secret ';
  assert.ok(lines[0].startsWith(prefix));
  const record=JSON.parse(lines[0].slice(prefix.length));
  assert.equal(record.attributes['cellhive.namespace'],'n');assert.equal(record.message,'hello\nworld');
  assert.equal(JSON.stringify(record).includes('host-secret'),false);
  telemetry.fileLog({FILE_LOG_TOKEN:'host-secret'}, {}, 'x'.repeat(100000));assert.equal(lines.length,2);assert.equal(JSON.parse(lines[1].slice(prefix.length)).kind,'drop');
 } finally { console.log=old; }
});


test("host spans use local trusted pipe without fetch", async () => {
  const originalLog=console.log,originalFetch=globalThis.fetch;
  const lines=[];
  console.log=(line)=>lines.push(line);
  globalThis.fetch=()=>{throw new Error("file mode must not fetch")};
  try {
    const span={name:"local-span",trace_id:"a".repeat(32),span_id:"b".repeat(16)};
    telemetry.endSpan(span);
    await telemetry.flushSpans({FILE_LOG_TOKEN:"host-token",CELL_URL:"http://agent"});
    assert.equal(lines.length,1);
    assert.ok(lines[0].startsWith("@cellhive-log:host-token "));
    const record=JSON.parse(lines[0].slice("@cellhive-log:host-token ".length));
    assert.equal(record.kind,"span");assert.equal(record.span.trace_id,span.trace_id);
    await telemetry.flushSpans({FILE_LOG_TOKEN:"host-token"});assert.equal(lines.length,1);
  } finally { console.log=originalLog;globalThis.fetch=originalFetch; }
});
