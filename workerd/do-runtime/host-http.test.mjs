import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
import { decode, encode, encodedSize, MAX_DO_ENVELOPE_BYTES, MAX_DO_REQUEST_BYTES, MAX_RPC_BYTES, RPC_METHOD_RE, RPC_RESERVED_METHODS } from '../platform/rpc-codec.js';

const source = readFileSync(new URL('./host.js', import.meta.url), 'utf8');
function extract(start, end) {
  const from = source.indexOf(start);
  return source.slice(from, source.indexOf(end, from));
}

function responseHarness() {
  const c = vm.createContext({
    Response, Headers, ReadableStream, Uint8Array, TextEncoder, TextDecoder,
    MAX_DO_ENVELOPE_BYTES, MAX_DO_REQUEST_BYTES,
  });
  vm.runInContext(
    extract('function responseHeaders(', 'function ticketSpec(') +
      '\nglobalThis.relayResponse=relayResponse;globalThis.readEnvelope=readEnvelope;',
    c,
  );
  return c;
}

function invokeHarness() {
  const c = vm.createContext({
    Response, Headers, Request, URL, ReadableStream, Uint8Array, TextEncoder, TextDecoder, Date,
    draining: false, inflight: 0, fnv1a: () => 0, SHARD_COUNT: 16,
    seen: new Map(), SEEN_MAX: 100, indexObject: async () => {},
    hints: new Map(), claims: new Map(), ensureClaim: async () => null,
    reportRouterBinding: async () => {}, internalHeaders: async () => ({}),
    budgetErrorBody: () => null, ownerURL: (address) => address,
    json: (o, status) => new Response(JSON.stringify(o), {
      status: status || 200, headers: { 'content-type': 'application/json' },
    }),
    MAX_DO_ENVELOPE_BYTES, MAX_DO_REQUEST_BYTES,
  });
  vm.runInContext(
    extract('function responseHeaders(', 'async function restoreClaimedHost') +
      '\nglobalThis.invoke=invoke;globalThis.forward=forward;',
    c,
  );
  return c;
}

function rpcHarness(overrides = {}) {
  const c = vm.createContext({
    Response, Headers, decode, encode, encodedSize, MAX_RPC_BYTES, RPC_METHOD_RE, RPC_RESERVED_METHODS,
    ...overrides,
  });
  vm.runInContext(
    extract('function errMessage(', 'const WRAP_SRC =') +
      '\nglobalThis.dispatchRPC=dispatchRPC;',
    c,
  );
  return c;
}

test('relayed DO responses keep a lazy body and preserve status/headers', async () => {
  const c = responseHarness();
  let pulls = 0;
  const upstream = new Response(new ReadableStream({
    pull(stream) {
      pulls++;
      if (pulls > 2) { stream.close(); return; }
      stream.enqueue(new Uint8Array([pulls]));
    },
  }, { highWaterMark: 0 }), { headers: { 'content-length': '2', 'x-result': 'yes' } });
  const out = c.relayResponse(upstream, 'GET', false);
  assert.equal(pulls, 0);
  assert.equal(out.status, 200);
  assert.equal(out.headers.get('x-result'), 'yes');
  assert.equal(out.headers.get('content-length'), null);
  assert.deepEqual([...new Uint8Array(await out.arrayBuffer())], [1, 2]);
  assert.ok(pulls > 0);
});

test('relayed empty responses cancel the upstream body for HEAD/204/205/304', async () => {
  const c = responseHarness();
  for (const [status, method] of [[204, 'GET'], [205, 'GET'], [304, 'GET'], [200, 'HEAD']]) {
    let cancelled = false;
    const upstream = {
      status,
      headers: new Headers({ 'x-result': 'kept' }),
      body: { cancel() { cancelled = true; } },
    };
    const out = c.relayResponse(upstream, method, false);
    assert.equal(out.status, status);
    assert.equal(out.body, null);
    assert.equal(cancelled, true);
  }
});

test('tenant response relay strips internal headers only for tenant responses', () => {
  const c = responseHarness();
  const headers = new Headers({ 'x-cellhive-do-app': '1', 'x-result': 'yes', connection: 'x-hop', 'x-hop': 'remove' });
  const tenant = c.relayResponse({ status: 200, headers, body: null }, 'GET', true);
  assert.equal(tenant.headers.get('x-cellhive-do-app'), null);
  assert.equal(tenant.headers.get('x-hop'), null);
  assert.equal(tenant.headers.get('x-result'), 'yes');
  const platform = c.relayResponse({ status: 200, headers, body: null }, 'GET', false);
  assert.equal(platform.headers.get('x-cellhive-do-app'), '1');
});

test('envelope reader enforces a byte cap before parsing', async () => {
  const c = responseHarness();
  const declared = { headers: { get: () => String(MAX_DO_ENVELOPE_BYTES + 1) }, text: async () => { throw new Error('must not read'); } };
  assert.equal((await c.readEnvelope(declared)).tooLarge, true);
  const text = { headers: { get: () => null }, text: async () => 'x'.repeat(64) };
  assert.equal((await c.readEnvelope(text, 32)).tooLarge, true);
  const ok = { headers: { get: () => null }, text: async () => '{"ok":true}' };
  assert.equal(JSON.stringify((await c.readEnvelope(ok, 32)).value), '{"ok":true}');
});

test('envelope reader cancels a chunked body at max+1', async () => {
  const c = responseHarness();
  let cancelled = false;
  let reads = 0;
  const body = {
    getReader: () => ({
      read: async () => ++reads <= 2 ? { done: false, value: new Uint8Array(20) } : { done: true },
      cancel: async () => { cancelled = true; },
    }),
  };
  const req = { headers: { get: () => null }, body, text: async () => { throw new Error('must stream'); } };
  assert.equal((await c.readEnvelope(req, 32)).tooLarge, true);
  assert.equal(cancelled, true);
  assert.equal(reads, 2);
});

test('invoke returns typed 413 for an oversized JSON envelope', async () => {
  const c = invokeHarness();
  const req = {
    headers: { get: (name) => name === 'content-length' ? String(MAX_DO_ENVELOPE_BYTES + 1) : null },
    text: async () => { throw new Error('must not read'); },
    json: async () => { throw new Error('must not parse'); },
  };
  const out = await c.invoke(req, {}, null);
  assert.equal(out.status, 413, await out.clone().text());
  assert.equal((await out.json()).error, 'body_too_large');
});

test('RPC rejects oversized encoded args before decoding or dispatch', async () => {
  let decoded = false;
  let dispatched = false;
  const c = rpcHarness({
    decode: () => { decoded = true; throw new Error('must not decode'); },
    encodedSize: () => MAX_RPC_BYTES + 1,
  });
  const out = await c.dispatchRPC(new Proxy({}, { get: () => { dispatched = true; throw new Error('must not dispatch'); } }), { method: 'execute', args: { oversized: true } });
  assert.equal(out.status, 413, await out.clone().text());
  assert.equal((await out.json()).error, 'do_rpc_args_too_large');
  assert.equal(decoded, false);
  assert.equal(dispatched, false);
});

test('host facet invoke and forwarded invoke stream the response body', async () => {
  let pulls = 0;
  const upstream = new Response(new ReadableStream({
    pull(stream) {
      pulls++;
      if (pulls > 1) { stream.close(); return; }
      stream.enqueue(new Uint8Array([9, 8, 7]));
    },
  }, { highWaterMark: 0 }), { headers: { 'x-cellhive-do-app': '1' } });
  const env = {
    NODE_ID: 'node', ADVERTISE: '', CELL_TOKEN: 'key',
    HOST: { idFromName: () => 'id', get: () => ({ fetch: async () => upstream }) },
  };
  const spec = { namespace: 'acme', worker: 'web', class: 'Room', id: 'one', bundle_sha: 'sha' };
  const c = invokeHarness();
  c.claims.set('acme/web/Room/shard0', { epoch: 1, expiryMs: Date.now() + 10000 });
  const req = { headers: { get: () => null }, text: async () => JSON.stringify(spec) };
  const out = await c.invoke(req, env, null);
  assert.equal(pulls, 0);
  assert.deepEqual([...new Uint8Array(await out.arrayBuffer())], [9, 8, 7]);
  assert.equal(out.headers.get('x-cellhive-do-app'), '1');

  let forwardedPulls = 0;
  c.fetch = async () => new Response(new ReadableStream({
    pull(stream) {
      forwardedPulls++;
      if (forwardedPulls > 1) { stream.close(); return; }
      stream.enqueue(new Uint8Array([1, 2]));
    },
  }, { highWaterMark: 0 }));
  const fwd = await c.forward(env, 'http://owner', spec);
  assert.equal(forwardedPulls, 0);
  assert.deepEqual([...new Uint8Array(await fwd.arrayBuffer())], [1, 2]);
});

test('same storage and shard isolate classes while aliases retain identity', async () => {
  const c = invokeHarness();
  const hosts = [];
  const env = { NODE_ID: 'n', HOST: { idFromName: id => { hosts.push(id); return id; }, get: () => ({fetch: async () => new Response('ok')}) } };
  for (const [cls, alias] of [['SessionDO',''], ['SchedulerDO',''], ['RenamedSession','SessionDO']]) {
    const spec = {namespace:'app',worker:'web',storage_id:'sid',class:cls,storage_class:alias,id:'same',bundle_sha:'sha'};
    await c.invoke({headers:new Headers(),text:async()=>JSON.stringify(spec)},env,null);
  }
  assert.notEqual(hosts[0],hosts[1]);
  assert.equal(hosts[0],hosts[2]);
});
