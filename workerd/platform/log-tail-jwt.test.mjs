import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
import { authorized, internalHeaders, mintJWT, verifyJWT } from './jwt.js';
import { webcrypto } from 'node:crypto';

const key = Buffer.alloc(32, 8).toString('base64url');
const source = readFileSync(new URL('./log-tail.js', import.meta.url), 'utf8')
  .replace(/^import .*?;\n/gm, '').replace(/^export /gm, '');
function harness(extra = {}) {
  const context = vm.createContext({ WorkerEntrypoint: class {}, authorized, internalHeaders,
    URL, Response, TextEncoder, AbortController, setTimeout, clearTimeout, crypto: webcrypto, ...extra });
  vm.runInContext(source, context);
  return context;
}
test('tail query accepts only current log-role Bearer JWT, no static fallback', async () => {
  const context = harness();
  const now = Math.floor(Date.now() / 1000);
  const log = await mintJWT(key, 'token/log', { type: 'internal', sub: 'test' });
  const internal = await mintJWT(key, 'token/internal', { type: 'internal', sub: 'test' });
  const expired = await mintJWT(key, 'token/log', { type: 'internal', sub: 'test' }, now - 600);
  for (const [headers, status] of [
    [{ authorization: 'Bearer ' + log }, 200],
    [{ authorization: 'Bearer ' + internal }, 401],
    [{ authorization: 'Bearer ' + expired }, 401],
    [{ authorization: 'Bearer ' + key }, 401],
    [{ 'x-cellhive-internal-token': key }, 401],
    [{ authorization: 'Bearer ' + log, 'x-cellhive-scope-token': 'old' }, 401],
    [{}, 401],
  ]) assert.equal((await context.tailQuery(new Request('http://runtime/tail', { headers }), { LOG_TOKEN: key })).status, status);
  assert.equal((await context.tailQuery(new Request('http://runtime/tail', { headers: { authorization: 'Bearer ' + internal } }), { CELL_TOKEN: key })).status, 401);
});
test('log capture reads authenticate using an internal JWT, never the key', async () => {
  let headers;
  const context = harness({ fetch: async (_url, init) => { headers = new Headers(init.headers); return Response.json({ mode: 'tail', workers: ['test-worker'] }); } });
  const capture = await context.readLogCapture({ CELL_URL: 'http://agent', CELL_TOKEN: key });
  assert.equal(capture.mode, 'tail');
  assert.deepEqual(Array.from(capture.workers), ['test-worker']);
  assert.ok(await verifyJWT(key, headers.get('authorization').slice(7), 'token/internal', 'internal'));
  assert.equal(headers.has('x-cellhive-internal-token'), false);
});
