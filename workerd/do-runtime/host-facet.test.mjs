import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
import { cacheSource, sourceCache } from '../platform/lifecycle.js';

const source = readFileSync(new URL('./host.js', import.meta.url), 'utf8');
const hostSource = source.slice(source.indexOf('export class Host'), source.indexOf('async function fetchBundle'))
  .replace(/^export /gm, '');

function harness() {
  const resident = new Map();
  const aborted = [];
  const deleted = [];
  const closed = [];
  const ctx = {
    id: { name: 'host/0', toString: () => 'host-hash' },
    facets: {
      get(id) {
        if (!resident.has(id)) {
          resident.set(id, {
            fetch: async () => ({ status: 200, headers: new Headers(), body: null }),
            __chAlarmState: async () => ({ alarmId: 'alarm' }),
            __chCloseAll: async (code) => { closed.push([id, code]); return 2; },
          });
        }
        return resident.get(id);
      },
      abort(id) { aborted.push(id); resident.delete(id); },
      delete(id) { deleted.push(id); resident.delete(id); },
    },
    exports: {},
  };
  class DurableObject { constructor(context, env) { this.ctx = context; this.env = env; } }
  const context = vm.createContext({
    DurableObject, Response, Request, Headers, URL, Uint8Array, TextEncoder, TextDecoder,
    Date, setTimeout, clearTimeout, console, crypto: { randomUUID: () => 'nonce' },
    wsNet: 0, boundActors: new Set(), BUNDLE_CACHE_MAX: 64,
    sourceCache, cacheSource,
    fetchBundle: async () => 'export default {}',
    checkedWorkerGet: () => ({ getDurableObjectClass: () => class {} }),
    quotaSource: () => '', workerLimits: () => ({}),
    renderFacetModule: () => 'facet', platformConsts: () => '', buildFacetEnv: () => ({}),
    refreshLogCapture: async () => {}, tailEnabled: () => false,
    reportAlarm: async () => {}, reportDoStats: async () => {}, reportHostBinding: async () => {},
    gateSync: async () => null, budgetErrorBody: () => null,
    startSpan: () => ({}), endSpan: () => {}, flushSpans: async () => {},
    relayResponse: (res) => new Response(res.body || null, { status: res.status, headers: new Headers(res.headers) }),
  });
  vm.runInContext(hostSource + '\nglobalThis.Host=Host;', context);
  const env = { GATE_URL: '', CELL_URL: 'http://cell', NODE_ID: 'node', LOADER: {}, OUTBOUND: {} };
  const host = new context.Host(ctx, env);
  return { c: context, host, aborted, deleted, closed, resident };
}

function invocation(id, version = 1, sha = 'sha1') {
  return {
    url: 'http://host/invoke',
    json: async () => ({
      namespace: 'acme', worker: 'web', class: 'Room', id, bundle_sha: sha, version,
      request: { method: 'GET', path: '/' },
    }),
  };
}

test('facet build identity survives >1024 objects and version change aborts the old stub', async () => {
  const { host, aborted } = harness();
  for (let i = 0; i < 1025; i++) {
    const res = await host.fetch(invocation('object-' + i));
    assert.equal(res.status, 200);
  }
  assert.equal(aborted.length, 0);
  await host.fetch(invocation('object-0', 2, 'sha2'));
  assert.deepEqual(aborted, ['Room/object-0']);
});

test('abort closes a resident facet with 1012 and decrements ws accounting', async () => {
  const { c, host, closed } = harness();
  await host.fetch(invocation('target'));
  c.wsNet = 5;
  const res = await host.fetch({
    url: 'http://host/v1/do/abort',
    json: async () => ({ class: 'Room', storage_class: 'Room', id: 'target' }),
  });
  assert.equal(res.status, 200);
  assert.deepEqual(closed, [['Room/target', 1012]]);
  assert.equal(c.wsNet, 3);
});
