import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
import { CountLRU, cacheSource, sourceCache } from '../platform/lifecycle.js';

const source = readFileSync(new URL('./host.js', import.meta.url), 'utf8');
const hostSource = source.slice(source.indexOf('export class Host'), source.indexOf('async function fetchBundle'))
  .replace(/^export /gm, '');

function harness(opts = {}) {
  const resident = new Map();
  let admissions = 0;
  const aborted = [];
  const deleted = [];
  const closed = [];
  const ctx = {
    id: { name: 'host/0', toString: () => 'host-hash' },
    facets: {
      get(id, build) {
        if (!resident.has(id)) {
          const ready = opts.coldBuild ? Promise.resolve().then(build) : Promise.resolve();
          resident.set(id, {
            fetch: async (req) => { await ready; if (opts.onFetch) await opts.onFetch(id, req); return { status: 200, headers: new Headers(), body: null, ...(opts.socket ? { webSocket: { addEventListener() {} } } : {}) }; },
            __chAlarmState: async () => ({ alarmId: 'alarm' }),
            __chCloseAll: async (code) => { closed.push([id, code]); return 2; },
          });
        }
        return resident.get(id);
      },
      abort(id) { aborted.push(id); resident.delete(id); },
      delete(id) { deleted.push(id); resident.delete(id); },
    },
    exports: { AlarmScheduler: () => ({}) },
  };
  class DurableObject { constructor(context, env) { this.ctx = context; this.env = env; } }
  const context = vm.createContext({
    DurableObject, Response, Request, Headers, URL, Uint8Array, TextEncoder, TextDecoder,
    AbortSignal, Date, setTimeout, clearTimeout, console, crypto: { randomUUID: () => 'nonce' },
    wsNet: 0, boundActors: new Set(), CountLRU, FACET_CACHE_MAX: 512, FACET_IDLE_MS: opts.idle ?? 60000, FACET_SWEEP_MS: opts.sweep ?? 0, BUNDLE_CACHE_MAX: 64,
    sourceCache, cacheSource, claims: new Map(), tenantRequest: (req) => req,
    fetchBundle: async () => 'export default {}',
    internalHeaders: async () => ({}),
    fetch: async (url) => {
      if (url.endsWith('/admit')) {
        admissions++;
        await new Promise(resolve => setTimeout(resolve, 5));
        if (opts.failFirstAdmission && admissions === 1) return new Response('temporary failure', {status: 503});
        return Response.json({activation: 'same-native-activation'});
      }
      await new Promise(resolve => setTimeout(resolve, 15));
      return Response.json({bindings: {}, vars: {}});
    },
    retainLoadedWorker: () => {}, releaseLoadedWorker: () => {}, loadedWorkers: new Map(),
    LIFECYCLE_SRC: '', json: (body, status) => Response.json(body, {status}),
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
  const env = { PAGING_LIFECYCLE: opts.coldBuild ? 'true' : 'false', GATE_URL: opts.coldBuild ? 'http://gate' : '', CELL_URL: 'http://cell', NODE_ID: 'node', LOADER: {}, OUTBOUND: {} };
  const host = new context.Host(ctx, env);
  return { c: context, host, aborted, deleted, closed, resident, admissions: () => admissions };
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
  const { c, host, closed } = harness({ socket: true });
  const connect = 'http://host/v1/do/connect?namespace=acme&worker=web&class=Room&id=target&bundle_sha=sha1&version=1';
  await host.fetch(new Request(connect));
  await host.fetch(new Request(connect));
  c.wsNet = 5;
  const res = await host.fetch({
    url: 'http://host/v1/do/abort',
    json: async () => ({ class: 'Room', storage_class: 'Room', id: 'target' }),
  });
  assert.equal(res.status, 200);
  assert.deepEqual(closed, [['Room/target', 1012]]);
  assert.equal(c.wsNet, 3);
});

test('idle facets are aborted after the idle TTL when no request or socket is active', async () => {
  const { host, aborted } = harness({ idle: 1, sweep: 0 });
  await host.fetch(invocation('idle-object'));
  await new Promise((r) => setTimeout(r, 5));
  await host.fetch(invocation('other-object'));
  assert.ok(aborted.includes('Room/idle-object'));
});

test('WebSocket connect forwards verified owner identity to Host', async () => {
  let received;
  const claims=new Map([['acme/web/Alias/shard7',{epoch:9}]]);
  const c=vm.createContext({URL,Request,draining:false,SHARD_COUNT:32,fnv1a:()=>7,
    seen:new Map(),hints:new Map(),claims,wsNet:0,indexObject:async()=>{},
    ensureClaim:async()=>null,reportRouterBinding:async()=>{}});
  const start=source.indexOf('async function connect(');
  vm.runInContext(source.slice(source.indexOf('function hostIdentity('), source.indexOf('async function invoke(')) + source.slice(start,source.indexOf('// buildFacetEnv',start)),c);
  const env={NODE_ID:'verified-node',HOST:{idFromName:s=>s,get:()=>({fetch:r=>{received=r;return 'connected';}})}};
  const url=new URL('http://host/v1/do/connect?namespace=acme&worker=web&class=Room&storage_class=Alias&id=x&bundle_sha=sha&shard=999&owner_node=forged&owner_epoch=1');
  assert.equal(await c.connect(new Request(url,{headers:{Upgrade:'websocket'}}),env,url,null),'connected');
  const out=new URL(received.url);
  assert.equal(out.searchParams.get('shard'),'7');
  assert.equal(out.searchParams.get('owner_node'),'verified-node');
  assert.equal(out.searchParams.get('owner_epoch'),'9');
  assert.equal(received.headers.get('Upgrade'),'websocket');
});


test('concurrent cold requests share one admission and do not poison the native facet', async () => {
  const h = harness({coldBuild: true});
  const responses = await Promise.all(Array.from({length: 16}, () => h.host.fetch(invocation('cold'))));
  assert.ok(responses.every(r => r.status === 200));
  assert.equal(h.admissions(), 1);
  assert.equal((await h.host.fetch(invocation('cold'))).status, 200);
});

test('failed cold admission can be retried without retaining a rejected promise', async () => {
  const h = harness({coldBuild: true, failFirstAdmission: true});
  assert.equal((await h.host.fetch(invocation('retry'))).status, 503);
  assert.equal((await h.host.fetch(invocation('retry'))).status, 200);
  assert.equal(h.admissions(), 2);
});

test('version change closes old sockets and switches only the target facet', async () => {
  const h = harness({ socket: true, coldBuild: true });
  await h.host.fetch(invocation('unrelated'));
  await h.host.fetch(new Request('http://host/v1/do/connect?namespace=acme&worker=web&class=Room&id=target&bundle_sha=sha1&version=1'));
  const responses = await Promise.all(Array.from({length:16}, () => h.host.fetch(invocation('target', 2, 'sha2'))));
  assert.ok(responses.every(r => r.status === 200));
  assert.deepEqual(h.closed, [['Room/target', 1012]]);
  assert.deepEqual(h.aborted, ['Room/target']);
  assert.equal((await h.host.fetch(invocation('unrelated'))).status, 200);
  assert.equal(h.admissions(), 3);
});


test('version switch waits for executing requests while unrelated facets keep serving', async () => {
  let release, entered, hold = false;
  const started = new Promise(r => entered = r);
  const blocked = new Promise(r => release = r);
  const h = harness({coldBuild:true,onFetch:async(id) => {
    if (hold && id === 'Room/target') { hold=false; entered(); await blocked; }
  }});
  await h.host.fetch(invocation('target'));
  hold=true;
  const old = h.host.fetch(invocation('target'));
  await started;
  const next = Promise.all(Array.from({length:16},()=>h.host.fetch(invocation('target',2,'sha2'))));
  assert.equal((await h.host.fetch(invocation('unrelated'))).status,200);
  assert.deepEqual(h.aborted,[]);
  release();
  assert.equal((await old).status,200);
  assert.ok((await next).every(r=>r.status===200));
  assert.deepEqual(h.aborted,['Room/target']);
});
