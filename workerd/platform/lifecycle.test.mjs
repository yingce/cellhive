import test from 'node:test';
import assert from 'node:assert/strict';
import { ByteLRU, CountLRU, Lifetime, RuntimeCapacity, SOURCE_CACHE_BYTES, cacheSource, sourceCache, quotaSource, managedModules } from './lifecycle.js';

test('quota wrapper invokes binding methods without RPC apply lookup', () => {
  const source = quotaSource(10, { CACHE: { kind: 'kv' } });
  assert.match(source, /Reflect\.apply\(v,\s*target,\s*args\)/);
  assert.doesNotMatch(source, /v\.apply\(target, args\)/);
});

test('byte cache evicts by weight, updates accounting and skips oversized values', () => {
  const c = new ByteLRU(10, 12, v => v.length, 8);
  c.set('a', 'aaaa'); c.set('b', 'bbbb'); c.get('a'); c.set('c', 'cc');
  assert.equal(c.get('b'), undefined); assert.equal(c.bytes, 10);
  c.set('a', 'a'); assert.equal(c.bytes, 7);
  c.set('large', 'x'.repeat(9)); assert.equal(c.get('large'), undefined);
  c.delete('a'); assert.equal(c.bytes, 4);
});

test('retirement waits for handler, response body and background completion', async () => {
  const life = new Lifetime();
  const release = life.hold();
  let done; const pending = new Promise(r => done = r);
  const bg = life.background(pending);
  assert.equal(life.idle(), false); release(); assert.equal(life.idle(), false);
  done(); await bg; assert.equal(life.idle(), true);
  const response = await life.response(new Response(new ReadableStream({start(c){c.enqueue(new Uint8Array([1]));}})));
  assert.equal(life.idle(), false); await response.body.cancel(); assert.equal(life.idle(), true);
});

test('stream is not eagerly consumed and error releases lifetime', async () => {
  const life = new Lifetime(); let pulls=0;
  const res = await life.response(new Response(new ReadableStream({pull(c){pulls++;c.error(new Error('boom'));}}, {highWaterMark:0})));
  assert.equal(pulls,0); assert.equal(life.idle(),false);
  await assert.rejects(res.text(), /boom/); assert.equal(life.idle(),true);
});

test('small JSON body is buffered and released before the client reads it', async () => {
  const life = new Lifetime();
  const res = await life.response(new Response('{"a":1}', {headers:{'content-type':'application/json'}}));
  assert.equal(life.idle(), true);
  assert.equal(res.headers.get('content-type'), 'application/json');
  assert.equal(res.status, 200);
  assert.equal(await res.text(), '{"a":1}');
  assert.equal(life.idle(), true);
});

test('plain-text body under the cap is buffered without changing bytes or status', async () => {
  const life = new Lifetime();
  const res = await life.response(new Response('pong', {status:201, headers:{'content-type':'text/plain;charset=UTF-8'}}));
  assert.equal(life.idle(), true);
  assert.equal(res.status, 201);
  assert.equal(await res.text(), 'pong');
});

test('event-stream body stays lazy and is tracked until cancel', async () => {
  const life = new Lifetime();
  let pulls = 0;
  const stream = new ReadableStream({pull(c){pulls++;c.enqueue(new TextEncoder().encode('data: x\n\n'));}}, {highWaterMark:0});
  const res = await life.response(new Response(stream, {headers:{'content-type':'text/event-stream'}}));
  assert.equal(pulls, 0);
  assert.equal(life.idle(), false);
  await res.body.cancel();
  assert.equal(life.idle(), true);
});

test('oversized plain-text body falls back to a tracked stream', async () => {
  const life = new Lifetime();
  const chunk = new Uint8Array(32 * 1024).fill(7);
  let sent = 0;
  const stream = new ReadableStream({pull(c){
    if (sent >= 8) { c.close(); return; }
    sent++; c.enqueue(chunk.slice());
  }}, {highWaterMark:0});
  const res = await life.response(new Response(stream, {headers:{'content-type':'text/plain'}}));
  assert.equal(life.idle(), false);
  const buf = new Uint8Array(await res.arrayBuffer());
  assert.equal(buf.byteLength, 8 * chunk.byteLength);
  assert.equal(buf[0], 7);
  assert.equal(life.idle(), true);
});

test('empty 204 response releases the lifetime without buffering', async () => {
  const life = new Lifetime();
  const res = await life.response(new Response(null, {status:204}));
  assert.equal(res.status, 204);
  assert.equal(res.body, null);
  assert.equal(life.idle(), true);
});

test('capacity reserves synchronously across concurrent cold requests', () => {
  const c = new RuntimeCapacity(2, 10);
  assert.equal(c.reserve('a', 6), true); assert.equal(c.reserve('a', 6), true);
  assert.equal(c.reserve('b', 5), false); assert.equal(c.reserve('b', 4), true);
  assert.equal(c.reserve('c', 1), false); c.release('a'); assert.equal(c.reserve('c', 1), true);
});

test('count LRU bounds N>cap entries and refreshes recency on get', () => {
  const c = new CountLRU(3);
  c.set('a', 1); c.set('b', 2); c.set('c', 3);
  c.get('a');
  c.set('d', 4);
  assert.equal(c.size, 3);
  assert.equal(c.has('a'), true);
  assert.equal(c.has('b'), false);
  assert.deepEqual([...c.values()], [3, 1, 4]);
  c.delete('a');
  assert.equal(c.has('a'), false);
});

test('cacheable sources are retained and oversized valid sources bypass the cache', () => {
  const c = sourceCache(2000);
  const source = 'x'.repeat(20 * 1024 * 1024);
  cacheSource(c, 'sha', source);
  assert.equal(c.get('sha'), source);
  const oversized = 'x'.repeat(Math.ceil(SOURCE_CACHE_BYTES / 2) + 1);
  assert.equal(cacheSource(c, 'too-big', oversized), oversized);
  assert.equal(c.has('too-big'), false);
});

test('a 101 without a usable websocket does not pin the isolate', async () => {
  const life = new Lifetime();
  const res = {status: 101};
  assert.equal(await life.response(res), res);
  assert.equal(life.pinned, false);
  assert.equal(life.sockets, 0);
  assert.equal(life.idle(), true);
});

test('websocket pin clears on close or error and releases its reservation', async () => {
  for (const event of ['close', 'error']) {
    const life = new Lifetime();
    const capacity = new RuntimeCapacity(1, 32);
    assert.equal(capacity.reserve('resident', 16), true);
    const socket = new EventTarget();
    const res = { status: 101, webSocket: socket };
    const out = await life.response(res, () => capacity.release('resident'));
    assert.equal(out, res);
    assert.equal(life.pinned, true);
    assert.equal(life.sockets, 1);
    assert.equal(life.idle(), false);
    assert.equal(capacity.reserve('other', 16), false);
    socket.dispatchEvent(new Event(event));
    assert.equal(life.pinned, false);
    assert.equal(life.sockets, 0);
    assert.equal(life.idle(), true);
    assert.equal(capacity.reserve('other', 16), true);
  }
});

test('lifecycle status reports live websocket count', () => {
  const code = managedModules({ mainModule: 'tenant.js', compatibilityFlags: [] }, 'default', 'nonce')['__cellhive-managed.js'];
  assert.match(code, /sockets:life\.sockets/);
});

// Execute the generated entrypoint; only the native imports are replaced.
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const failureSource=readFileSync(new URL('./telemetry.js',import.meta.url),'utf8');
function managedHarness(handler, response) {
  const c=vm.createContext({Response,Headers,encodeURIComponent,JSON, target:{default:handler},
    WorkerEntrypoint:class {ctx={waitUntil(){}};env={};}, life:new Lifetime(),runInvocation:fn=>fn()});
  vm.runInContext(failureSource.slice(failureSource.indexOf('export function invocationFailureAttributes')).replace(/^export /gm,''),c);
  if(response)c.life.response=response;
  const modules=managedModules({mainModule:'tenant.js'},'default','nonce',{},c.invocationFailureAttributes);
  vm.runInContext(modules['__cellhive-managed.js'].replace(/^\s*import .*;$/gm,'').replace(/export class /g,'class ')+'\nglobalThis.entry=new CellHiveManagedFetch();',c);
  return c;
}
test('managed fetch preserves sanitized user throw before RPC serialization',async()=>{
 const err=new TypeError('hello token=private https://private.test/path');err.stack='TypeError: hello\n    at fetch (tenant.js:14:7)';
 const c=managedHarness({fetch(){throw err;}});
 const res=await c.entry.fetch(new Request('https://app.test/'));
 assert.equal(res.status,500);
 const d=JSON.parse(decodeURIComponent(res.headers.get('x-cellhive-invocation-failure')));
 assert.equal(d['cellhive.error_origin'],'user_function');assert.equal(d['cellhive.function_result'],'threw');
 assert.equal(d['cellhive.error_frame'],'tenant.js:14:7');assert.equal(d['cellhive.error_name'],'TypeError');
 assert.doesNotMatch(JSON.stringify(d),/private/);assert.equal(await res.text(),String(err));assert.equal(c.life.idle(),true);
});
test('managed fetch strips forged diagnostics without consuming response body',async()=>{
 let pulls=0;
 const c=managedHarness({fetch(){return new Response(new ReadableStream({pull(){pulls++;}},{highWaterMark:0}),{headers:{'x-cellhive-invocation-failure':'forged'}});}});
 const res=await c.entry.fetch(new Request('https://app.test/'));
 assert.equal(res.headers.has('x-cellhive-invocation-failure'),false);assert.equal(pulls,0);await res.body.cancel();
});
test('managed response processing failure is separate from a user throw',async()=>{
 const c=managedHarness({fetch(){return new Response('ok');}},()=>{throw new Error('response plumbing failed');});
 const res=await c.entry.fetch(new Request('https://app.test/'));
 const d=JSON.parse(decodeURIComponent(res.headers.get('x-cellhive-invocation-failure')));
 assert.equal(d['cellhive.error_origin'],'unknown');assert.equal(d['cellhive.function_result'],'returned');
});
test('platform invocation setup failure is not attributed to tenant code',async()=>{
 const c=managedHarness({fetch(){throw Error('must not run');}});
 c.runInvocation=()=>{throw Error('invocation context failed');};
 const res=await c.entry.fetch(new Request('https://app.test/'));
 const d=JSON.parse(decodeURIComponent(res.headers.get('x-cellhive-invocation-failure')));
 assert.equal(d['cellhive.error_origin'],'platform');
});
test('a rejected tenant response stream is not automatically blamed on platform plumbing',async()=>{
 const c=managedHarness({fetch(){return new Response(new ReadableStream({start(controller){controller.error(new Error('tenant stream failed'));}}),{headers:{'content-type':'text/plain'}});}});
 const res=await c.entry.fetch(new Request('https://app.test/'));
 const d=JSON.parse(decodeURIComponent(res.headers.get('x-cellhive-invocation-failure')));
 assert.equal(d['cellhive.failure_stage'],'response_processing');
 assert.equal(d['cellhive.error_origin'],'unknown');
 assert.equal(d['cellhive.function_result'],'returned');
});
