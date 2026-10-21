import test from 'node:test';
import assert from 'node:assert/strict';
import { ByteLRU, Lifetime, RuntimeCapacity, quotaSource } from './lifecycle.js';

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
