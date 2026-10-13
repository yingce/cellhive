import test from 'node:test';
import assert from 'node:assert/strict';
import { ByteLRU, Lifetime, RuntimeCapacity } from './lifecycle.js';

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
  const response = life.response(new Response(new ReadableStream({start(c){c.enqueue(new Uint8Array([1]));}})));
  assert.equal(life.idle(), false); await response.body.cancel(); assert.equal(life.idle(), true);
});

test('stream is not eagerly consumed and error releases lifetime', async () => {
  const life = new Lifetime(); let pulls=0;
  const res = life.response(new Response(new ReadableStream({pull(c){pulls++;c.error(new Error('boom'));}}, {highWaterMark:0})));
  assert.equal(pulls,0); assert.equal(life.idle(),false);
  await assert.rejects(res.text(), /boom/); assert.equal(life.idle(),true);
});

test('capacity reserves synchronously across concurrent cold requests', () => {
  const c = new RuntimeCapacity(2, 10);
  assert.equal(c.reserve('a', 6), true); assert.equal(c.reserve('a', 6), true);
  assert.equal(c.reserve('b', 5), false); assert.equal(c.reserve('b', 4), true);
  assert.equal(c.reserve('c', 1), false); c.release('a'); assert.equal(c.reserve('c', 1), true);
});
