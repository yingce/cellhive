import assert from 'node:assert/strict';
import test from 'node:test';
import { encode, decode } from './rpc-codec.js';

test('Queue v8 envelope preserves structured values and shared references', () => {
  const shared = { n: 1n, when: new Date('2026-10-15T00:00:00Z'), bytes: new Uint8Array([0, 128, 255]) };
  const value = { first: shared, second: shared };
  value.self = value;
  const payload = JSON.stringify({ __cellhive_v8: encode(value) });
  const got = decode(JSON.parse(payload).__cellhive_v8);
  assert.equal(got.first, got.second);
  assert.equal(got.self, got);
  assert.equal(got.first.n, 1n);
  assert.equal(got.first.when.toISOString(), '2026-10-15T00:00:00.000Z');
  assert.deepEqual([...got.first.bytes], [0, 128, 255]);
});
