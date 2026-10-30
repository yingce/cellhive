import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import test from 'node:test';

const source = readFileSync(new URL('./log-tail.js', import.meta.url), 'utf8');
function extract(start, end) {
  return source.slice(source.indexOf(start), source.indexOf(end, source.indexOf(start)));
}

function harness() {
  const c = vm.createContext({ JSON, Date, TextEncoder, console });
  vm.runInContext(
    extract('function safeArgs(', 'export function readTailEvents(')
      .replace('export function platformLifecycleEvent', 'function platformLifecycleEvent'),
    c,
  );
  return c;
}

const props = { worker: 'web', version: 3, tailKey: '' };

test('platform lifecycle RPCs are not tenant tail events', () => {
  const c = harness();
  const records = c.eventRecords([
    { entrypoint: 'CellHiveLifecycle', event: { rpcMethod: 'status' }, outcome: 'ok', eventTimestamp: 1 },
    {
      entrypoint: 'CellHiveLifecycle',
      event: { rpcMethod: 'retire' },
      outcome: 'exception',
      exceptions: [{ name: 'Error', message: 'Worker execution was aborted (isolate aborted): cellhive: retired' }],
      eventTimestamp: 2,
    },
    { entrypoint: 'CellHiveManagedFetch', event: { request: { method: 'GET', url: 'https://example.test/' } }, outcome: 'ok', eventTimestamp: 3 },
    { event: { request: { method: 'GET', url: 'https://example.test/boom' } }, outcome: 'exception', exceptions: [{ name: 'Error', message: 'tenant failure' }], eventTimestamp: 4 },
  ], props);
  assert.equal(records.length, 2);
  assert.deepEqual(records.map((r) => r.event_type), ['fetch', 'fetch']);
  assert.equal(records[1].exceptions[0].message, 'tenant failure');
});

test('lifecycle filtering falls back to rpcMethod only without an entrypoint', () => {
  const c = harness();
  assert.equal(c.platformLifecycleEvent({ event: { rpcMethod: 'retire' } }), true);
  assert.equal(c.platformLifecycleEvent({ event: { rpcMethod: 'status' } }), true);
  // A tenant entrypoint may legitimately expose a method called retire.
  assert.equal(c.platformLifecycleEvent({ entrypoint: 'Api', event: { rpcMethod: 'retire' } }), false);
  assert.equal(c.platformLifecycleEvent({ entrypoint: 'CellHiveManagedFetch', event: { request: {} } }), false);
});

test('queue and scheduled dispatch records are still exported', () => {
  const c = harness();
  const tailKey = 'tail-1';
  const line = { message: ['__cellhive_dispatch_v1__', tailKey, { queue: 'jobs', batchSize: 1 }], level: 'log' };
  const records = c.eventRecords([
    { event: { rpcMethod: 'handleQueue' }, logs: [line], outcome: 'ok', eventTimestamp: 1 },
  ], { ...props, tailKey });
  assert.equal(records.length, 1);
  assert.equal(records[0].event_type, 'queue');
  assert.equal(records[0].event.queue, 'jobs');
});
