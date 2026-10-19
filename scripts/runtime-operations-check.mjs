// JSON assertions for the real Docker runtime-baseline acceptance journey.
import assert from 'node:assert/strict';
const [mode, raw, id] = process.argv.slice(2);
const value = JSON.parse(raw);
if (mode === 'preview') {
  assert.equal(value.messages.length, 1);
  const message = value.messages[0];
  assert.ok(message.id);
  assert.ok(message.timestamp_ms > 0);
  assert.equal(message.lease_until_ms, 0);
  assert.equal(message.source_queue, 'JOBS');
  assert.equal(message.source_attempts, 3); // initial delivery + max_retries=2
  assert.ok(message.producer_request_id);
  assert.match(message.producer_traceparent, /^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/);
  assert.deepEqual(JSON.parse(Buffer.from(message.body, 'base64')), { id: 'poison' });
} else if (mode === 'empty') {
  assert.equal(value.messages.length, 0);
} else if (mode === 'preserved') {
  assert.equal(value.messages.length, 1);
  assert.equal(Buffer.from(value.messages[0].body, 'base64').toString(), 'keep');
} else if (mode === 'invocations') {
  const records = value.invocations;
  assert.ok(records.some(r => r.kind === 'cron' && r.status === 'success' && r.logs.some(l => l.message.includes('cron invocation'))));
  assert.ok(records.some(r => r.kind === 'cron' && r.status === 'failed' && r.no_retry && r.error.includes('scheduled noRetry probe')));
  assert.ok(records.some(r => r.kind === 'queue' && r.status === 'retry'));
  const failed = records.find(r => r.kind === 'queue' && r.status === 'failed' && r.error.includes('invocation failure probe'));
  assert.ok(failed, 'missing failed Queue attempt');
  assert.ok(failed.logs.some(l => l.message.includes('invocation-fail')));
  assert.equal(failed.producer_links.length, 1);
  assert.ok(failed.producer_links[0].message_id);
  assert.ok(failed.producer_links[0].request_id);
  assert.match(failed.producer_links[0].traceparent, /^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/);
  assert.ok(records.some(r => r.kind === 'queue' && r.status === 'success' && r.logical_id === failed.logical_id && r.attempt > failed.attempt &&
    r.producer_links?.[0]?.message_id === failed.producer_links[0].message_id &&
    r.producer_links?.[0]?.traceparent === failed.producer_links[0].traceparent));
  console.log(failed.invocation_id);
} else if (mode === 'one') {
  assert.equal(value.invocations.length, 1);
  assert.equal(value.invocations[0].invocation_id, id);
  assert.equal(value.invocations[0].status, 'failed');
  assert.ok(value.invocations[0].logs.length > 0);
} else if (mode === 'workflow') {
  const records = value.invocations.filter(r => r.kind === 'workflow' && r.resource === 'REPORTS');
  assert.ok(records.length >= 2, 'sleep and resume must be separate Workflow attempts');
  assert.ok(records.every(r => r.status === 'success' && r.request_id === r.invocation_id));
  assert.ok(records.every(r => /^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/.test(r.traceparent)));
  assert.equal(new Set(records.map(r => r.invocation_id)).size, records.length);
  assert.equal(new Set(records.map(r => r.logical_id)).size, 1);
} else {
  throw new Error(`unknown assertion mode: ${mode}`);
}
