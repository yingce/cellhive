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
  assert.equal(message.source_attempts, 2);
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
  assert.ok(records.some(r => r.kind === 'queue' && r.status === 'success' && r.logical_id === failed.logical_id && r.attempt > failed.attempt));
  console.log(failed.invocation_id);
} else if (mode === 'one') {
  assert.equal(value.invocations.length, 1);
  assert.equal(value.invocations[0].invocation_id, id);
  assert.equal(value.invocations[0].status, 'failed');
  assert.ok(value.invocations[0].logs.length > 0);
} else {
  throw new Error(`unknown assertion mode: ${mode}`);
}
