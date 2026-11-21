// JSON assertions for the real Docker runtime-baseline acceptance journey.
import assert from 'node:assert/strict';
const [mode, raw] = process.argv.slice(2);
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
} else {
  throw new Error(`unknown assertion mode: ${mode}`);
}
