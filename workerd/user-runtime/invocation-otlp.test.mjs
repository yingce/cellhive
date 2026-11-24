// Invocation metadata must reach OTLP (or the local log pipe) instead of the
// removed __invocations__ cell. These tests pin that contract.
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import test from 'node:test';
import { internalHeaders, authorized as verifyInternal } from '../platform/jwt.js';

const internalSource = readFileSync(new URL('./internal.js', import.meta.url), 'utf8');
const telemetrySource = readFileSync(new URL('../platform/telemetry.js', import.meta.url), 'utf8');
const lifecycleSource = readFileSync(new URL('../platform/lifecycle.js', import.meta.url), 'utf8');

function harness() {
  const posts = [];
  const lines = [];
  const context = vm.createContext({
    Request, Response, URL, TextEncoder, Headers, AbortController, setTimeout, clearTimeout,
    console: { log: (...a) => lines.push(a.join(' ')), error: (...a) => lines.push(a.join(' ')) },
    crypto: { randomUUID: () => 'test-uuid' },
    internalHeaders, verifyInternal,
    fetch: async (url, init) => { posts.push({ url: String(url), init }); return new Response(null, { status: 204 }); },
    bindingStub() {}, workerLimits: () => ({}), quotaSource: () => '', invocationSource: () => '',
    checkedWorkerGet: async () => ({ getEntrypoint: () => ({ handleFetch: async () => new Response('ok'), callMethod: async () => 'ok' }) }),
    budgetErrorBody: () => null,
  });
  vm.runInContext(lifecycleSource.replace(/^export /gm, ''), context);
  vm.runInContext(telemetrySource.replace(/^import .*;$/gm, '').replace(/^export /gm, ''), context);
  vm.runInContext(
    internalSource
      .replace(/^import .*;$/gm, '')
      .replace(/^export \{.*;$/gm, '')
      .replace('export default {', 'const entry = {') +
      '\nglobalThis.entry = entry;globalThis.finishInvocation = finishInvocation;globalThis.startInvocation = startInvocation;',
    context,
  );
  return { context, posts, lines };
}

test('finished invocation exports one OTLP record and no console payload', async () => {
  const h = harness();
  const record = {
    invocation_id: 'inv-1', request_id: 'req-1',
    traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
    namespace: 'acme', worker: 'api', kind: 'queue', resource: 'JOBS', logical_id: 'queue:abc', attempt: 2,
    started_ms: 100, status: 'running', messages: [{ id: 'm1', logical_id: 'message:x', attempt: 1 }],
    producer_links: [{ message_id: 'm1', request_id: 'producer-1', traceparent: '00-aa-bb-01' }],
    _logging_enabled: true,
  };
  await h.context.finishInvocation({ OTLP_LOGS_URL: 'http://collector:4318' }, record, {
    value: { result: 'ok', retry: [{ id: 'm2', delay_seconds: 5 }] },
  });
  assert.equal(h.posts.length, 1);
  assert.equal(h.posts[0].url, 'http://collector:4318/v1/logs');
  assert.ok(h.posts[0].init.body && h.posts[0].init.body.byteLength > 0, 'OTLP protobuf body expected');
});

test('invocation no longer posts to the removed internal endpoint', () => {
  assert.equal(/\/v1\/internal\/invocations/.test(internalSource), false);
  assert.equal(/persistInvocation/.test(internalSource), false);
});

test('logging disabled skips export entirely', async () => {
  const h = harness();
  await h.context.finishInvocation({ OTLP_LOGS_URL: 'http://collector:4318' }, {
    invocation_id: 'inv-2', namespace: 'acme', worker: 'api', kind: 'cron', logical_id: 'cron:x', attempt: 1,
    started_ms: 1, status: 'running', _logging_enabled: false,
  }, { value: {} });
  assert.equal(h.posts.length, 0);
});

test('invocation console stays out of the exported record', async () => {
  const h = harness();
  const record = {
    invocation_id: 'inv-3', request_id: 'req-3', namespace: 'acme', worker: 'api', kind: 'cron',
    logical_id: 'cron:y', attempt: 1, started_ms: 5, status: 'running', _logging_enabled: true,
  };
  await h.context.finishInvocation({ OTLP_LOGS_URL: 'http://collector:4318' }, record, {
    value: {}, logs: [{ level: 'log', message: 'tenant secret line', at_ms: 6 }],
  });
  assert.equal(h.posts.length, 1);
  const body = Buffer.from(h.posts[0].init.body).toString('latin1');
  assert.equal(body.includes('tenant secret line'), false, 'console must not be embedded in invocations export');
});

test('cron identity survives retry time changes and unknown attempts are not reported as one', async () => {
  const {webcrypto} = await import('node:crypto');
  const h=harness(); h.context.crypto=webcrypto;
  const body={namespace:'vwork-105',worker:'pageverra-api',token:'stable-timer',scheduled_time_ms:1000,cron:'* * * * *'};
  const first=await h.context.startInvocation({}, {...body}, 'cron');
  const retry=await h.context.startInvocation({}, {...body,scheduled_time_ms:5000}, 'cron');
  const next=await h.context.startInvocation({}, {...body,token:'next-timer'}, 'cron');
  assert.equal(first.logical_id,retry.logical_id);
  assert.notEqual(first.logical_id,next.logical_id);
  assert.equal(first.attempt,null);
  assert.equal(first.scheduled_time_ms,1000);
  assert.equal((await h.context.startInvocation({}, {...body,attempt:3}, 'cron')).attempt,3);
});

test('failed invocation exports safe location and stage without claiming root cause', async () => {
  const h = harness();
  vm.runInContext('globalThis.captured=[]; exportLogRecords=async (_env, records)=>captured.push(...records)', h.context);
  await h.context.finishInvocation({}, {kind:'cron',_logging_enabled:true}, {
    error:'Error: INTERNAL_ERROR', error_stack:'Error: INTERNAL_ERROR\n    at db (https://secret.example/tenant.js?token=private:49:20)',
  });
  const a=h.context.captured[0].attributes;
  assert.equal(a['cellhive.error_name'],'Error');
  assert.equal(a['cellhive.error_frame'],'tenant.js:49:20');
  assert.equal(a['cellhive.failure_stage'],'handler');
  assert.equal(a['cellhive.error_origin'],'unknown');
  assert.equal(JSON.stringify(a).includes('private'),false);
});

test('worker loading failure is distinct from a handler exception', async () => {
  const h=harness();
  vm.runInContext('globalThis.captured=[]; exportLogRecords=async (_env, records)=>captured.push(...records)',h.context);
  await h.context.finishInvocation({}, {kind:'cron',_logging_enabled:true}, {error:'bundle unavailable',failure_stage:'worker_load'});
  assert.equal(h.context.captured[0].attributes['cellhive.failure_stage'],'worker_load');
  assert.equal(h.context.captured[0].attributes['cellhive.error_origin'],'unknown');
});

test('cron noRetry envelope retains exception frame and requested queue retries are not failures', async () => {
 const h=harness();vm.runInContext('globalThis.captured=[]; exportLogRecords=async (_e,r)=>captured.push(...r)',h.context);
 await h.context.finishInvocation({}, {kind:'cron',_logging_enabled:true}, {value:{ok:false,no_retry:true,error:'Error: failed',error_stack:'Error: failed\n at run (tenant.js:12:3)'}});
 assert.equal(h.context.captured[0].attributes['cellhive.error_frame'],'tenant.js:12:3');
 await h.context.finishInvocation({}, {kind:'queue',_logging_enabled:true}, {value:{retry:[{id:'m1'}]}});
 assert.equal(h.context.captured[1].attributes['cellhive.outcome'],'retry');
 assert.equal(h.context.captured[1].attributes['cellhive.failure_stage'],null);
});

test('internal service fetch records returned HTTP failure without reading response for diagnostics',async()=>{
 const {webcrypto}=await import('node:crypto');const h=harness();h.context.crypto=webcrypto;
 vm.runInContext('globalThis.captured=[];exportLogRecords=async (_e,r)=>captured.push(...r);loadWorker=async()=>({getEntrypoint:()=>({handleFetch:async()=>new Response("private-body",{status:502})})});globalThis.serviceFetch=dispatchServiceFetch',h.context);
 const r=await h.context.serviceFetch(new Request('http://internal',{method:'POST',body:JSON.stringify({namespace:'ns',worker:'fn',bundle_sha:'sha',url:'https://app.test/'})}),{CH_INVOCATION_LOGS:'1'},{});
 assert.equal(r.status,502);assert.equal(await r.text(),'private-body');
 const a=h.context.captured[0].attributes;
 assert.equal(a['cellhive.event_type'],'fetch');assert.equal(a['cellhive.status'],502);
 assert.equal(a['cellhive.failure_stage'],'handler_response');
 assert.equal(JSON.stringify(a).includes('private-body'),false);
});
