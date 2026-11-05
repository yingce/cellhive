import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
import {MAX_DO_ENVELOPE_BYTES, MAX_DO_REQUEST_BYTES} from './rpc-codec.js';
const source=readFileSync(new URL('./facades.js',import.meta.url),'utf8').replace(/import\s*\{[^}]+\}\s*from\s*"[^"]+";/g,'').replace(/^export /gm,'');
function transport(fetcher){
 const c=vm.createContext({Request,Response,Headers,URL,Uint8Array,TextEncoder,TextDecoder,ReadableStream,btoa,atob,console,MAX_DO_ENVELOPE_BYTES,MAX_DO_REQUEST_BYTES});
 vm.runInContext(source+'\nglobalThis.make = makeDOTransport;',c);
 return c.make({url:'http://agent',fetcher:{fetch:fetcher}},{ns:'demo',worker:'web',class:'Room',bundle_sha:'sha',direct:false});
}
test('DO transport preserves request and response bytes and tenant headers',async()=>{
 let sent;
 const t=transport(async(_url,init)=>{sent=JSON.parse(init.body);return new Response(new Uint8Array([0,255,128,65]),{headers:{'x-cellhive-do-app':'1','x-cellhive-do-owner-ticket':'secret','x-result':'yes','set-cookie':'a=b','location':'/next'}})});
 const response=await t.__call('one',new Request('http://do/echo',{method:'POST',body:new Uint8Array([0,255,128,65])}));
 assert.equal(sent.request.body_base64,'AP+AQQ==');
 assert.deepEqual([...new Uint8Array(await response.arrayBuffer())],[0,255,128,65]);
 assert.equal(response.headers.get('x-result'),'yes');assert.equal(response.headers.get('set-cookie'),'a=b');assert.equal(response.headers.get('location'),'/next');assert.equal(response.headers.get('x-cellhive-do-owner-ticket'),null);
});
for(const status of [204,205,304])test(`DO transport preserves null body ${status}`,async()=>{
 const t=transport(async()=>new Response(null,{status,headers:{'x-cellhive-do-app':'1'}}));
 assert.equal((await t.__call('one',new Request('http://do/'))).status,status);
});

test('DO transport streams an ordinary response body without eager buffering', async () => {
  let pulls = 0;
  const t = transport(async () => new Response(new ReadableStream({
    pull(c) {
      pulls++;
      if (pulls > 2) { c.close(); return; }
      c.enqueue(new Uint8Array([pulls, 0, 255]));
    },
  }, {highWaterMark: 0}), {headers: {'x-cellhive-do-app': '1'}}));
  const response = await t.__call('one', new Request('http://do/large'));
  assert.equal(pulls, 0);
  const bytes = new Uint8Array(await response.arrayBuffer());
  assert.deepEqual([...bytes], [1, 0, 255, 2, 0, 255]);
  assert.ok(pulls > 0);
});

test('DO transport relays a response larger than the JSON envelope cap', async () => {
  const size = MAX_DO_ENVELOPE_BYTES + 1024;
  let sent = 0;
  const t = transport(async () => new Response(new ReadableStream({
    pull(c) {
      const left = size - sent;
      if (left <= 0) { c.close(); return; }
      const n = Math.min(left, 64 * 1024);
      sent += n;
      c.enqueue(new Uint8Array(n).fill(7));
    },
  }, {highWaterMark: 0}), {headers: {'x-cellhive-do-app': '1'}}));
  const bytes = new Uint8Array(await (await t.__call('one', new Request('http://do/large'))).arrayBuffer());
  assert.equal(bytes.byteLength, size);
  assert.equal(bytes[0], 7);
});

test('DO transport rejects an unstreamable request envelope above its cap with 413', async () => {
  const t = transport(async () => { throw new Error('must not dispatch'); });
  const body = new Uint8Array(MAX_DO_REQUEST_BYTES + 1);
  await assert.rejects(
    t.__call('one', new Request('http://do/upload', {method: 'POST', body})),
    (e) => e && e.status === 413 && /exceed|too large/i.test(e.message),
  );
});

test('DO transport cancels an oversized chunked request body at max+1', async () => {
  let cancelled = false;
  let sent = 0;
  const body = new ReadableStream({
    pull(c) {
      if (sent >= MAX_DO_REQUEST_BYTES + 64 * 1024) { c.close(); return; }
      sent += 64 * 1024;
      c.enqueue(new Uint8Array(64 * 1024));
    },
    cancel() { cancelled = true; },
  }, {highWaterMark: 0});
  const t = transport(async () => { throw new Error('must not dispatch'); });
  await assert.rejects(
    t.__call('one', new Request('http://do/upload', {method: 'POST', body, duplex: 'half'})),
    (e) => e && e.status === 413,
  );
  assert.equal(cancelled, true);
  assert.ok(sent <= MAX_DO_REQUEST_BYTES + 64 * 1024);
});
