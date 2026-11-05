import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import test from 'node:test';
import { internalHeaders, authorized as verifyInternal } from '../platform/jwt.js';
import { SOURCE_CACHE_BYTES } from '../platform/lifecycle.js';
const signingKey=Buffer.alloc(32,7).toString('base64url');
const source = readFileSync(new URL('./internal.js', import.meta.url), 'utf8');
function harness(metadata, bundle='export default {}') {
  const context = vm.createContext({ internalHeaders, verifyInternal, Request, Response, URL, TextEncoder, console, crypto: { randomUUID: ()=>'test' },
    bindingStub(){}, workerLimits:()=>({}), quotaSource:()=>'', invocationSource:()=>'',
    checkedWorkerGet: async (_loader,id,code)=>({id,code,getEntrypoint:()=>({handleFetch:async()=>new Response('ok'),callMethod:async()=> 'ok'})}),
    budgetErrorBody:()=>null,
    fetch:async url=>url.includes('/worker/bindings') ? new Response(JSON.stringify(metadata())) : url.includes('/bundle-url') ? new Response('',{status:404}) : url.includes('/log-capture') ? new Response('{"mode":"off"}') : new Response(bundle) });
  vm.runInContext(readFileSync(new URL('../platform/lifecycle.js', import.meta.url),'utf8').replace(/^export /gm,''), context);
  vm.runInContext('globalThis.__rewrites=0;{const original=String.prototype.replaceAll;String.prototype.replaceAll=function(...args){globalThis.__rewrites++;return original.apply(this,args);};}', context);
  vm.runInContext(source.replace(/^import .*;$/gm,'').replace(/^export \{.*;$/gm,'').replace('export default {','const entry = {')+'\nglobalThis.entry = entry;globalThis.loadSource = loadSource;globalThis.bundleCache = bundleCache;', context);
  context.env={CELL_URL:'http://local',CELL_TOKEN:signingKey,DISPATCH_TOKEN:signingKey,CH_LOG_CAPTURE:'0'};
  return context;
}
const body={namespace:'ns',worker:'fn',version:1,bundle_sha:'same'};
const spec=(storage,vars={})=>({version:1,bundle_sha:'same',storage_id:storage,bindings:{},vars});
test('legacy same numeric version revalidates metadata after worker recreation',async()=>{
 let current=spec('old',{MODE:'old'});const c=harness(()=>current);
 await c.specFor(c.env,body);current=spec('new',{MODE:'new'});
 assert.equal((await c.specFor(c.env,body)).vars.MODE,'new');
});
test('legacy cached version cannot accept a different returned SHA',async()=>{
 let current=spec('old');const c=harness(()=>current);await c.specFor(c.env,body);
 current={...spec('new'),bundle_sha:'other'};await assert.rejects(c.specFor(c.env,body),/identity/);
});
for(const [name,request,returned] of [
 ['version',{...body,version:2},spec('old')],['SHA',{...body,bundle_sha:'other'},spec('old')],
 ['incarnation',{...body,storage_id:'new'},spec('old')],['missing incarnation',{...body,storage_id:'new'},{...spec(undefined)}],
])test(`rejects mismatched ${name}`,async()=>{const c=harness(()=>returned);await assert.rejects(c.specFor(c.env,request),/identity/);});
test('version zero retains active lookup semantics without immutable caching',async()=>{
 let current=spec('first',{MODE:'first'});const c=harness(()=>current);
 const request={...body,version:0};assert.equal((await c.specFor(c.env,request)).vars.MODE,'first');
 current={...spec('second',{MODE:'second'}),version:2};assert.equal((await c.specFor(c.env,request)).vars.MODE,'second');
});
test('SHA-only explicit service pins and absent numeric versions remain supported',async()=>{
 const c=harness(()=>spec('new'));const got=await c.specFor(c.env,{...body,version:undefined,bundle_sha:'historic',bindings:{},vars:{MODE:'pinned'}});
 assert.equal(got.vars.MODE,'pinned');
});
for(const load of ['loadWorker','loadWorkflowWorker'])test(`${load} separates returned incarnations for legacy requests`,async()=>{
 let current=spec('old',{MODE:'old'});const c=harness(()=>current);const first=await c[load](c.env,{},body);
 current=spec('new',{MODE:'new'});const second=await c[load](c.env,{},body);
 assert.notEqual(first.id,second.id);assert.equal(second.code.env.MODE,'new');
});
for(const load of ['loadWorker','loadWorkflowWorker'])test(`${load} uses resolved version for active identity`,async()=>{
 let current=spec('one',{MODE:'v1'});const c=harness(()=>current);const request={...body,version:0};
 const first=await c[load](c.env,{},request);current={...spec('one',{MODE:'v2'}),version:2};
 const second=await c[load](c.env,{},request);assert.notEqual(first.id,second.id);
});
for(const endpoint of ['fetch','run'])test(`service ${endpoint} retains supplied incarnation validation`,async()=>{
 const c=harness(()=>spec('new'));
 const req=new Request(`http://local/v1/services/${endpoint}`,{method:'POST',body:JSON.stringify({...body,storage_id:'old',method:'which'})});
 const response=await c[endpoint==='fetch'?'dispatchServiceFetch':'dispatchServiceRun'](req,c.env,{});
 assert.equal(response.status,502);
});
for(const [target,provided,status] of [['new','old',403],['new',undefined,403],['new','new',404],[undefined,'',404],['',undefined,404]])test(`pool incarnation ${target}/${provided}`,async()=>{
 const c=harness(()=>spec('new'));c.env.CH_POOL_TARGET=JSON.stringify({...body,storage_id:target});
  const response=await c.entry.fetch(new Request('http://local/unknown',{method:'POST',headers:await internalHeaders(signingKey,'dispatch'),body:JSON.stringify({...body,storage_id:provided})}),c.env,{});
 assert.equal(response.status,status);
});

test('timer handler failure diagnostics keep only sanitized name and first frame',async()=>{
 const c=harness(()=>spec('new'));
 c.startInvocation=async()=>({invocation_id:'inv'});
 c.finishInvocationBestEffort=async()=>{};
 const call=async execution=>{
  c.loadWorker=async()=>({getEntrypoint:()=>({handleScheduled:async()=>{if(execution.errorObject)throw execution.errorObject;return execution;}})});
  const res=await c.dispatchTimers({json:async()=>({...body})},c.env,{});
  return {status:res.status,body:await res.json()};
 };
 const stack=['TypeError: business-secret?token=message-secret','    at handler (https://user:password@app.test/app/src/index.js?token=query-secret:12:7)','    at runner (/app/src/runner.js:20:3)'].join('\n');
 let got=await call({error:'business-secret',error_stack:stack});
 assert.equal(got.status,500);assert.equal(got.body.error,'handler_failed');assert.equal(got.body.message,'business-secret');
 assert.equal(got.body.error_name,'TypeError');assert.equal(got.body.error_frame,'index.js:12:7');
 assert.ok(!JSON.stringify({name:got.body.error_name,frame:got.body.error_frame}).match(/business|secret|password|query/));

 got=await call({error:'minified',error_stack:'Error: minified\n    at <anonymous>:1:1'});
 assert.equal(got.body.error_name,'Error');assert.equal('error_frame' in got.body,false);

 const bad=new Error('business-secret');bad.name='bad name!';bad.stack='bad name!\n    at handler (/app/src/worker.js:5:2)';
 got=await call({errorObject:bad});
 assert.ok(got.body.message.includes('business-secret'));assert.equal(got.body.error_name,'Error');assert.equal(got.body.error_frame,'worker.js:5:2');
 assert.ok(!JSON.stringify({name:got.body.error_name,frame:got.body.error_frame}).match(/business|secret|bad name/));
});

test('dispatch bundle rewrite is cached once per sha and variant', async () => {
  const c = harness(() => spec('one'));
  await c.loadWorker(c.env, {}, body);
  const workerRewrites = c.__rewrites;
  assert.equal(workerRewrites, 2);
  await c.loadWorker(c.env, {}, body);
  assert.equal(c.__rewrites, workerRewrites);
  await c.loadWorkflowWorker(c.env, {}, body);
  assert.equal(c.__rewrites, workerRewrites + 4);
  await c.loadWorkflowWorker(c.env, {}, body);
  assert.equal(c.__rewrites, workerRewrites + 4);
});

test('dispatch source caches share one conservative budget and admit oversized sources', () => {
  assert.match(source, /const bundleCache = sourceCache\(BUNDLE_CACHE_MAX\)/);
  assert.match(source, /const rewrittenCache = bundleCache/);
  assert.match(source, /cacheSource\(bundleCache, sha, text\)/);
  assert.match(source, /cacheSource\(rewrittenCache, key, out\)/);
  assert.match(source, /const cached = rewrittenCache\.get\(key\);[\s\S]*const source = await loadSource/);
});

test('oversized valid bundle is returned without cache admission failure', async () => {
  const oversized = 'x'.repeat(Math.ceil(SOURCE_CACHE_BYTES / 2) + 1);
  const big = harness(() => spec('one'), oversized);
  assert.equal(await big.loadSource(big.env, 'oversized-sha'), oversized);
  assert.equal(big.bundleCache.has('oversized-sha'), false);
  const ordinary = harness(() => spec('one'), 'export default {}');
  assert.equal(await ordinary.loadSource(ordinary.env, 'ordinary-sha'), 'export default {}');
  assert.equal(ordinary.bundleCache.has('ordinary-sha'), true);
});
