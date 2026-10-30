import { readFileSync } from 'node:fs';
import { webcrypto } from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import test from 'node:test';
import { Lifetime } from '../platform/lifecycle.js';
const source = readFileSync(new URL('./loader.js', import.meta.url), 'utf8');
function extract(start, end) { return source.slice(source.indexOf(start), source.indexOf(end, source.indexOf(start))); }
function harness(fetch) {
  const c = vm.createContext({ envCache:new Map(), envInflight:new Map(), Date, encodeURIComponent, JSON, fetch, noteFailure(){}, noteSuccess(){}, hostTtlMs(){return 10000;}, routeEpoch:0 });
  vm.runInContext(extract('async function workerEnv(', '// matchPath picks'), c);
  return (...args) => c.workerEnv({CELL_URL:'http://mock'}, 'ns', 'fn', ...args);
}
const ok = v => ({ok:true,status:200,json:async()=>v});
const version = (number, sha, storage, vars={}) => ({version:number,bundle_sha:sha,storage_id:storage,vars});
test('blank host returns 404 without a control-plane lookup',async()=>{
  let lookups=0;
  const c=vm.createContext({URL,Response,Number,JSON,text:(m,s)=>({status:s,body:m}),hostView:()=>{lookups++;return undefined;},maybePollRoutes(){},maybePollLogCapture(){}});
  vm.runInContext(extract('function normalizeHost(', '// hostView returns'),c);
  const start=source.indexOf('async fetch(req, env, ctx) {');
  const end=source.indexOf('\n  },\n};',start);
  vm.runInContext(source.slice(start,end).replace('async fetch(','async function routeFetch(')+'\n}',c);
  for(const header of ['   ','.','\t']){
    const req={method:'GET',url:'http://example.test/',headers:{get:()=>header}};
    const res=await c.routeFetch(req,{CH_POOL_MODE:''},{});
    assert.equal(res.status,404);
    assert.equal(res.body,'no route for host');
  }
  assert.equal(lookups,0);
  const req={method:'GET',url:'http://example.test/',headers:{get:()=>'app.test'}};
  const res=await c.routeFetch(req,{CH_POOL_MODE:''},{});
  assert.equal(res.status,404);
  assert.equal(res.body,'no route for host app.test');
  assert.equal(lookups,1);
});
test('kv pinned token signs the binding id, not the env key',async()=>{
  const c=vm.createContext({crypto:webcrypto,TextEncoder,JSON,btoa:s=>Buffer.from(s,'binary').toString('base64')});
  vm.runInContext(extract('async function bindingSpec(', '// scopedToken computes'),c);
  vm.runInContext(extract('async function scopedToken(', 'function text('),c);
  const spec=await c.bindingSpec({SCOPE_SECRET:'secret'},{namespace:'acme'},'web',{version:3,bundle_sha:'sha',bindings:[{type:'kv',name:'KV',id:'acme/__kv__/main'}]});
  assert.equal(spec.KV.name,'KV');
  const claims=JSON.parse(Buffer.from(spec.KV.token.split('.')[0],'base64url').toString());
  assert.deepEqual(claims,{ns:'acme',kind:'kv',name:'KV',worker:'web',version:3,resource:'acme/__kv__/main'});
});
test('explicit version lookup fails closed instead of using another cached version', async()=>{
  let fail=false;const load=harness(async()=>{if(fail)throw Error('offline');return ok(version(1,'same','one'));});
  await load(1);fail=true;await assert.rejects(load(2));
});
test('explicit lookup does not populate active pointer cache',async()=>{
 const calls=[];const load=harness(async url=>{calls.push(url);return ok(url.includes('version=')?version(9,'staged','one'):version(8,'active','one'));});
 await load(9);assert.equal((await load()).version,8);assert.equal(calls.length,2);
});
test('recreated worker same version cannot reuse old SHA or environment',async()=>{
 let current=version(1,'old','one',{MODE:'old'});const load=harness(async()=>ok(current));
 await load(1,'old','one');current=version(1,'new','two',{MODE:'new'});
 assert.equal((await load(1,'new','two')).bundle_sha,'new');
 current=version(1,'new','three',{MODE:'recreated'});
 assert.equal((await load(1,'new','three')).vars.MODE,'recreated');
});
test('cached immutable old identity remains usable by already-pinned requests',async()=>{
 let current=version(1,'old','one');const load=harness(async()=>ok(current));
 await load(1,'old','one');current=version(2,'new','one');await load(2,'new','one');
 assert.equal((await load(1,'old','one')).bundle_sha,'old');
});
test('returned immutable identity must match version SHA and incarnation',async()=>{
 const load=harness(async()=>ok(version(1,'same','old')));
 await assert.rejects(load(2,'same','old'));
 await assert.rejects(load(1,'other','old'));
 await assert.rejects(load(1,'same','new'));
});
test('late pinned 404 cannot poison active lookup',async()=>{
 let release;const load=harness(async url=>url.includes('version=1')?await new Promise(r=>release=r):ok(version(2,'new','one')));
 const old=load(1);await load(2);release({status:404,ok:false});await old;
 assert.equal((await load()).version,2);
});
test('resident version keys include incarnation even for identical code/version',()=>{
 const c=vm.createContext({});vm.runInContext(extract('function workerVersionKey(', 'async function preparedWorker('),c);
 assert.notEqual(c.workerVersionKey({namespace:'ns'},'fn',version(1,'same','old')),c.workerVersionKey({namespace:'ns'},'fn',version(1,'same','new')));
});
test('host deadline aborts a hung response body and releases the flight',async()=>{
 let expire, signal;const entry={value:{id:'cached'},etag:'',expiresAt:0};
 const c=vm.createContext({Date,AbortController,setTimeout:fn=>{expire=fn;return 1;},clearTimeout(){},encodeURIComponent,routeEpoch:0,backoffUntil:0,hostTtlMs:()=>10000,noteSuccess(){},noteFailure(){},fetch:async(_url,opts)=>{signal=opts.signal;return {status:200,headers:new Headers(),json:()=>new Promise(()=>{})};}});
 vm.runInContext(extract('function revalidateHost(', '// workerEnv returns'),c);
 const pending=c.revalidateHost({},'host',entry);const rejected=assert.rejects(pending,/timeout/);
 await Promise.resolve();expire();await rejected;
 assert.equal(signal.aborted,true);assert.equal(entry.inflight,null);assert.equal(entry.value.id,'cached');
});
test('warm host replaces stuck flight and ignores its late response',async()=>{
 let resolveOld, calls=0;const entry={value:{id:'cached'},etag:'',expiresAt:0,inflight:null};
 const c=vm.createContext({Date,AbortController,setTimeout,clearTimeout,encodeURIComponent,routeEpoch:0,backoffUntil:0,hostCache:new Map([['host',entry]]),hostTtlMs:()=>10000,noteSuccess(){},noteFailure(){},fetch:async()=>{calls++;if(calls===1)return await new Promise(r=>resolveOld=r);return {status:200,headers:new Headers(),json:async()=>({id:'new'})};}});
 vm.runInContext(extract('async function hostView(', '// workerEnv returns'),c);
 const old=c.revalidateHost({},'host',entry);entry.inflightAt=Date.now()-6000;
 const pending=[];await c.hostView({},'host',{waitUntil:p=>pending.push(p)});await Promise.all(pending);
 assert.equal(calls,2);assert.equal(entry.value.id,'new');
 resolveOld({status:200,headers:new Headers(),json:async()=>({id:'old'})});await old;
 assert.equal(entry.value.id,'new');
});
test('sampled response and resident release do not wait for telemetry export',async()=>{
 for(const failed of [false,true]) {
  let finishExport, released=0, settled=false;
  const exportPromise=new Promise(resolve=>finishExport=resolve), background=[];
  const c=vm.createContext({preparedCache:new Map([['key',true]]),coldLoads:0,workerVersionKey:()=> 'key',preparedWorker:async()=>({}),tailEnabled:()=>false,residentWorker:async()=>({entry:{},release(){released++;}}),scheduleSweep(){},prepareRequest:request=>({request,sampled:true}),startSpan:()=>({}),endSpan(){},flushSpans:()=>exportPromise,managedStub:()=>({getEntrypoint:()=>({fetch:async()=>{if(failed)throw Error('tenant failed');return {status:200};}})}),text:(_s,status)=>({status})});
  vm.runInContext(extract('async function runWorker(', 'function workerVersionKey('),c);
  const result=c.runWorker({}, {}, {waitUntil:p=>background.push(p)}, {namespace:'ns'}, 'fn', {}, null, []).then(r=>{settled=true;return r;});
  // All mocks except export settle immediately; run through their microtasks.
  for(let i=0;i<20;i++)await Promise.resolve();
  try {assert.equal(settled,true);assert.equal(released,1);assert.equal(background.length,1);assert.equal((await result).status,failed?500:200);}
  finally {finishExport();await result;}
 }
});


test('non-fetch entrypoints keep the isolate alive until the handler settles', async () => {
  const wrapper = readFileSync(new URL('./queue-wrapper.js', import.meta.url), 'utf8')
    .replace('import { WorkerEntrypoint, env as __env } from "cloudflare:workers";', 'const WorkerEntrypoint = class {}; const __env = {};')
    .replace('import { loggedInvocation, dispatchDebug } from "__cellhive-invocation.js";', 'const loggedInvocation=(fn)=>fn(); const dispatchDebug=()=>{};')
    .replace('import * as tenantMod from "tenant.js";', 'const tenantMod={default:{}};')
    .replace('import { buildBindings, wrapR2Metadata, makeKVFromStub, makeD1FromStub, makeDOFromStub, makeQueueFromStub } from "facades.js";', 'const buildBindings=()=>({}); const wrapR2Metadata=x=>x; const makeKVFromStub=x=>x; const makeD1FromStub=x=>x; const makeDOFromStub=x=>x; const makeQueueFromStub=x=>x;')
    .replace('import { runInvocation } from "__cellhive-quota.js";', 'const runInvocation=(fn)=>fn();')
    .replace('export class CellHiveHost', 'class CellHiveHost')
    .replace('export function withR2Metadata', 'function withR2Metadata')
    .replace('export async function handleFetch', 'async function handleFetch')
    .replace(/export default \{[\s\S]*?\n\};/, 'const defaultExport={};')
    + '\nglobalThis.CellHiveHost=CellHiveHost;globalThis.tenantMod=tenantMod;';
  const life = new Lifetime();
  const c = vm.createContext({
    Response, globalThis: null, __cellhivePlatform:{kvBindings:[],d1Bindings:[],r2Bindings:[],queueBindings:[],doBindings:[]}, __cellhiveTailKey:'', __cellhiveLife:life,
  });
  c.globalThis = c;
  vm.runInContext(wrapper, c);
  const host = new c.CellHiveHost();
  host.env = {};
  host.ctx = { waitUntil(){} };

  let resolveQueue;
  c.tenantMod.default.queue = () => new Promise(r => { resolveQueue=r; });
  const queued = host.handleQueue({ queue:'q', messages:[] });
  await Promise.resolve();
  assert.equal(life.idle(), false);
  resolveQueue({ ok:true });
  await queued;
  assert.equal(life.idle(), true);

  let resolveRPC;
  c.tenantMod.default.slow = () => new Promise(r => { resolveRPC=r; });
  const rpc = host.callMethod('', 'slow', [], undefined);
  await Promise.resolve();
  assert.equal(life.idle(), false);
  resolveRPC('ok');
  assert.equal(await rpc, 'ok');
  assert.equal(life.idle(), true);

  c.tenantMod.default.nullValue = () => null;
  assert.equal(await host.callMethod('', 'nullValue', [], undefined), null);
});
