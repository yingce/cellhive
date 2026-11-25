import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { AsyncLocalStorage } from 'node:async_hooks';
import test from 'node:test';
import assert from 'node:assert/strict';
const src=n=>readFileSync(new URL(n,import.meta.url),'utf8').replace(/^import .*?;\n/gms,'').replace(/^export default/gm,'const defaultExport =').replace(/^export /gm,'');
test('binding calls retain explicit request trace through signing interleaving',async()=>{
 let unblock;const gate=new Promise(r=>unblock=r),sent=[];
 const ctx=vm.createContext({AsyncLocalStorage,Headers,Request,Response,URL,TextEncoder,TextDecoder,WorkerEntrypoint:class{constructor(ctx,env){this.ctx=ctx;this.env=env;}},RpcTarget:class{},mintJWT:()=>gate});
 // Separate module scopes, as in workerd.
 vm.runInContext('{'+src('facades.js')+';Object.assign(globalThis,{runWithTrace,currentTrace,traceStub,makeDOTransport});}',ctx);
 vm.runInContext(src('bindings.js')+';globalThis.KV=KV;',ctx);
 const raw=new ctx.KV({props:{ns:'n',authorization:{}}},{CELL_URL:'http://agent',PLATFORM:{fetch:async(_,init)=>{sent.push(init.headers.traceparent);return new Response('ok');}}});
 const stub=ctx.traceStub(raw);
 const a=ctx.runWithTrace({traceparent:'A'},()=>stub.get('a'));
 const b=ctx.runWithTrace({traceparent:'B'},()=>stub.get('b'));
 ctx.__cellhiveTraceparent='WRONG';unblock('jwt');await Promise.all([a,b]);
 assert.deepEqual(sent,['A','B']);assert.equal(ctx.currentTrace().traceparent,'');
});
test('Queue wrapper concurrent fetch, cleanup and background retain own context',async()=>{
 const ctx=vm.createContext({AsyncLocalStorage,Request,Response,Headers,URL,WorkerEntrypoint:class{constructor(ctx,env){this.ctx=ctx;this.env=env;}},__env:{},__cellhivePlatform:{kvBindings:[],d1Bindings:[],queueBindings:[],doBindings:[],r2Bindings:[],tracedBindings:[]},runInvocation:fn=>fn()});
 vm.runInContext('{'+src('facades.js')+';Object.assign(globalThis,{runWithTrace,currentTrace,traceStub,prepareBindingEnv});}',ctx);
 let release;const gate=new Promise(r=>release=r),seen=[];
 ctx.tenantMod={default:{async fetch(req){if(req.url.endsWith('/a'))await gate;seen.push(ctx.currentTrace().traceparent);return new Response('ok');}}};
 vm.runInContext(src('../user-runtime/queue-wrapper.js')+';globalThis.Host=CellHiveHost;',ctx);
 const host=new ctx.Host({},{});const a=host.fetch(new Request('http://t/a',{headers:{traceparent:'A'}}));await host.fetch(new Request('http://t/b',{headers:{traceparent:'B'}}));release();await a;
 assert.deepEqual(seen,['B','A']);assert.equal(ctx.currentTrace().traceparent,'');
});

test('Workflow overlap and waitUntil tasks retain context after another invocation completes',async()=>{
 const ctx=vm.createContext({AsyncLocalStorage,Request,Response,Headers,URL,TextEncoder,TextDecoder,btoa,atob,WorkerEntrypoint:class{constructor(ctx,env){this.ctx=ctx;this.env=env;}},__env:{},__cellhivePlatform:{},runInvocation:fn=>fn(),NonRetryableError:class extends Error{},WorkflowTimeoutError:class extends Error{}});
 vm.runInContext('{'+src('facades.js')+';Object.assign(globalThis,{runWithTrace,currentTrace,prepareBindingEnv});}',ctx);
 let release;const gate=new Promise(r=>release=r),seen=[],background=[];
 ctx.tenant={Flow:class {async run(event){if(event==='a')await gate;seen.push(ctx.currentTrace().traceparent);background.push(gate.then(()=>seen.push('bg:'+ctx.currentTrace().traceparent)));return 'ok';}}};
 vm.runInContext(src('../user-runtime/workflow-wrapper.js')+';globalThis.Flow=CellHiveWorkflow;',ctx);
 const host=new ctx.Flow({},{}),bridge={workflowStep:async()=>({status:200,body:'{}'})};
 const a=host.handleRun('Flow',{instanceId:'a',event:'a',traceparent:'A'},bridge);
 await host.handleRun('Flow',{instanceId:'b',event:'b',traceparent:'B'},bridge);release();await a;await Promise.all(background);
 assert.equal(seen.filter(x=>x==='A').length,1);assert.equal(seen.filter(x=>x==='B').length,1);
 assert.ok(seen.includes('bg:A'));assert.ok(seen.includes('bg:B'));assert.equal(ctx.currentTrace().traceparent,'');
});

test('traced dispatch cannot invoke internal helpers or object prototype methods',async()=>{
 const ctx=vm.createContext({WorkerEntrypoint:class{constructor(ctx,env){this.ctx=ctx;this.env=env;}},RpcTarget:class{},runWithTrace:(_c,fn)=>fn()});
 vm.runInContext(src('bindings.js')+';globalThis.KV=KV;',ctx);
 const kv=new ctx.KV({props:{}},{});
 for(const method of ['constructor','traceCall','__call','toString','env']) assert.throws(()=>kv.traceCall(method,[],{}));
});

test('trace RPC preserves local workflow get quota exemption and counts network operations',async()=>{
 const {quotaSource}=await import('./lifecycle.js');
 const env={WF:{async traceCall(method){return method==='get'?{id:'one'}:{};}}};
 const ctx=vm.createContext({AsyncLocalStorage,env,fetch:async()=>new Response(),Response});
 vm.runInContext(quotaSource(1,{WF:{kind:'workflow'}}).replace(/^\s*import .*?;$/gm,'').replace('export const runInvocation','const runInvocation')+';globalThis.run=runInvocation;',ctx);
 await ctx.run(async()=>{
  await env.WF.traceCall('get',['one'],{});
  await env.WF.traceCall('create',[],{});
  await assert.rejects(async()=>env.WF.traceCall('create',[],{}),/subrequest limit/);
 });
});

test('KV micro-batches never combine requests with different invocation contexts',async()=>{
 const ctx=vm.createContext({AsyncLocalStorage,queueMicrotask,TextDecoder,Uint8Array,atob});
 vm.runInContext(src('facades.js')+';Object.assign(globalThis,{runWithTrace,currentTrace,makeKVFromStub});',ctx);
 const seen=[];
 const kv=ctx.makeKVFromStub({async getBatch(keys){seen.push({keys:[...keys],trace:ctx.currentTrace().traceparent});return keys.map(()=>({found:false}));}});
 await Promise.all([ctx.runWithTrace({traceparent:'A'},()=>Promise.all([kv.get('a1'),kv.get('a2')])),ctx.runWithTrace({traceparent:'B'},()=>kv.get('b'))]);
 assert.deepEqual(seen,[{keys:['a1','a2'],trace:'A'},{keys:['b'],trace:'B'}]);
});
