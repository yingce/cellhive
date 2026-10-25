import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import test from 'node:test';
const source = readFileSync(new URL('./internal.js', import.meta.url), 'utf8');
function harness(metadata) {
  const context = vm.createContext({ Request, Response, URL, TextEncoder, console, crypto: { randomUUID: ()=>'test' },
    bindingStub(){}, workerLimits:()=>({}), quotaSource:()=>'', invocationSource:()=>'',
    checkedWorkerGet: async (_loader,id,code)=>({id,code,getEntrypoint:()=>({handleFetch:async()=>new Response('ok'),callMethod:async()=> 'ok'})}),
    budgetErrorBody:()=>null,
    fetch:async url=>url.includes('/worker/bindings') ? new Response(JSON.stringify(metadata())) : url.includes('/bundle-url') ? new Response('',{status:404}) : url.includes('/log-capture') ? new Response('{"mode":"off"}') : new Response('export default {}') });
  vm.runInContext(readFileSync(new URL('../platform/lifecycle.js', import.meta.url),'utf8').replace(/^export /gm,''), context);
  vm.runInContext(source.replace(/^import .*;$/gm,'').replace(/^export \{.*;$/gm,'').replace('export default {','const entry = {')+'\nglobalThis.entry = entry;', context);
  context.env={CELL_URL:'http://local',DISPATCH_TOKEN:'tok',CH_LOG_CAPTURE:'0'};
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
 const response=await c.entry.fetch(new Request('http://local/unknown',{method:'POST',headers:{'x-cellhive-internal-token':'tok'},body:JSON.stringify({...body,storage_id:provided})}),c.env,{});
 assert.equal(response.status,status);
});
