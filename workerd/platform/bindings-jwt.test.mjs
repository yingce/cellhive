import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
import {mintJWT,verifyJWT,internalHeaders} from './jwt.js';
const key=Buffer.alloc(32,7).toString('base64url');
test('cached KV binding refreshes JWT on each call without exposing key in props',async()=>{
 const source=readFileSync(new URL('./bindings.js',import.meta.url),'utf8').replace(/^import .*?;\n/gms,'').replace(/^export /gm,'');
 let now=1700000000;const calls=[];
 const context=vm.createContext({WorkerEntrypoint:class{constructor(ctx,env){this.ctx=ctx;this.env=env}},RpcTarget:class{},mintJWT:(k,id,c)=>mintJWT(k,id,c,now),internalHeaders,URL,Headers,Request,Response,TextEncoder,TextDecoder,makeDOTransport(){}});
 vm.runInContext(source+'\nglobalThis.KV=KV;',context);
 const props={ns:'acme',kind:'kv',name:'KV',authorization:{ns:'acme',kind:'kv',name:'KV',worker:'web',version:1,resource:'acme/__kv__/main'}};
 const binding=new context.KV({props},{CELL_URL:'http://local',BINDING_SIGNING_KEY:key,PLATFORM:{fetch:async(url,opts)=>{calls.push(opts.headers);return new Response('ok')}}});
 await binding.get('first');now+=600;await binding.get('second');
 for(let i=0;i<calls.length;i++){
  const token=new Headers(calls[i]).get('authorization')?.slice(7);
  assert.ok(token,'must send Bearer JWT');
  const claims=await verifyJWT(key,token,'token/scope','binding',1700000000+i*600);
  assert.equal(claims.resource,'acme/__kv__/main');
  assert.equal(claims.iat,1700000000+i*600);
  assert.equal(new Headers(calls[i]).has('x-cellhive-scope-token'),false);
 }
 assert.equal(JSON.stringify(props).includes(key),false);
});

test('binding props cannot override JWT purpose, subject or refresh timestamps', async () => {
 const source=readFileSync(new URL('./bindings.js',import.meta.url),'utf8').replace(/^import .*?;\n/gms,'').replace(/^export /gm,'');
 let sent;
 const context=vm.createContext({WorkerEntrypoint:class{constructor(ctx,env){this.ctx=ctx;this.env=env}},RpcTarget:class{},mintJWT,internalHeaders,Headers});
 vm.runInContext(source+'\nglobalThis.KV=KV;',context);
 const authorization={ns:'acme',kind:'kv',name:'DATA',type:'admin',sub:'attacker',aud:'other',iat:1,exp:2};
 const binding=new context.KV({props:{ns:'acme',authorization}},{CELL_URL:'http://agent',BINDING_SIGNING_KEY:key,PLATFORM:{fetch:async(_url,opts)=>{sent=new Headers(opts.headers);return new Response('ok')}}});
 await binding.get('key');
 const claims=await verifyJWT(key,sent.get('authorization').slice(7),'token/scope','binding');
 assert.ok(claims,'binding purpose and bounded timestamps are owned by the platform');
 assert.equal(claims.sub,'workerd');
 assert.equal(claims.name,'DATA');
});

test('DO transport cache separates binding authorization identities', async () => {
 const source=readFileSync(new URL('./bindings.js',import.meta.url),'utf8').replace(/^import .*?;\n/gms,'').replace(/^export /gm,'');
 const context=vm.createContext({WorkerEntrypoint:class{},RpcTarget:class{},mintJWT,internalHeaders,makeDOTransport:(platform)=>platform});
 vm.runInContext(source+'\nglobalThis.transport=doTransportFor;',context);
 const env={CELL_URL:'http://local',BINDING_SIGNING_KEY:key};
 const props={ns:'acme',worker:'web',class:'Room',version:1,authorization:{ns:'acme',kind:'do',name:'FIRST'}};
 const first=context.transport(env,props);
 const second=context.transport(env,{...props,authorization:{...props.authorization,name:'SECOND'}});
 assert.notEqual(first,second,'a different binding must not reuse another binding credential');
 const claims=await verifyJWT(key,(await second.authorization()).slice(7),'token/scope','binding');
 assert.equal(claims.name,'SECOND');
});

for (const [kind, entrypoint, invoke] of [
 ['d1','D1Database', b => b.exec('select 1')],
 ['r2','R2Bucket', b => b.head('key')],
 ['queue','QueueProducer', b => b.send('hello')],
 ['workflow','WorkflowBinding', b => b.create()],
 ['vectorize','Vectorize', b => b.describe()],
 ['service','ServiceBinding', b => b.fetch(new Request('http://tenant/', {headers:{authorization:'Bearer tenant-secret','x-cellhive-internal-token':'spoof'}}))],
]) test(`${kind} platform entrypoint mints fresh JWT with pinned props`, async () => {
 const source=readFileSync(new URL('./bindings.js',import.meta.url),'utf8').replace(/^import .*?;\n/gms,'').replace(/^export /gm,'');
 let now=1700000000; const calls=[];
 const context=vm.createContext({WorkerEntrypoint:class{constructor(ctx,env){this.ctx=ctx;this.env=env}},RpcTarget:class{},mintJWT:(k,id,c)=>mintJWT(k,id,c,now),internalHeaders,URL,Headers,Request,Response,TextEncoder,TextDecoder,btoa,atob});
 vm.runInContext(source+`\nglobalThis.Binding=${entrypoint};`,context);
 const props={ns:'acme',kind,name:'DATA',target:'target',version:2,authorization:{ns:'acme',kind,name:'DATA'}};
 const binding=new context.Binding({props},{CELL_URL:'http://agent',BINDING_SIGNING_KEY:key,PLATFORM:{fetch:async(url,opts)=>{calls.push({url,...opts});return Response.json({results:[{statement_count:1}],id:'one'})}}});
 await invoke(binding);now+=600;await invoke(binding);
 for(const [i,request] of calls.entries()) {
  const headers=new Headers(request.headers);
  const claims=await verifyJWT(key,headers.get('authorization').slice(7),'token/scope','binding',1700000000+i*600);
  assert.equal(claims.iat,1700000000+i*600);
  assert.equal(claims.kind,kind);assert.equal(claims.name,'DATA');
  assert.equal(headers.has('__bindingClaims'),false);
  assert.equal(headers.has('x-cellhive-internal-token'),false);
  assert.equal(headers.has('x-cellhive-scope-token'),false);
  if(kind==='service') {
   const tenantHeaders=JSON.parse(atob(headers.get('x-cellhive-req-headers')));
   assert.equal(tenantHeaders.authorization,'Bearer tenant-secret');
   assert.equal(tenantHeaders['x-cellhive-internal-token'],undefined);
  }
 }
});
