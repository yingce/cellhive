import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
const source=readFileSync(new URL('./host.js',import.meta.url),'utf8');
function block(start,end){return source.slice(source.indexOf(start),source.indexOf(end,source.indexOf(start)));}
function harness(post){
 const c=vm.createContext({claims:new Map(),post,json:x=>x,leaseTtlS:()=>30,AbortController,setTimeout,clearTimeout});
 vm.runInContext(block('async function renewLease(', 'async function drain('),c);
 return c;
}
const claim=()=>({epoch:1,spec:{namespace:'ns',worker:'w',class:'C'},shard:1,expiryMs:0});
test('transport error of one lease does not stop renewing the others',async()=>{
 const expiry=Date.now()+30000;let n=0;const c=harness(async()=>{if(++n===1)throw Error('offline');return Response.json({epoch:1,expiry_ms:expiry})});
 c.claims.set('first',claim());c.claims.set('second',claim());
 const result=await c.renewAll({});assert.equal(result.lost,1);assert.equal(result.renewed,1);assert.equal(c.claims.has('first'),false);assert.equal(c.claims.get('second').expiryMs,expiry);
});
test('late failed renewal cannot delete a replacement claim',async()=>{
 let finish;const c=harness(()=>new Promise(r=>finish=r));const replacement=claim();replacement.epoch=2;
 c.claims.set('key',claim());const pending=c.renewAll({});c.claims.set('key',replacement);finish(new Response('',{status:409}));await pending;
 assert.equal(c.claims.get('key'),replacement);
});

test('renewal deadline also covers a hung response body',async()=>{
 let expire,signal;
 const c=harness(async(_env,_path,_body,s)=>{signal=s;return {ok:true,json:()=>new Promise(()=>{})}});
 c.setTimeout=fn=>{expire=fn;return 1};c.clearTimeout=()=>{};
 c.claims.set('key',claim());const pending=c.renewAll({});await Promise.resolve();expire();
 assert.equal((await pending).lost,1);assert.equal(signal.aborted,true);assert.equal(c.claims.size,0);
});

test('temporary renewal error retains the confirmed expiry and retries without claiming again',async()=>{
 let calls=0; const expiry=Date.now()+30000;
 const c=harness(async()=>{if(++calls===1)return new Response('',{status:503});return Response.json({epoch:1,expiry_ms:expiry+1000});});
 const value={...claim(),expiryMs:expiry};c.claims.set('key',value);
 assert.equal((await c.renewAll({})).lost,0);
 assert.equal(c.claims.get('key'),value);assert.equal(value.expiryMs,expiry);
 assert.equal((await c.renewAll({})).renewed,1);assert.equal(value.expiryMs,expiry+1000);
});
test('definitive owner rejection drops even a locally unexpired claim',async()=>{
 const c=harness(async()=>new Response('',{status:409}));
 c.claims.set('key',{...claim(),expiryMs:Date.now()+30000});
 assert.equal((await c.renewAll({})).lost,1);assert.equal(c.claims.size,0);
});

function claimHarness({post, fetch, restore}){
 const c=vm.createContext({
  claims:new Map(), pendingClaims:new Map(), GUARD_MS:3000,
  post, fetch, restoreClaimedHost: restore || (async()=>null),
  json:(o,status)=>new Response(JSON.stringify(o),{status:status||200,headers:{'content-type':'application/json'}}),
  Date, AbortController, setTimeout, clearTimeout, leaseTtlS:()=>30,
 });
 const ensure=block('async function ensureClaim(env, spec, shard) {','// connect upgrades');
 const renew=block('async function renewLease(','async function drain(');
 vm.runInContext(ensure+renew+'\nglobalThis.ensureClaim=ensureClaim;',c);
 return c;
}
const spec={namespace:'platform',worker:'workbench',class:'SessionDO',storage_class:'SessionDO'};
const shardKey='platform/workbench/SessionDO/shard7';

test('a renew failure inside the guard claims again before the request runs',async()=>{
 const paths=[];
 const c=claimHarness({post:async(_env,path)=>{
  paths.push(path);
  if(path==='/v1/internal/do/renew') return new Response('',{status:503});
  return Response.json({epoch:9,expiry_ms:Date.now()+30000});
 }});
 const held={epoch:8,spec:{namespace:'platform',worker:'workbench',class:'SessionDO'},shard:7,expiryMs:Date.now()+2000};
 c.claims.set(shardKey,held);
 const out=await c.ensureClaim({NODE_ID:'migration-test-do',ADVERTISE:'do-runtime:8788'},spec,7);
 assert.equal(out,null);
 assert.equal(c.claims.get(shardKey).epoch,9);
 assert.notEqual(c.claims.get(shardKey),held);
 assert.deepEqual(paths,['/v1/internal/do/renew','/v1/internal/do/claim']);
});

test('a second request waits for the claim that is restoring the host',async()=>{
 let renews=0;
 let releaseRenew;
 const renewGate=new Promise(r=>{releaseRenew=r;});
 let claimsStarted=0;
 let releaseRestore;
 const restoreGate=new Promise(r=>{releaseRestore=r;});
 const c=claimHarness({
  post:async(_env,path)=>{
   if(path==='/v1/internal/do/renew'){
    renews++;
    await renewGate;
    return new Response('',{status:503});
   }
   claimsStarted++;
   return Response.json({epoch:9,expiry_ms:Date.now()+30000});
  },
  restore:async()=>{await restoreGate;return null;},
 });
 c.claims.set(shardKey,{epoch:8,spec:{namespace:'platform',worker:'workbench',class:'SessionDO'},shard:7,expiryMs:Date.now()+2000});
 const env={NODE_ID:'migration-test-do',ADVERTISE:'do-runtime:8788'};
 const first=c.ensureClaim(env,spec,7);
 const second=c.ensureClaim(env,spec,7);
 while(renews<2) await Promise.resolve();
 releaseRenew();
 let secondDone=false;
 second.then(()=>{secondDone=true;});
 await new Promise(r=>setTimeout(r,20));
 assert.equal(secondDone,false);
 assert.equal(claimsStarted,1);
 releaseRestore();
 const [a,b]=await Promise.all([first,second]);
 assert.equal(a,null);
 assert.equal(b,null);
 assert.equal(c.claims.get(shardKey).epoch,9);
 assert.equal(claimsStarted,1);
});

test('a dead lease is claimed again on the same request',async()=>{
 const paths=[];
 const c=claimHarness({post:async(_env,path)=>{
  paths.push(path);
  if(path==='/v1/internal/do/renew') return new Response('',{status:409});
  return Response.json({epoch:9,expiry_ms:Date.now()+30000});
 }});
 c.claims.set(shardKey,{epoch:8,spec:{namespace:'platform',worker:'workbench',class:'SessionDO'},shard:7,expiryMs:Date.now()+2000});
 const out=await c.ensureClaim({NODE_ID:'migration-test-do',ADVERTISE:'do-runtime:8788'},spec,7);
 assert.equal(out,null);
 assert.equal(c.claims.get(shardKey).epoch,9);
 assert.deepEqual(paths,['/v1/internal/do/renew','/v1/internal/do/claim']);
});

test('a managed lease that the supervisor already dropped is claimed again',async()=>{
 const paths=[];
 const c=claimHarness({
  post:async(_env,path)=>{
   paths.push(path);
   return Response.json({epoch:10,expiry_ms:Date.now()+30000});
  },
  fetch:async()=>new Response(JSON.stringify({error:'DO lease is not managed'}),{status:409}),
 });
 c.claims.set(shardKey,{epoch:8,managed:true,spec:{namespace:'platform',worker:'workbench',class:'SessionDO'},shard:7,expiryMs:Date.now()+2000});
 const out=await c.ensureClaim({NODE_ID:'migration-test-do',GATE_URL:'http://do-runtime:18901',ADVERTISE:'do-runtime:8788'},spec,7);
 assert.equal(out,null);
 assert.equal(c.claims.get(shardKey).epoch,10);
 assert.deepEqual(paths,['/v1/internal/do/claim']);
});

test('reclaim forwards when another node still holds the live lease',async()=>{
 const c=claimHarness({post:async(_env,path)=>{
  if(path==='/v1/internal/do/renew') return new Response('',{status:409});
  return Response.json({address:'http://other-do:8788'},{status:409});
 }});
 c.claims.set(shardKey,{epoch:8,spec:{namespace:'platform',worker:'workbench',class:'SessionDO'},shard:7,expiryMs:Date.now()-1});
 const out=await c.ensureClaim({NODE_ID:'migration-test-do',ADVERTISE:'do-runtime:8788'},spec,7);
 assert.equal(out.forwardTo,'http://other-do:8788');
 assert.equal(c.claims.has(shardKey),false);
});
