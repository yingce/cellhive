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
