import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import test from 'node:test';
const source = readFileSync(new URL('./loader.js', import.meta.url), 'utf8');
function extract(start, end) { return source.slice(source.indexOf(start), source.indexOf(end, source.indexOf(start))); }
function harness(fetch) {
  const c = vm.createContext({ envCache:new Map(), envInflight:new Map(), Date, encodeURIComponent, JSON, fetch, noteFailure(){}, noteSuccess(){}, hostTtlMs(){return 10000;}, routeEpoch:0 });
  vm.runInContext(extract('async function workerEnv(', '// matchPath picks'), c);
  return (...args) => c.workerEnv({CELL_URL:'http://mock'}, 'ns', 'fn', ...args);
}
const ok = v => ({ok:true,status:200,json:async()=>v});
const version = (number, sha, storage, vars={}) => ({version:number,bundle_sha:sha,storage_id:storage,vars});
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
