import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
import {mintJWT,verifyJWT,authorized,bearer,internalHeaders} from '../platform/jwt.js';
const key=Buffer.alloc(32,7).toString('base64url');
const source=readFileSync(new URL('./host.js',import.meta.url),'utf8');
function harness(){
 const c=vm.createContext({authorized,bearer,verifyJWT,Date,SHARD_COUNT:16,Headers,Request});
 vm.runInContext(source.slice(source.indexOf('async function authorize('),source.indexOf('function json(')),c);
 vm.runInContext(source.slice(source.indexOf('function tenantRequest('),source.indexOf('export class Host')),c);
 return c;
}
test('owner gate accepts dedicated ticket and rejects legacy, wrong purpose and malformed shard',async()=>{
 const c=harness(),env={CELL_TOKEN:key,DO_TICKET_SECRET:key};
  const token=await mintJWT(key,'token/do-ticket',{type:'internal',sub:'ticket',ns:'acme',worker:'web',storage_class:'Room',shard:0,execution:JSON.stringify({class:'Room',bundle_sha:'bundle',storage_id:'incarnation',version:1,limits:null}),exp:Math.floor(Date.now()/1000)+30});
 assert.equal((await c.authorize(new Request('http://local',{headers:{authorization:'Bearer '+token}}),env)).ticket.shard,0);
 for(const headers of [{'x-cellhive-do-ticket':token},await internalHeaders(key,'dispatch'),{authorization:'Bearer '+await mintJWT(key,'token/do-ticket',{type:'internal',sub:'ticket',ns:'acme',worker:'web',storage_class:'Room',shard:16})}]){
  assert.equal((await c.authorize(new Request('http://local',{headers}),env)).ok,false);
 }
});
test('WebSocket tenant request restores encapsulated auth and strips platform credentials',()=>{
 const c=harness();
 const req=c.tenantRequest(new Request('http://local',{headers:{authorization:'Bearer host-jwt','x-cellhive-app-authorization':'Bearer tenant-token','x-cellhive-extra':'private'}}));
 assert.equal(req.headers.get('authorization'),'Bearer tenant-token');
 assert.equal(req.headers.has('x-cellhive-app-authorization'),false);
 assert.equal(req.headers.has('x-cellhive-extra'),false);
 const absent=c.tenantRequest(new Request('http://local',{headers:{authorization:'Bearer host-jwt'}}));
 assert.equal(absent.headers.has('authorization'),false);
});
test('wrong shard ticket cannot publish object index side effects',async()=>{
 let published=0;
 const c=vm.createContext({draining:false,fnv1a:()=>3,SHARD_COUNT:16,indexObject:async()=>{published++},json:(body,status)=>({body,status}),seen:new Map(),SEEN_MAX:100});
 vm.runInContext(source.slice(source.indexOf('function ticketSpec('),source.indexOf('async function forward(')),c);
 const response=await c.invoke({json:async()=>({namespace:'acme',worker:'web',class:'Room',id:'object',bundle_sha:'bundle'})},{},{ns:'other',worker:'web',storage_class:'Room',shard:3});
 assert.equal(response.status,403);assert.equal(published,0);assert.equal(c.seen.size,0);
});

test('ticket cannot substitute executable identity or dispatch alarms before forwarding',async()=>{
 const base={namespace:'acme',worker:'web',class:'Room',id:'object',bundle_sha:'bundle',storage_id:'incarnation',version:2};
 const ticket={ns:'acme',worker:'web',storage_class:'Room',shard:3,execution:JSON.stringify({class:'Room',bundle_sha:'bundle',storage_id:'incarnation',version:2,limits:null})};
 for(const change of [{class:'Other',storage_class:'Room'},{bundle_sha:'other'},{storage_id:'other'},{version:3},{version:'2e0'},{kind:'alarm'},{compat_flags:['nodejs_compat']}]){
  let sideEffects=0;
  const c=vm.createContext({draining:false,fnv1a:()=>3,SHARD_COUNT:16,indexObject:async()=>{sideEffects++},json:(body,status)=>({body,status}),seen:new Map(),SEEN_MAX:100,hints:new Map([['acme/web/Room/shard3',{expiry:Date.now()+10000,address:'other'}]]),Date,forward:async()=>{sideEffects++;return {status:200}}});
  vm.runInContext(source.slice(source.indexOf('function ticketSpec('),source.indexOf('async function forward(')),c);
  const result=await c.invoke({json:async()=>({...base,...change})},{},ticket);
  assert.equal(result.status,403,JSON.stringify(change));assert.equal(sideEffects,0);
 }
});

test('matching shard cache ticket keeps signed limits when forwarding a different object',async()=>{
 const forwarded=[];
 const c=vm.createContext({draining:false,fnv1a:()=>3,SHARD_COUNT:16,indexObject:async()=>{},json:(body,status)=>({body,status}),seen:new Map(),SEEN_MAX:100,hints:new Map([['acme/web/Room/shard3',{expiry:Date.now()+10000,address:'other'}]]),Date,forward:async(_env,_address,spec)=>{forwarded.push(spec);return {status:200}}});
 vm.runInContext(source.slice(source.indexOf('function ticketSpec('),source.indexOf('async function forward(')),c);
 const ticket={ns:'acme',worker:'web',storage_class:'Room',shard:3,execution:JSON.stringify({class:'Room',bundle_sha:'bundle',storage_id:'incarnation',version:2,limits:{subrequests:4}})};
 for(const id of ['object-a','object-b']){
  const result=await c.invoke({json:async()=>({namespace:'acme',worker:'web',class:'Room',id,bundle_sha:'bundle',storage_id:'incarnation',version:2,limits:{subrequests:99999}})},{},ticket);
  assert.equal(result.status,200);
 }
 assert.equal(forwarded.length,2);
 assert.equal(forwarded[1].limits.subrequests,4);
});
