import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
const source=readFileSync(new URL('./facades.js',import.meta.url),'utf8').replace(/import\s*\{[^}]+\}\s*from\s*"[^"]+";/g,'').replace(/^export /gm,'');
function transport(fetcher){
 const c=vm.createContext({Request,Response,Headers,URL,Uint8Array,TextEncoder,TextDecoder,btoa,atob,console});
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
