import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const source=readFileSync(new URL('./facades.js',import.meta.url),'utf8');
const context=vm.createContext({});
vm.runInContext(source.slice(source.indexOf('export function wrapR2Metadata'),source.indexOf('export function buildBindings')).replace('export function','function'),context);
const {wrapR2Metadata}=context;
for (const method of ['get','head','put']) {
 test(`R2 ${method} metadata writes caller Headers`,async()=>{
  const raw={httpMetadata:{contentType:'text/plain',cacheControl:'max-age=60'}};
  const bucket=wrapR2Metadata({[method]:async()=>Object.freeze(raw)});
  const object=await bucket[method]('key');
  const headers=new Headers();object.writeHttpMetadata(headers);
  assert.equal(headers.get('content-type'),'text/plain');
  assert.equal(headers.get('cache-control'),'max-age=60');
  assert.equal(await wrapR2Metadata({[method]:async()=>null})[method]('missing'),null);
 });
}
