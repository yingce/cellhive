import test from 'node:test';
import assert from 'node:assert/strict';
import { webcrypto, createHmac } from 'node:crypto';
import { readFile } from 'node:fs/promises';
if (!globalThis.crypto) Object.defineProperty(globalThis, 'crypto', {value:webcrypto});
const key = Buffer.from(Array.from({length:32}, (_,i)=>i)).toString('base64url');
async function module() {
  const source = await readFile(new URL('./jwt.js', import.meta.url), 'utf8');
  return import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
}
test('JWT signer uses decoded key, standard header and second-based bounded claims', async () => {
  const { mintJWT, verifyJWT } = await module();
  const token = await mintJWT(key, 'token/internal', {type:'internal',sub:'workerd'}, 1700000000);
  const [h,p,s] = token.split('.');
  assert.deepEqual(JSON.parse(Buffer.from(h,'base64url')), {alg:'HS256',typ:'JWT',kid:'token/internal'});
  assert.deepEqual(JSON.parse(Buffer.from(p,'base64url')), {ver:1,type:'internal',sub:'workerd',aud:'cellhive',iat:1700000000,exp:1700000300});
  assert.equal(s, createHmac('sha256',Buffer.from(key,'base64url')).update(h+'.'+p).digest('base64url'));
  assert.ok(await verifyJWT(key,token,'token/internal','internal',1700000001));
  for (const [kid,type,now] of [['token/log','internal',1700000001],['token/internal','binding',1700000001],['token/internal','internal',1700000300],['token/internal','internal',1699999969]]) {
    assert.equal(await verifyJWT(key,token,kid,type,now),null);
  }
  assert.equal(await verifyJWT(key,token+'x','token/internal','internal',1700000001),null);
});
test('JWT runtime verification shares Go clock skew and size bounds', async () => {
  const {mintJWT, verifyJWT}=await module();
  const token=await mintJWT(key,'token/internal',{type:'internal',sub:'workerd'},1700000000);
  assert.ok(await verifyJWT(key,token,'token/internal','internal',1699999970));
  assert.equal(await verifyJWT(key,token,'token/internal','internal',1699999969),null);
  const large=await mintJWT(key,'token/internal',{type:'internal',sub:'x'.repeat(17000)},1700000000);
  assert.equal(await verifyJWT(key,large,'token/internal','internal',1700000001),null);
});
test('Bearer gate rejects legacy credentials and malformed authorization', async () => {
  const { internalHeaders, authorized } = await module();
  const headers = await internalHeaders(key,'dispatch');
  assert.equal(Object.keys(headers).join(','),'authorization');
  assert.equal(await authorized(new Request('http://local',{headers}),key,'dispatch'),true);
  assert.equal(await authorized(new Request('http://local',{headers}),key,'internal'),false);
  assert.equal(await authorized(new Request('http://local',{headers:{'x-cellhive-internal-token':key}}),key,'dispatch'),false);
  assert.equal(await authorized(new Request('http://local',{headers:{...headers,'x-cellhive-internal-token':key}}),key,'dispatch'),false);
});
test('JWT signing rejects noncanonical or wrong-length signing keys',async()=>{
  const { mintJWT }=await module();
  for(const invalid of ['', 'secret', key+'=', key.replace(/.$/,'B')]) await assert.rejects(mintJWT(invalid,'token/internal',{type:'internal',sub:'workerd'}));
});
test('JWT verification rejects duplicate claims and header properties',async()=>{
 const {verifyJWT}=await module();
 const validHeader='{"alg":"HS256","typ":"JWT","kid":"token/internal"}';
 const validPayload='{"ver":1,"type":"internal","sub":"workerd","aud":"cellhive","iat":1700000000,"exp":1700000300}';
 for(const [header,payload] of [[validHeader.replace('"alg":','"alg":"none","alg":'),validPayload],[validHeader,validPayload.replace('"sub":','"sub":"other","sub":')]]){
  const input=Buffer.from(header).toString('base64url')+'.'+Buffer.from(payload).toString('base64url');
  const token=input+'.'+createHmac('sha256',Buffer.from(key,'base64url')).update(input).digest('base64url');
  assert.equal(await verifyJWT(key,token,'token/internal','internal',1700000001),null);
 }
});
