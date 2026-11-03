const {test} = require('node:test');
const assert = require('node:assert/strict');
const {createHmac} = require('node:crypto');
const {mint} = require('./jwt.cjs');
const {execFileSync, spawnSync} = require('node:child_process');
const path = require('node:path');

test('role keys mint short-lived purpose-separated JWTs, not key bearers', () => {
  const key = Buffer.alloc(32, 7).toString('base64url');
  for (const [role, type] of [['admin','admin'], ['internal','internal'], ['peer','internal'], ['dispatch','internal'], ['log','internal'], ['scope','binding']]) {
    const token = mint(key, role, role === 'scope' ? {ns:'test',kind:'kv',name:'KV'} : {}, 1700000000);
    const [h,p,s] = token.split('.');
    assert.deepEqual(JSON.parse(Buffer.from(h,'base64url')), {alg:'HS256',typ:'JWT',kid:`token/${role}`});
    const claims = JSON.parse(Buffer.from(p,'base64url'));
    assert.equal(claims.type, type);
    assert.equal(claims.ver, 1);
    assert.equal(claims.aud, 'cellhive');
    assert.equal(claims.exp-claims.iat, 300);
    assert.equal(s, createHmac('sha256',Buffer.from(key,'base64url')).update(`${h}.${p}`).digest('base64url'));
    assert.notEqual(token, mint(key, role, role === 'scope' ? {ns:'test',kind:'kv',name:'KV'} : {}, 1700000301));
  }
});

test('reject malformed keys, purposes and binding scope', () => {
  const key = Buffer.alloc(32).toString('base64url');
  for (const bad of ['secret', key+'=', key+'\n']) assert.throws(() => mint(bad,'admin'));
  assert.throws(() => mint(key,'unknown'));
  assert.throws(() => mint(key,'scope'));
  assert.throws(() => mint(key,'scope',{ns:'*',kind:'kv',name:'KV'}));
});

test('stdin mode accepts container-derived keys without putting them in arguments', () => {
  const helper = path.join(__dirname, 'jwt.cjs');
  const key = Buffer.alloc(32, 9).toString('base64url');
  const token = execFileSync(process.execPath, [helper,'internal','--stdin'], {input:key+'\n',encoding:'utf8'});
  const [h,p,s] = token.split('.');
  assert.equal(JSON.parse(Buffer.from(h,'base64url')).kid, 'token/internal');
  assert.equal(JSON.parse(Buffer.from(p,'base64url')).exp-JSON.parse(Buffer.from(p,'base64url')).iat, 300);
  assert.equal(s,createHmac('sha256',Buffer.from(key,'base64url')).update(`${h}.${p}`).digest('base64url'));
  const rejected = spawnSync(process.execPath,[helper,'admin','--stdin'],{input:'not-a-key',encoding:'utf8'});
  assert.notEqual(rejected.status,0);
  assert.equal(rejected.stdout,'');
  assert.ok(!rejected.stderr.includes('not-a-key'));
});
