// Local/disposable acceptance harness only. Signing keys are never Bearer tokens.
const {createHmac} = require('node:crypto');
const {execFileSync} = require('node:child_process');
const {readFileSync} = require('node:fs');
const path = require('node:path');

function mint(encoded, role, scope = {}, now = Math.floor(Date.now()/1000)) {
  const ttl = Number(process.env.E2E_TOKEN_TTL || 300);
  const key = Buffer.from(encoded, 'base64url');
  if (key.length !== 32 || key.toString('base64url') !== encoded) throw new Error('expected canonical base64url 32-byte signing key');
  if (!['admin','internal','peer','dispatch','log','scope'].includes(role)) throw new Error('unsupported JWT purpose');
  const claims = {ver:1,type:role === 'scope' ? 'binding' : role === 'admin' ? 'admin' : 'internal',sub:'e2e',aud:'cellhive',iat:now,exp:now+ttl};
  if (role === 'scope') {
    if (!/^[A-Za-z0-9_.-]+$/.test(scope.ns || '') || scope.ns.startsWith('__') || scope.ns.includes('..') || scope.ns === '.' || !scope.kind || !scope.name) throw new Error('binding requires exact namespace, kind and name');
    Object.assign(claims, {ns:scope.ns,kind:scope.kind,name:scope.name});
  }
  const encode = v => Buffer.from(JSON.stringify(v)).toString('base64url');
  const data = encode({alg:'HS256',typ:'JWT',kid:`token/${role}`})+'.'+encode(claims);
  return data+'.'+createHmac('sha256',key).update(data).digest('base64url');
}

const keys = new Map();
function token(role, scope = {}) {
  if (!keys.has(role)) {
    const cli = path.join(process.env.E2E_BIN || path.resolve(__dirname,'../../bin'), 'cellhive');
    keys.set(role, execFileSync(cli, ['creds',role], {encoding:'utf8'}).trim());
  }
  return mint(keys.get(role), role, scope);
}
module.exports = {mint, token};
if (require.main === module) {
  try {
    const [role, ns, kind, name] = process.argv.slice(2);
    const scope = {ns,kind,name};
    process.stdout.write(ns === '--stdin' ? mint(readFileSync(0,'utf8').trim(),role) : token(role,scope));
  } catch (e) { console.error(e.message); process.exitCode = 1; }
}
