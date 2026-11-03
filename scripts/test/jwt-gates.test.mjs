import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync, readdirSync} from 'node:fs';
import {resolve, join} from 'node:path';
import {spawnSync} from 'node:child_process';

const root = resolve(import.meta.dirname, '..');
function files(dir) {
  return readdirSync(dir, {withFileTypes:true}).flatMap(entry => entry.isDirectory() ? files(join(dir, entry.name)) : [join(dir, entry.name)]);
}
test('executable scripts have no retired platform authentication headers or key derivations', () => {
  const retired = new RegExp('x-cellhive-'+'(?:admin|scope|internal|peer|dispatch|log)-token', 'i');
  for (const file of files(root).filter(f => /\.(sh|py|cjs|mjs)$/.test(f) && !f.includes('.test.'))) {
    const source = readFileSync(file, 'utf8');
    assert.ok(!retired.test(source), `${file}: retired authentication header`);
    assert.ok(!source.includes('cellhive/'+'token/'), `${file}: retired signing derivation`);
  }
});

test('shell gates and embedded Python fixtures parse without executing external operations', () => {
  const gates = ['deploy-e2e.sh','crash-e2e.sh','rpo-zero-fault.sh','bench-capture-cells.sh','bench-fleet.sh','k3d-runtime-e2e.sh','cf-tail-multinode-e2e.sh','runtime-baseline-e2e.sh','e2e/owner-lease.sh','e2e/owner-takeover.sh','e2e/cold-write-cos.sh','e2e/mysql-auto.sh'];
  for (const gate of gates) {
    const file = join(root, gate);
    const shell = spawnSync('bash', ['-n', file], {encoding:'utf8'});
    assert.equal(shell.status, 0, `${gate}: ${shell.stderr}`);
    const source = readFileSync(file, 'utf8');
    for (const match of source.matchAll(/<<'PY'[^\n]*\n([\s\S]*?)\nPY(?:\n|$)/g)) {
      const python = spawnSync('python3', ['-c', 'import sys; compile(sys.stdin.read(), "fixture", "exec")'], {input:match[1],encoding:'utf8'});
      assert.equal(python.status, 0, `${gate}: ${python.stderr}`);
    }
  }
});
