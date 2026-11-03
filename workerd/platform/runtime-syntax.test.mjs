import { readFileSync, readdirSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import assert from 'node:assert/strict';
import test from 'node:test';

// Explicit module mode is essential: Node's .js autodetection can accept an
// invalid ES module as CommonJS during --check without reporting the error.
for (const directory of ['platform', 'user-runtime', 'do-runtime']) {
  const base = new URL(`../${directory}/`, import.meta.url);
  for (const file of readdirSync(base).filter(name => name.endsWith('.js'))) {
    test(`complete production module parses: ${directory}/${file}`, () => {
      const result = spawnSync(process.execPath, ['--input-type=module', '--check'], {
        input: readFileSync(new URL(file, base)), encoding: 'utf8', timeout: 10000,
      });
      assert.ifError(result.error);
      assert.equal(result.status, 0, result.stderr);
    });
  }
}
