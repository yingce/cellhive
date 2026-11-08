import { readFileSync } from 'node:fs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { CountLRU, SOURCE_CACHE_BYTES, textWeight } from '../platform/lifecycle.js';

const source = readFileSync(new URL('./host.js', import.meta.url), 'utf8');

test('DO seen registry is a count-bounded LRU', () => {
  const seen = new CountLRU(3);
  for (let i = 0; i < 10; i++) seen.set('object-' + i, { id: i });
  assert.equal(seen.size, 3);
  assert.equal(seen.has('object-9'), true);
  assert.equal(seen.has('object-0'), false);
});

test('seen is bounded, facet version identity is retained, facet stubs are count-bounded', () => {
  assert.match(source, /const seen = new CountLRU\(SEEN_MAX\)/);
  assert.match(source, /#facets = new CountLRU\(FACET_CACHE_MAX\)/);
  assert.match(source, /#facetVersion = new Map\(\)/);
  assert.match(source, /#bundles = sourceCache\(BUNDLE_CACHE_MAX\)/);
  assert.match(source, /#forgetFacet\(facetId\)/);
  assert.match(source, /#facetVersion\.delete\(facetId\)/);
  assert.equal(SOURCE_CACHE_BYTES, 65 * 1024 * 1024);
  assert.equal(textWeight('abc'), 6);
});

test('dynamic DO workers expose an explicit retirement path', () => {
  assert.match(source, /CellHiveLifecycle/);
  assert.match(source, /abortIsolate\(\"cellhive: retired\"\)/);
  assert.match(source, /const loadedWorkers = new Map/);
  assert.match(source, /releaseLoadedWorker\(/);
  assert.match(source, /#facetLoader\.size >= FACET_CACHE_MAX/);
});
