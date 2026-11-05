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

test('seen is bounded, facet identity maps are unbounded, bundles use a byte budget', () => {
  assert.match(source, /const seen = new CountLRU\(SEEN_MAX\)/);
  assert.match(source, /#facets = new Map\(\)/);
  assert.match(source, /#facetVersion = new Map\(\)/);
  assert.match(source, /#bundles = sourceCache\(BUNDLE_CACHE_MAX\)/);
  assert.match(source, /#facets\.delete\(facetId\)/);
  assert.match(source, /#facetVersion\.delete\(facetId\)/);
  assert.equal(SOURCE_CACHE_BYTES, 65 * 1024 * 1024);
  assert.equal(textWeight('abc'), 6);
});
