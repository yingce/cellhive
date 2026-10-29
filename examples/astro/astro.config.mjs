import { defineConfig } from 'astro/config';
import cloudflare from '@astrojs/cloudflare';

// imageService: "passthrough" keeps sharp and its Node built-ins out of the
// Worker bundle. CellHive's bundler rejects them; use "cloudflare" only when a
// Cloudflare image service binding is available.
export default defineConfig({
  output: 'server',
  adapter: cloudflare({ imageService: 'passthrough' }),
});
