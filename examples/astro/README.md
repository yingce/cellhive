# Astro SSR on CellHive

A minimal server-rendered Astro app using `@astrojs/cloudflare`. The adapter
emits a Worker plus static assets; CellHive runs the Worker on its workerd fork
and serves the assets from the same immutable version.

```bash
npm install
npm run build

cellhive app create <namespace>
cellhive domain add <namespace> <host>

# CellHive's deploy bundler needs esbuild; point it at the local copy.
export CELLHIVE_ESBUILD="$PWD/node_modules/.bin/esbuild"
cellhive deploy <namespace> astro-ssr --config wrangler.jsonc
cellhive route add <namespace> <host> astro-ssr
```

The Worker name (`astro-ssr`) comes from `wrangler.jsonc`.

Open `/` (server-rendered timestamp changes per request), `/about`,
`/style.css`, and a missing path (Astro's 404 page).

## Notes and caveats

1. **Use `imageService: "passthrough"`** (see `astro.config.mjs`). The default
   `compile` mode pulls `sharp`/`detect-libc` and Node built-ins into the Worker
   bundle; CellHive's bundler fails with `Could not resolve "child_process"` /
   `"fs"`. Use `"cloudflare"` only when a Cloudflare image service is available.
2. **Do not point `assets.directory` at `dist`.** Adapter 12 writes public
   assets next to `dist/_worker.js`; uploading `dist` would expose server code
   as static files. `npm run build` runs `scripts/prepare-assets.mjs`, which
   copies everything except `_worker.js` and `_routes.json` into `dist/client`.
3. `assets.binding` must be `ASSETS`: the adapter calls `env.ASSETS.fetch(...)`
   for static paths.
4. `nodejs_compat` is enabled in `wrangler.jsonc`. `compatibility_date` must not
   exceed the platform maximum (currently `2026-09-23`).
5. Sessions are not configured. If you enable Astro sessions, create the KV
   binding named `SESSION` (the adapter warns about it at build time).
6. The route target is the Worker name from `wrangler.jsonc` (`astro-ssr`), not
   the namespace. A route pointing at the namespace produces
   `worker <name> has no active version` (503).
7. Verified locally against CellHive's workerd; image optimization and browser
   hydration were not exercised. Production SG/USW acceptance is separate.
