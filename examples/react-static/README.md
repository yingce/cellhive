# React/Vite Static SPA

Build any React/Vite application into `dist/`, then deploy this directory with
the standard Wrangler assets configuration. CellHive runs the same Worker
assets-first model as Workers Static Assets; `/api/*` is reserved for the
optional Worker entrypoint.

```bash
npm ci
npm run build
cellhive deploy <namespace> react-static --config wrangler.jsonc
```

The example uses an assets-only configuration: no Worker source is required.
For an API, add `main` and set `assets.binding` to `ASSETS`.
