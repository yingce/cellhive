# CellHive CLI

## Configuration model

Keep project identity separate from credentials:

- `.cellhive/project.json` in the project: namespace, profile, and optional multi-Worker config; safe to commit;
- `~/.config/cellhive/config.json`: Admin URL and token/JWT, mode `0600`; never commit it.

The AI Skill bundle is published from the static manifest and can be installed
or refreshed with:

```bash
CELLHIVE_SKILL_BASE_URL=https://cellhive.example.com \
  bash scripts/install-cellhive-skill.sh

cellhive skills status
cellhive skills update
```

The online AI router is [`docs/ai/index.md`](ai/index.md); the machine-readable
catalog is [`docs/ai/catalog.json`](ai/catalog.json).

## Login profiles

```bash
cellhive login \
  --profile staging \
  --url https://cellhive.example.com \
  --token "$CELLHIVE_DEPLOY_TOKEN"
```

JWT profiles are also supported:

```bash
cellhive login --profile production \
  --url https://cellhive.example.com \
  --jwt "$CELLHIVE_ADMIN_JWT"
```

Inspect profiles:

```bash
cellhive auth list
cellhive auth current
cellhive auth test
cellhive auth logout staging
```

Overrides:

```bash
CELLHIVE_CLI_CONFIG=/path/to/config.json
CELLHIVE_PROFILE=production
```

Environment variables override profiles. Project files must not contain root keys, admin tokens, or JWTs.

## Initialize a project

```bash
cellhive project init --namespace acme --profile staging
```

This creates:

```json
{
  "profile": "staging",
  "namespace": "acme"
}
```

For multiple Workers:

```json
{
  "profile": "staging",
  "namespace": "acme",
  "workers": {
    "web": "apps/web/wrangler.jsonc",
    "api": "apps/api/wrangler.jsonc"
  }
}
```

Deploy with:

```bash
cellhive wrangler deploy
cellhive wrangler deploy --name api
cellhive wrangler deploy --config apps/web/wrangler.jsonc
```

Explicit `--namespace` and `--config` override project settings.

## Development and deployment

Use official Wrangler locally:

```bash
npx wrangler dev
```

Or use CellHive's Bun/Miniflare development CLI:

```bash
cellhive dev .
```

Build and deploy:

```bash
npm run build
cellhive wrangler deploy
```

CellHive does not run `npm install` or `npm run build` for framework projects. The project or CI owns the framework build.

## Official Wrangler

Generate standard Wrangler environment variables:

```bash
eval "$(cellhive env)"
npx wrangler deploy
```

The mapping is:

```text
CLOUDFLARE_API_BASE_URL → CellHive /client/v4 API
CLOUDFLARE_API_TOKEN    → profile admin token/JWT
CLOUDFLARE_ACCOUNT_ID   → CellHive namespace
```

`cellhive env` only prints shell exports; it does not spawn a child process or modify the current shell.

## Security

- Do not put `CELLHIVE_ROOT_KEY` in a project;
- use a dedicated deployment token or namespace-scoped JWT in CI;
- profile files are written with mode `0600`;
- `cellhive wrangler` calls the configured CellHive Admin API, not Cloudflare.
