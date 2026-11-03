# CellHive Documentation

CellHive is a self-hosted, Cloudflare Workers-compatible runtime. The default documentation route is English; the maintained Chinese entry point is [`README.zh.md`](README.zh.md).

The full English documentation index is [`en/README.md`](en/README.md). Use it to choose a path:

- **Operate CellHive:** deployment → configuration → operations
- **Build Workers:** bindings → compatibility matrix → Wrangler compatibility
- **Contribute:** architecture → cell protocol → contributing → testing
- **Study the protocol:** cell protocol → protocol formats → Durable Objects → storage and replication

## Audience

- `ai/` (and the repo-root `skills/` bundle) are the Skill/agent entry points (`ai/catalog.json` is the machine catalog).
- The remaining `*.md`, `en/`, and `modules/` documents are developer/operator references; the Skill reads the canonical references listed in `ai/catalog.json`.
- `archive/` and `superpowers/` are historical records, not current contracts; `docs-site/dist/` is generated.

## Directory routes

| Area | English entry point | Chinese entry point |
|---|---|---|
| Design and operations | [`en/README.md`](en/README.md) | [`README.zh.md`](README.zh.md) |
| Module documentation | [`en/modules/README.md`](en/modules/README.md) | [`modules/README.zh.md`](modules/README.zh.md) |
| Runnable examples | [`../examples/README.md`](../examples/README.md) | [`../examples/README.zh.md`](../examples/README.zh.md) |

Keep new documents registered in the English and Chinese maps. Source code and tests remain authoritative for behavior; design changes must also update `decisions.md`.

_Last updated: 2026-11-01_
