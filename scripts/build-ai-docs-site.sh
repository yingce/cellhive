#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/docs-site/dist}"

rm -rf "$OUT"
mkdir -p "$OUT/ai" "$OUT/skills/cellhive/agents" "$OUT/examples"

# The docs Worker has its own host, so /ai/catalog.json and /skills/* are
# served directly at the site root. Keep this path stable for Skill clients.
cp "$ROOT/docs/ai/index.md" "$OUT/ai/index.md"
cp "$ROOT/docs/ai/catalog.json" "$OUT/ai/catalog.json"
cp "$ROOT/docs/ai/manifest.json" "$OUT/ai/manifest.json"
cp "$ROOT"/docs/*.md "$OUT/"
cp "$ROOT/skills/manifest.json" "$OUT/skills/manifest.json"
cp "$ROOT/skills/cellhive/manifest.json" "$OUT/skills/cellhive/manifest.json"
cp "$ROOT/skills/cellhive/SKILL.md" "$OUT/skills/cellhive/SKILL.md"
cp "$ROOT/skills/cellhive/agents/openai.yaml" "$OUT/skills/cellhive/agents/openai.yaml"

# Skill routes example/scenario requests to /examples/README.md; ship the
# runnable example tree so its feature/table links resolve on the docs host.
cp -R "$ROOT/examples/." "$OUT/examples/"

echo "built CellHive AI docs site at $OUT"
