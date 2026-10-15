#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${CELLHIVE_SKILL_BASE_URL:-https://cellhive.example.com}"
case "${CELLHIVE_AGENT:-}" in
  codex|Codex|CODEX) SKILL_HOME="${CODEX_HOME:-$HOME/.codex}" ;;
  *) SKILL_HOME="${AGENTS_HOME:-$HOME/.agents}" ;;
esac
SKILLS_DIR="$SKILL_HOME/skills"
VERSION_FILE="$SKILLS_DIR/.cellhive-skills-version"
mkdir -p "$SKILLS_DIR"
TMP="$(mktemp -d "$SKILLS_DIR/.cellhive-install.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }
command -v node >/dev/null || { echo "node is required" >&2; exit 1; }

curl -fsSL "${BASE_URL%/}/skills/manifest.json" -o "$TMP/manifest.json"
VERSION="$(node -e 'const x=require(process.argv[1]); if(!/^\d{4}\.\d{2}\.\d{2}\.\d+$/.test(x.version||"")) throw Error("invalid version"); process.stdout.write(x.version)' "$TMP/manifest.json")"
SKILL_NAME="cellhive"
mkdir -p "$TMP/$SKILL_NAME"
curl -fsSL "${BASE_URL%/}/skills/$SKILL_NAME/manifest.json" -o "$TMP/$SKILL_NAME/manifest.json"
node -e 'const x=require(process.argv[1]); if(x.name!==process.argv[2]||x.version!==process.argv[3]||x.entrypoint!=="SKILL.md") throw Error("invalid skill manifest"); for(const f of x.files||[]) if(f.startsWith("/")||f.includes("..")||f.includes("\\")) throw Error("unsafe skill path")' "$TMP/$SKILL_NAME/manifest.json" "$SKILL_NAME" "$VERSION"
while IFS= read -r file; do
  [ "$file" = "manifest.json" ] && continue
  mkdir -p "$(dirname "$TMP/$SKILL_NAME/$file")"
  curl -fsSL "${BASE_URL%/}/skills/$SKILL_NAME/$file" -o "$TMP/$SKILL_NAME/$file"
  [ -s "$TMP/$SKILL_NAME/$file" ] || { echo "empty skill file: $file" >&2; exit 1; }
done < <(node -e 'for(const f of require(process.argv[1]).files) console.log(f)' "$TMP/$SKILL_NAME/manifest.json")

TARGET="$SKILLS_DIR/$SKILL_NAME"
BACKUP="$TMP/backup"
if [ -e "$TARGET" ]; then mv "$TARGET" "$BACKUP"; fi
mv "$TMP/$SKILL_NAME" "$TARGET"
printf '%s\n' "$VERSION" > "$VERSION_FILE"
echo "Installed CellHive skill $VERSION to $TARGET"
