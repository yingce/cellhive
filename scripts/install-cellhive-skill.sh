#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${CELLHIVE_SKILL_BASE_URL:-}"
if [ -z "$BASE_URL" ]; then
  echo "CELLHIVE_SKILL_BASE_URL is required (for example https://docs.example.com)" >&2
  exit 1
fi
BASE_URL="${BASE_URL%/}"
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

curl -fsSL --max-time 15 --max-filesize 1048576 "${BASE_URL%/}/skills/manifest.json" -o "$TMP/manifest.json"
VERSION="$(node -e 'const x=require(process.argv[1]); if(!/^\d{4}\.\d{2}\.\d{2}\.\d+$/.test(x.version||"")) throw Error("invalid version"); process.stdout.write(x.version)' "$TMP/manifest.json")"
SKILL_NAME="cellhive"
mkdir -p "$TMP/$SKILL_NAME"
curl -fsSL --max-time 15 --max-filesize 1048576 "${BASE_URL%/}/skills/$SKILL_NAME/manifest.json" -o "$TMP/$SKILL_NAME/manifest.json"
node -e 'const x=require(process.argv[1]); if(x.name!==process.argv[2]||x.version!==process.argv[3]||x.entrypoint!=="SKILL.md") throw Error("invalid skill manifest"); for(const f of x.files||[]) if(f.startsWith("/")||f.includes("..")||f.includes("\\")) throw Error("unsafe skill path")' "$TMP/$SKILL_NAME/manifest.json" "$SKILL_NAME" "$VERSION"
while IFS= read -r file; do
  [ "$file" = "manifest.json" ] && continue
  mkdir -p "$(dirname "$TMP/$SKILL_NAME/$file")"
  curl -fsSL --max-time 15 --max-filesize 1048576 "${BASE_URL%/}/skills/$SKILL_NAME/$file" -o "$TMP/$SKILL_NAME/$file"
  [ -s "$TMP/$SKILL_NAME/$file" ] || { echo "empty skill file: $file" >&2; exit 1; }
  node -e 'const fs=require("fs"),c=require("crypto");const [m,p,f]=process.argv.slice(1);const want=(JSON.parse(fs.readFileSync(m,"utf8")).sha256||{})[f];if(!want)process.exit(0);const got=c.createHash("sha256").update(fs.readFileSync(p)).digest("hex");if(got!==want.toLowerCase()){console.error("checksum mismatch: "+f);process.exit(1)}' "$TMP/$SKILL_NAME/manifest.json" "$TMP/$SKILL_NAME/$file" "$file"
  case "$file" in
    *.md|*.json|*.yaml|*.yml|*.txt)
      node -e 'const fs=require("fs");const [p,base]=process.argv.slice(1);const s=fs.readFileSync(p,"utf8");fs.writeFileSync(p,s.split("https://cellhive.example.com").join(base))' "$TMP/$SKILL_NAME/$file" "$BASE_URL" ;;
  esac
done < <(node -e 'for(const f of require(process.argv[1]).files) console.log(f)' "$TMP/$SKILL_NAME/manifest.json")
# The installed manifest is the one already fetched; rewrite its baseUrl too.
node -e 'const fs=require("fs");const [p,base]=process.argv.slice(1);const s=fs.readFileSync(p,"utf8");fs.writeFileSync(p,s.split("https://cellhive.example.com").join(base))' "$TMP/$SKILL_NAME/manifest.json" "$BASE_URL"

TARGET="$SKILLS_DIR/$SKILL_NAME"
BACKUP="$SKILLS_DIR/.cellhive-backup.$$"
if [ -e "$TARGET" ]; then mv "$TARGET" "$BACKUP"; fi
if ! mv "$TMP/$SKILL_NAME" "$TARGET"; then
  if [ -e "$BACKUP" ]; then
    if ! mv "$BACKUP" "$TARGET"; then
      echo "install failed and restore failed; previous skill is at $BACKUP" >&2
      exit 1
    fi
  fi
  echo "install failed; previous skill restored" >&2
  exit 1
fi
if [ -e "$BACKUP" ]; then rm -rf "$BACKUP"; fi
printf '%s\n' "$VERSION" > "$VERSION_FILE"
echo "Installed CellHive skill $VERSION to $TARGET"
