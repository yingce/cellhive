import { readFile, stat } from "node:fs/promises";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const readJSON = async (path) => JSON.parse(await readFile(resolve(root, path), "utf8"));
const exists = async (path) => { try { await stat(resolve(root, path)); return true; } catch { return false; } };

const catalog = await readJSON("docs/ai/catalog.json");
const manifest = await readJSON("docs/ai/manifest.json");
if (!/^\d{4}\.\d{2}\.\d{2}\.\d+$/.test(catalog.version)) throw new Error("invalid catalog version");
if (manifest.version !== catalog.version) throw new Error("AI manifest/catalog version mismatch");
const ids = new Set();
for (const capability of catalog.capabilities) {
  if (!capability.id || ids.has(capability.id)) throw new Error(`duplicate/missing capability id: ${capability.id}`);
  ids.add(capability.id);
  if (!Array.isArray(capability.keywords) || capability.keywords.length === 0) throw new Error(`${capability.id}: keywords required`);
  if (!await exists(capability.reference.replace(/^\//, ""))) throw new Error(`${capability.id}: missing reference ${capability.reference}`);
  if (!["supported", "partial", "rejected"].includes(capability.status)) throw new Error(`${capability.id}: invalid status`);
}
for (const skill of manifest.skills) {
  const skillManifest = await readJSON(`skills/${skill.manifest}`);
  if (skillManifest.name !== skill.name || skillManifest.version !== catalog.version) throw new Error(`${skill.name}: manifest mismatch`);
  for (const file of skillManifest.files) {
    if (file.startsWith("/") || file.includes("..") || file.includes("\\")) throw new Error(`${skill.name}: unsafe file ${file}`);
    if (!await exists(`skills/${skill.name}/${file}`)) throw new Error(`${skill.name}: missing ${file}`);
  }
}
console.log(`AI docs contract: PASS (${catalog.capabilities.length} capabilities, ${manifest.skills.length} skills, ${catalog.version})`);
