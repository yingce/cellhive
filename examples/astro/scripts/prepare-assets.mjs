// The Cloudflare adapter writes public assets next to _worker.js. CellHive must
// not upload server code as an asset, so copy everything except _worker.js and
// _routes.json into dist/client and point assets.directory there.
import { cp, mkdir, readdir, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const dist = join(root, 'dist');
const client = join(dist, 'client');

await rm(client, { recursive: true, force: true });
await mkdir(client, { recursive: true });

for (const entry of await readdir(dist)) {
  if (entry === '_worker.js' || entry === '_routes.json' || entry === 'client') continue;
  await cp(join(dist, entry), join(client, entry), { recursive: true });
}

console.log('prepared dist/client (excluded _worker.js/_routes.json)');
