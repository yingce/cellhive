#!/usr/bin/env bun
import { runDev } from "./dev.ts";

const USAGE = `cellhive — CellHive CLI

Usage:
  cellhive dev [projectDir] [flags]

Flags (dev):
  --namespace <ns>   (reserved)
  --port <n>         local port (default 8787)
  --data-dir <path>  default <projectDir>/.cellhive-dev
  --env <name>       select wrangler env.<name>
  --var KEY=VALUE    override a var (repeatable)
  --clean            (reserved)
`;

async function main() {
  const [cmd, ...rest] = process.argv.slice(2);
  switch (cmd) {
    case "dev":
      await runDev(rest);
      break;
    case undefined:
    case "-h":
    case "--help":
      console.log(USAGE);
      break;
    default:
      console.error(`unknown command "${cmd}"\n`);
      console.log(USAGE);
      process.exit(1);
  }
}

main().catch((e) => {
  console.error(`cellhive: ${e instanceof Error ? e.message : String(e)}`);
  process.exit(1);
});
