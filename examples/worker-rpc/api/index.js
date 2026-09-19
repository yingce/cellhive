// worker-rpc/api: 目标 Worker 的入口类（原生 JSRPC，支持结构化克隆）。
import { WorkerEntrypoint } from "cloudflare:workers";
export class Math extends WorkerEntrypoint {
  add(a, b) { return a + b; }
  box(map) { return new Map([...map, ["checked", true]]); }
}
export default { async fetch() { return new Response("math ok\n"); } };
