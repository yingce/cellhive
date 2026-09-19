// services/api: 被服务绑定的目标 Worker，导出 RPC 入口类 Api。
import { WorkerEntrypoint } from "cloudflare:workers";

export class Api extends WorkerEntrypoint {
  greet(name) { return `hi ${name}`; }
  async sum(a, b) { return a + b; }
}
export default {
  async fetch() { return new Response("api ok\n"); },
};
