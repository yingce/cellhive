// workflows: 自研 Workflow 引擎，step.do / step.sleep，实例生命周期。
import { WorkflowEntrypoint } from "cloudflare:workers";

export class ReportBuilder extends WorkflowEntrypoint {
  async run(event, step) {
    const a = await step.do("first", async () => ({ value: (event.payload?.seed ?? 0) + 1 }));
    await step.sleep("pause", "2 seconds");
    const b = await step.do("second", async () => ({ value: a.value * 2 }));
    return b;
  }
}
export default {
  async fetch(request, env) {
    const u = new URL(request.url);
    if (u.pathname === "/create") {
      const inst = await env.REPORTS.create({ params: { seed: Number(u.searchParams.get("seed") || 1) } });
      return Response.json({ id: inst.id });
    }
    if (u.pathname === "/status") return Response.json(await (await env.REPORTS.get(u.searchParams.get("id"))).status());
    return new Response("Use /create?seed=N or /status?id=ID\n");
  },
};
