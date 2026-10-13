// Experimental lifecycle probe only; no production tenant traffic.
let loads = 0;
export default {
  async fetch(req, env) {
    const q = new URL(req.url).searchParams;
    if (q.has("health")) return new Response("ok");
    const mode = q.get("mode");
    const stub = env.LOADER.get(mode === "anonymous" ? null : q.get("id"), () => {
      loads++;
      return {
        compatibilityDate: "2026-06-15",
        mainModule: "tenant.js",
        modules: { "tenant.js": `
          import {abortIsolate} from "cloudflare:workers";
          let counter = 0;
          export default {fetch(req) {
            if (new URL(req.url).pathname === "/abort") abortIsolate("lifecycle probe");
            return new Response(String(++counter));
          }};
          /*${"x".repeat(Number(q.get("bytes") || 1048576))}*/
        ` },
      };
    });
    const ep = stub.getEntrypoint();
    const counter = await (await ep.fetch("http://tenant/ping")).text();
    if (mode === "abort") {
      try { await ep.fetch("http://tenant/abort"); } catch (_) {}
    }
    return Response.json({counter: Number(counter), loads});
  },
};
