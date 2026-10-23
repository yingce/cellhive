export default {
  async fetch(req, env) {
    const r = await env.SVC.fetch(new Request("http://target/", {
      headers: { cookie: "a=b", "x-test": "yes" },
    }));
    return new Response(await r.text(), {
      headers: { "content-type": "application/json", "x-caller": r.headers.get("x-svc-resp") || "" },
    });
  },
};
