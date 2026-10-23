export default {
  async fetch(req) {
    return new Response(JSON.stringify({
      cookie: req.headers.get("cookie") || "",
      xtest: req.headers.get("x-test") || "",
    }), { headers: { "content-type": "application/json", "x-svc-resp": "yes" } });
  },
};
