// webapi: 常用 Web Platform API（URL/Headers/TextEncoder/atob/crypto）。
export default {
  async fetch(request) {
    const u = new URL(request.url);
    const enc = new TextEncoder().encode("cellhive");
    const digest = await crypto.subtle.digest("SHA-256", enc);
    const hex = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
    const h = new Headers({ "x-demo": "1" });
    h.append("x-demo", "2");
    return Response.json({
      path: u.pathname,
      query: Object.fromEntries(u.searchParams),
      b64: btoa("hello"),
      back: atob("aGVsbG8="),
      header: h.get("x-demo"),
      sha256: hex.slice(0, 16),
    });
  },
};
