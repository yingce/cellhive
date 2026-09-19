// hello: 最小 Worker，证明 fetch 与路由。
export default {
  async fetch(request) {
    const url = new URL(request.url);
    return new Response(`hello from ${url.host}${url.pathname}\n`, {
      headers: { "content-type": "text/plain" },
    });
  },
};
