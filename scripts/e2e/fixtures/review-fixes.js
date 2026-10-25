export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/r2") {
      const metadata = {customMetadata: {old: "yes"}, httpMetadata: {contentType: "text/x-old"}};
      await env.BUCKET.put("overwrite", "hello", metadata);
      const fresh = await env.BUCKET.put("overwrite", "longer new value");
      const head = await env.BUCKET.head("overwrite");
      const obj = await env.BUCKET.get("overwrite");
      const range = await env.BUCKET.get("overwrite", {range: {offset: 0, length: 4}});
      await env.BUCKET.put("identical", "same", metadata);
      await env.BUCKET.put("identical", "same");
      const same = await env.BUCKET.head("identical");
      return Response.json({
        size: head.size, etag: head.etag === fresh.etag,
        body: await obj.text(), range: await range.text(),
        cleared: !head.customMetadata.old && !same.customMetadata.old,
      });
    }
    if (url.pathname === "/d1") {
      const rows = Math.min(20000, Math.max(1, Number(url.searchParams.get("rows") || 20000)));
      const started = Date.now();
      const result = await env.DB.prepare(
        "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<?) SELECT n, 'abcdef' AS value FROM t"
      ).bind(rows).all();
      return Response.json({
        rows: result.results.length,
        last: result.results.at(-1).n,
        duration_ms: Date.now() - started,
      });
    }
    return new Response("ok");
  },
  async queue() {},
};
