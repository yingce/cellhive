// body: 请求/响应体（json / text / arrayBuffer / 流）。
export default {
  async fetch(request) {
    const type = request.headers.get("content-type") || "";
    if (type.includes("application/json")) {
      const data = await request.json();
      return Response.json({ got: data });
    }
    if (request.method === "POST" || request.method === "PUT") {
      const buf = await request.arrayBuffer();
      return new Response(
        new ReadableStream({
          start(c) { c.enqueue(new Uint8Array(buf)); c.close(); },
        }),
        { headers: { "content-type": "application/octet-stream", "x-size": String(buf.byteLength) } },
      );
    }
    return new Response(await request.text());
  },
};
