// worker-rpc/caller: 同实例原生 JSRPC 调目标入口类（Map 等结构化值可往返）。
// 这个示例专门验证 native service RPC；关闭 CELLHIVE_SERVICE_NATIVE 后，
// HTTP/JSON fallback 不保证 Map/Date/ArrayBuffer 等 structured-clone 语义。
export default {
  async fetch(request, env) {
    const sum = await env.MATH.add(2, 3);
    const boxed = await env.MATH.box(new Map([["input", 1]]));
    return Response.json({ sum, boxed: Object.fromEntries(boxed) });
  },
};
