// hyperdrive: 连接串由注册资源解析并注入为数据对象；平台不池化连接。
export default {
  async fetch(request, env) {
    const cs = env.HYDR?.connectionString || "";
    return Response.json({ configured: cs.length > 0 });
  },
};
