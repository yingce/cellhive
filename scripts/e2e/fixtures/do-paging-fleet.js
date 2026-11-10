import { DurableObject } from "cloudflare:workers";
export class Bench extends DurableObject {
  async fetch(request) {
    const sql = this.ctx.storage.sql, url = new URL(request.url), path = url.pathname;
    if (path === "/seed") {
      this.ctx.storage.transactionSync(() => {
        sql.exec("CREATE TABLE counter(id INTEGER PRIMARY KEY,v INTEGER)");
        sql.exec("INSERT INTO counter VALUES(1,0)");
        sql.exec("CREATE TABLE payload(id INTEGER PRIMARY KEY,b BLOB)");
        for (let i = 1; i <= Number(url.searchParams.get("rows") || 256); i++) sql.exec("INSERT INTO payload VALUES(?,randomblob(65536))", i);
      });
      return new Response("seeded");
    }
    if (path === "/proof") {
      const sums = [];
      for (const row of sql.exec("SELECT id,b FROM payload ORDER BY id")) {
        const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", row.b));
        sums.push(row.id + ":" + Array.from(hash, v => v.toString(16).padStart(2, "0")).join(""));
      }
      return new Response(sums.join("\n"));
    }
    if (path === "/inc") sql.exec("UPDATE counter SET v=v+1 WHERE id=1");
    return Response.json({ v: [...sql.exec("SELECT v FROM counter WHERE id=1")][0].v });
  }
}
export default { fetch() { return new Response("use the authenticated DO invoke API"); } };
