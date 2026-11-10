import { DurableObject } from 'cloudflare:workers';
class Counter extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url), sql = this.ctx.storage.sql;
    if (url.pathname === '/seed') {
      sql.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL)');
      sql.exec('INSERT INTO t VALUES(1, ?)', Number(url.searchParams.get('value')));
    }
    if (url.pathname === '/inc') sql.exec('UPDATE t SET v=v+1 WHERE id=1');
    return Response.json({v: [...sql.exec('SELECT v FROM t WHERE id=1')][0].v});
  }
}
export class CounterA extends Counter {}
export class CounterQ extends Counter {}
export default {fetch() {return new Response('use DO invoke');}};
