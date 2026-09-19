// Control worker: identical DO SQL write, but NO output gate (no GATE binding).
// Used to decompose workerd DO cost vs the ADR-051 gate cost.
export class A {
  constructor(state, env) {
    this.state = state;
    state.blockConcurrencyWhile(async () => {
      state.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, n INTEGER)"
      );
    });
  }

  async fetch() {
    const sql = this.state.storage.sql;
    sql.exec("INSERT INTO t (id,n) VALUES (1,0) ON CONFLICT(id) DO UPDATE SET n=n+1");
    const rows = [...sql.exec("SELECT n FROM t WHERE id=1")];
    return new Response(JSON.stringify({ n: rows[0].n }));
  }
}

export default {
  async fetch(req, env) {
    return env.A.get(env.A.idFromName("a")).fetch(req);
  },
};
