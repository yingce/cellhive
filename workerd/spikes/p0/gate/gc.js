// Demo DO using the platform GroupCommit wrapper (workerd/wrapper/groupcommit.js).
// One HTTP request = one op = one SQL write; the wrapper groups concurrent ops
// into one commit + one gate proof.
//
//   GET /            submit one op (k writes, default 1)
//   GET /read        current counter (no write)
//   GET /stats       wrapper batch statistics
import { GroupCommit } from "wrapper/groupcommit";

export class A {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    state.blockConcurrencyWhile(async () => {
      state.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, n INTEGER)"
      );
    });
    this.gc = new GroupCommit({
      state,
      env,
      gate: env.GATE,
      windowMs: 3,
      maxBatch: 512,
      apply: (op) => {
        const sql = this.state.storage.sql;
        const stmt =
          "INSERT INTO t (id,n) VALUES (1,0) ON CONFLICT(id) DO UPDATE SET n=n+1";
        for (let i = 0; i < (op.k || 1); i++) sql.exec(stmt);
      },
    });
  }

  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === "/read") {
      const rows = [...this.state.storage.sql.exec("SELECT n FROM t WHERE id=1")];
      return new Response(JSON.stringify({ n: rows[0] ? rows[0].n : 0 }));
    }
    if (u.pathname === "/stats") {
      const s = this.gc.stats;
      return new Response(
        JSON.stringify({ ...s, avg_batch: s.batches ? s.ops / s.batches : 0 })
      );
    }
    const k = parseInt(u.searchParams.get("k") || "1", 10);
    const batch = await this.gc.submit({ k });
    return new Response(JSON.stringify({ k, batch }));
  }
}

export default {
  async fetch(req, env) {
    return env.A.get(env.A.idFromName("a")).fetch(req);
  },
};
