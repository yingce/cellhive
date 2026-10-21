import mysql from 'mysql2/promise';

export default {
  async fetch(req, env) {
    const conn = await mysql.createConnection({
      uri: env.HYPERDRIVE.connectionString,
      // workerd forbids dynamic code generation; mysql2's compiled row parser
      // must be replaced by the static parser.
      disableEval: true,
    });
    try {
      const [rows] = await conn.query('SELECT 1 AS ok, DATABASE() AS db, VERSION() AS v');
      return Response.json(rows);
    } finally {
      await conn.end();
    }
  },
};
