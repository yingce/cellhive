// CellHive platform DurableObject base (ADR-079).
//
// stock workerd cannot set native alarms on facet-backed SQLite Durable Objects
// ("Facets currently cannot set alarms"), so the platform shims
// ctx.storage.setAlarm/getAlarm/deleteAlarm to store the alarm in the object's
// own storage. The do-runtime host reads the alarm state after each invocation
// and reports it to cell-agent, which schedules delivery through the unified
// timer + waker.
//
// The tenant bundle's `cloudflare:workers` import is rewritten to `cellhive-do.js` by
// the do-runtime host, so tenant DO classes extend this base class and get the
// shimmed storage.

import { setLogBridge } from "log-tail.js";
import { DurableObject as NativeDurableObject } from "cloudflare:workers";

export * from "cloudflare:workers";

// Reserved storage key for the object's single alarm (KV semantics, durable).
const ALARM_KEY = "__cellhive_alarm";

function define(target, name, value) {
  Object.defineProperty(target, name, { value, configurable: true, writable: true });
}

export class DurableObject extends NativeDurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    if (ctx && ctx.props && ctx.props.logBridge) setLogBridge(ctx.props.logBridge);
    const storage = ctx.storage;
    const rawGet = storage.get.bind(storage);
    const rawPut = storage.put.bind(storage);
    const rawDelete = storage.delete.bind(storage);
    let cursor;

    define(storage, "setAlarm", async (scheduledTime) => {
      const ms = typeof scheduledTime === "number" ? scheduledTime : Number(scheduledTime);
      if (!Number.isFinite(ms)) throw new TypeError("setAlarm: scheduledTime must be a finite number");
      await rawPut(ALARM_KEY, ms);
    });
    define(storage, "getAlarm", async () => {
      const v = await rawGet(ALARM_KEY);
      return typeof v === "number" ? v : null;
    });
    define(storage, "deleteAlarm", async () => {
      await rawDelete(ALARM_KEY);
    });
    // Native deleteAll() errors on SQLite-backed facets ("expected parent ==
    // kj::none"), so the platform shims it: clear all KV keys, drop user SQL
    // tables, and clear the shimmed alarm.
    define(storage, "deleteAll", async () => {
      const LIMIT = 256;
      for (let i = 0; i < 100000; i++) {
        const opts = { limit: LIMIT };
        if (cursor !== undefined) opts.startAfter = cursor;
        const page = await storage.list(opts);
        const keys = [...page.keys()];
        if (keys.length) await storage.delete(keys);
        if (keys.length === 0 || page.list_complete === true || page.cursor === undefined || keys.length < LIMIT) break;
        cursor = page.cursor;
      }
      if (storage.sql) {
        const rows = [...storage.sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%'",
        )];
        for (const r of rows) {
          const name = String(r.name).replaceAll('"', '""');
          storage.sql.exec('DROP TABLE IF EXISTS "' + name + '"');
        }
      }
      await rawDelete(ALARM_KEY);
    });
  }

  // Platform RPC: current alarm state, read by the do-runtime host after each
  // invocation to report it to cell-agent.
  async __chAlarmState() {
    const v = await this.ctx.storage.get(ALARM_KEY);
    return { dueMs: typeof v === "number" ? v : null };
  }

  // Platform RPC: close all accepted sockets with a code (restart semantics,
  // ADR-078/080: old owner closes with 1012).
  async __chCloseAll(code) {
    const socks = typeof this.ctx.getWebSockets === "function" ? this.ctx.getWebSockets() : [];
    for (const ws of socks) {
      try {
        ws.close(code, "restart");
      } catch (e) {
        /* already closed */
      }
    }
    return socks.length;
  }

  // Platform RPC: run the tenant alarm() handler (no-op when absent). Cloudflare
  // passes an AlarmInvocationInfo; retry count is not tracked yet (bounded by the
  // scheduler), so report 0 rather than undefined.
  async __chRunAlarm() {
    if (typeof this.alarm === "function") return await this.alarm({ retryCount: 0, isRetry: false });
    return null;
  }
}
