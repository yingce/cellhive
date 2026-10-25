// CellHive platform DurableObject base (ADR-079).
//
// stock workerd cannot set native alarms on facet-backed SQLite Durable Objects
// ("Facets currently cannot set alarms"), so the platform shims
// ctx.storage.setAlarm/getAlarm/deleteAlarm. setAlarm first durably registers a
// generation-bearing timer through a narrow host capability, then stores the
// alarm in the object's storage. Numeric legacy alarms are mirrored by the host.
//
// The tenant bundle's `cloudflare:workers` import is rewritten to `cellhive-do.js` by
// the do-runtime host, so tenant DO classes extend this base class and get the
// shimmed storage.

import { DurableObject as NativeDurableObject } from "cloudflare:workers";

export * from "cloudflare:workers";

// Reserved storage key for the object's single alarm (KV semantics, durable).
const ALARM_KEY = "__cellhive_alarm";
const ALARM_ID = "__cellhive_alarm_id";
const ALARM_PENDING = "__cellhive_alarm_pending";

function define(target, name, value) {
  Object.defineProperty(target, name, { value, configurable: true, writable: true });
}

export class DurableObject extends NativeDurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    const storage = ctx.storage;
    const rawGet = storage.get.bind(storage);
    const rawPut = storage.put.bind(storage);
    const rawDelete = storage.delete.bind(storage);

    define(storage, "setAlarm", async (scheduledTime) => {
      const ms = typeof scheduledTime === "number" ? scheduledTime : Number(scheduledTime);
      if (!Number.isFinite(ms)) throw new TypeError("setAlarm: scheduledTime must be a finite number");
      // Publish before committing. blockConcurrencyWhile prevents a due timer
      // from observing the old state while the durable handoff is in flight.
      // A crash before the write leaves only a harmless stale timer.
      await ctx.blockConcurrencyWhile(async () => {
        const id = crypto.randomUUID();
        const bridge = ctx.props && ctx.props.alarmBridge;
        if (!bridge) throw new Error("alarm transport unavailable");
        await bridge.prepare(ms, id);
        await rawPut({ [ALARM_KEY]: ms, [ALARM_ID]: id });
      });
    });
    define(storage, "getAlarm", async () => {
      const v = await rawGet(ALARM_KEY);
      return typeof v === "number" ? v : null;
    });
    define(storage, "deleteAlarm", async () => {
      await rawDelete([ALARM_KEY, ALARM_ID, ALARM_PENDING]);
    });
    // Native deleteAll() errors on SQLite-backed facets ("expected parent ==
    // kj::none"), so the platform shims it: clear all KV keys, drop user SQL
    // tables, and clear the shimmed alarm.
    define(storage, "deleteAll", async () => {
      const LIMIT = 256;
      for (;;) {
        const opts = { limit: LIMIT };
        const page = await storage.list(opts);
        const keys = [...page.keys()];
        if (keys.length) await storage.delete(keys);
        if (keys.length < LIMIT) break;
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
      await rawDelete([ALARM_KEY, ALARM_ID, ALARM_PENDING]);
    });
  }

  // Platform RPC: current alarm state, read by the do-runtime host after each
  // invocation to report it to cell-agent.
  async __chAlarmState() {
    const v = await this.ctx.storage.get(ALARM_KEY);
    if (typeof v === "number") return { dueMs: v, alarmId: await this.ctx.storage.get(ALARM_ID) };
    const pending = await this.ctx.storage.get(ALARM_PENDING);
    return pending || { dueMs: null };
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

  // Platform RPC: consume the matching alarm and run the handler. Pending
  // delivery state survives a crash/failure; a replacement active alarm remains
  // independent of the old delivery. Retry timing is owned by the timer runner.
  async __chRunAlarm(expected = {}) {
    if (this.__alarmRunning) throw new Error("alarm invocation already running");
    this.__alarmRunning = true;
    try {
      let alarm;
      await this.ctx.blockConcurrencyWhile(async () => {
        await this.ctx.storage.transaction(async (storage) => {
          const pending = await storage.get(ALARM_PENDING);
          const dueMs = await storage.get(ALARM_KEY);
          const alarmId = await storage.get(ALARM_ID);
          const matches = (a) => a && (expected.alarmId
            ? a.alarmId === expected.alarmId
            : expected.dueMs !== undefined ? a.dueMs === expected.dueMs : true);
          if (matches(pending)) alarm = pending;
          else if (typeof dueMs === "number" && matches({ dueMs, alarmId })) {
            alarm = { dueMs, alarmId, retryCount: 0 };
            await storage.put(ALARM_PENDING, alarm);
            await storage.delete([ALARM_KEY, ALARM_ID]);
          }
        });
      });
      if (!alarm) return null;
      try {
        const out = typeof this.alarm === "function"
          ? await this.alarm({ retryCount: alarm.retryCount || 0, isRetry: !!alarm.retryCount }) : null;
        await this.ctx.storage.delete(ALARM_PENDING);
        return out;
      } catch (e) {
        await this.ctx.storage.put(ALARM_PENDING, { ...alarm, retryCount: (alarm.retryCount || 0) + 1 });
        throw e;
      }
    } finally { this.__alarmRunning = false; }
  }
}
