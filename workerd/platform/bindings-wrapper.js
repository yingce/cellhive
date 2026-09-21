// Loaded-worker entry that patches the importable `env` with local facades for
// bindings whose Cloudflare API cannot cross RPC as-is (DO namespaces and
// Workflows need SYNCHRONOUS idFromName/newUniqueId and local method objects),
// then re-exports the tenant module (ADR-090 Phase 2).
//
// Migrated bindings (KV/D1/R2/Queue) are already props-bound entrypoint stubs in
// env and need no patching. This runs in the loaded isolate, and because workerd
// hands the same env object to the tenant's fetch handler and to Durable Object
// constructors, `this.env.X` AND the constructor-parameter `env.X` both see the
// patched facades.
import "log-tail.js";
import { env } from "cloudflare:workers";
import { buildBindings, wrapR2Metadata, makeDOFromStub } from "facades.js";

try {
  // DO namespaces are platform-side entrypoint stubs; wrap them so a WebSocket
  // upgrade routes through the cluster-only WS binding.
  if (__cellhivePlatform.doBindings.length > 0 && env.CH_DO_CONNECT) {
    for (const name of __cellhivePlatform.doBindings) {
      if (env[name]) {
        Object.defineProperty(env, name, {
          value: makeDOFromStub(env[name], env.CH_DO_CONNECT),
          writable: true, configurable: true, enumerable: true,
        });
      }
    }
  }
  // R2ObjectBody.writeHttpMetadata mutates the caller's Headers, so it must run
  // in this isolate (RPC would serialize the argument by value).
  if (__cellhivePlatform.r2Bindings.length > 0) {
    for (const name of __cellhivePlatform.r2Bindings) {
      if (env[name]) {
        Object.defineProperty(env, name, { value: wrapR2Metadata(env[name]), writable: true, configurable: true, enumerable: true });
      }
    }
  }
} catch (e) {
  // Leave env unpatched; the tenant will surface the missing binding.
}

export * from "tenant.js";
