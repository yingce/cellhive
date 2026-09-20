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
import { buildBindings, wrapR2Metadata } from "facades.js";

try {
  const specText = env.CH_FACADE_SPEC;
  if (specText) {
    // __cellhivePlatform is injected as a module-scope const in this module's
    // source (host.js platformConsts) — never in the facet env (ADR-074).
    const facades = buildBindings(
      { url: __cellhivePlatform.cellUrl, token: __cellhivePlatform.cellToken, fetcher: env.PLATFORM },
      JSON.parse(specText),
    );
    for (const [name, value] of Object.entries(facades)) {
      Object.defineProperty(env, name, { value, writable: true, configurable: true, enumerable: true });
    }
  }
  // R2ObjectBody.writeHttpMetadata mutates the caller's Headers, so it must run
  // in this isolate (RPC would serialize the argument by value).
  if (env.CH_R2_BINDINGS) {
    for (const name of JSON.parse(env.CH_R2_BINDINGS)) {
      if (env[name]) {
        Object.defineProperty(env, name, { value: wrapR2Metadata(env[name]), writable: true, configurable: true, enumerable: true });
      }
    }
  }
} catch (e) {
  // Leave env unpatched; the tenant will surface the missing binding.
}

export * from "tenant.js";
