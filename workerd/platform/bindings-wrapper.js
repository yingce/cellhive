// Loaded-worker entry that patches the importable `env` with local facades for
// bindings whose Cloudflare API cannot cross RPC as-is. DO namespaces and
// Workflows require synchronous local method objects; D1 requires synchronous
// prepare()/bind() statement objects; R2 metadata writing mutates caller state.
//
// This runs in the loaded isolate, and because workerd hands the same env object
// to the tenant's fetch handler and Durable Object constructors, `this.env.X`
// and the constructor-parameter `env.X` both see the patched facades.
import { env } from "cloudflare:workers";
import { buildBindings, wrapR2Metadata, makeKVFromStub, makeD1FromStub, makeDOFromStub } from "facades.js";

try {
  for (const name of __cellhivePlatform.kvBindings) {
    if (env[name]) {
      try { Object.defineProperty(env, name, { value: makeKVFromStub(env[name]), writable: true, configurable: true, enumerable: true }); } catch (e) { /* keep raw stub */ }
    }
  }
  // DO namespaces are platform-side entrypoint stubs; rebuild the CF-shaped
  // facade on top of them (fetch(request)/rpcObject; ADR-184).
  if (__cellhivePlatform.doBindings.length > 0) {
    for (const name of __cellhivePlatform.doBindings) {
      if (env[name]) {
        Object.defineProperty(env, name, {
          value: makeDOFromStub(env[name]),
          writable: true, configurable: true, enumerable: true,
        });
      }
    }
  }
  // D1 prepare()/bind() are synchronous; keep prepared statements in the tenant
  // isolate and send only SQL/params through the props-bound entrypoint.
  for (const name of __cellhivePlatform.d1Bindings) {
    if (env[name]) {
      try { Object.defineProperty(env, name, { value: makeD1FromStub(env[name]), writable: true, configurable: true, enumerable: true }); } catch (e) { /* keep raw stub */ }
    }
  }
  // R2ObjectBody.writeHttpMetadata mutates the caller's Headers, so it must run
  // in this isolate (RPC would serialize the argument by value).
  if (__cellhivePlatform.r2Bindings.length > 0) {
    for (const name of __cellhivePlatform.r2Bindings) {
      if (env[name]) {
        try { Object.defineProperty(env, name, { value: wrapR2Metadata(env[name]), writable: true, configurable: true, enumerable: true }); } catch (e) { /* keep raw stub */ }
      }
    }
  }
} catch (e) {
  // Leave env unpatched; the tenant will surface the missing binding.
}

export * from "tenant.js";
