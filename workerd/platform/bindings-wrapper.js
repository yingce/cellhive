// Loaded-worker entry that patches the importable `env` with local facades for
// bindings whose Cloudflare API cannot cross RPC as-is. DO namespaces and
// Workflows require synchronous local method objects; D1 requires synchronous
// prepare()/bind() statement objects; R2 metadata writing mutates caller state.
//
// This runs in the loaded isolate, and because workerd hands the same env object
// to the tenant's fetch handler and Durable Object constructors, `this.env.X`
// and the constructor-parameter `env.X` both see the patched facades.
import "__cellhive-bindings.js";
import { env } from "cloudflare:workers";
import { prepareBindingEnv } from "facades.js";

prepareBindingEnv(env, __cellhivePlatform);

export * from "tenant.js";
