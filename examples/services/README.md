# Service binding: fetch + RPC

This example uses one standard Wrangler `services` binding for both forms:

- `env.API.fetch(request)` calls the target Worker's default `fetch` handler.
- `env.API.greet()` and `env.API.sum()` call the named `Api` entrypoint.

The URL passed to `env.API.fetch()` is only the target request URL. The target
Worker is selected by `service: "service-api"`, not by the URL hostname.

Deploy `api` first, then `caller`, in the same namespace. The caller deploy
pins the target version, so redeploying the API does not silently change an
already-published caller version.
