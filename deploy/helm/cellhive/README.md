# CellHive Helm

## Agent-first upgrades

For ADR-302 upgrades, first set `cellAgent.image.digest` to the new immutable digest while retaining the global `image.digest`. Wait for every cell-agent to be Ready on that digest. Then set the global digest to the new version and remove the agent override to roll out the runtimes. The optional agent image map inherits unspecified fields from the global image.

## Bearer JWT Cutover

Keep the existing `CELLHIVE_ROOT_KEY` unchanged. JWT role keys use a new
purpose-separated derivation; persisted secret encryption and Hyperdrive
credential derivations are unchanged. The root is supplied to trusted Go
processes, never tenant JS bindings. Do not copy root or role signing keys into
tenant configuration or collector headers.

Remove retired `CELLHIVE_ADMIN_TOKEN` and `CELLHIVE_OIDC_*` entries from existing
Secrets and external environment configuration. They do not configure the new
authentication scheme. Use short-lived Bearer JWTs; the CLI can bootstrap from
the root to provision a persisted issuer. Issuer registry management requires
the bootstrap admin JWT. Coordinate the JWT-only agent/runtime/client upgrade;
do not assume mixed old-header and new-JWT versions interoperate.

The chart does not install a metrics collector. Under ADR-178, private
`:7001/metrics` remains an explicit unauthenticated pull exception; restrict it
to trusted collectors using network controls, never public ingress or tenant
egress. No JWT refresher, proxy or extra service is required. Business monitoring
uses authenticated, filtered `/v1/control/metrics`; admin-listener full `/metrics`
requires admin or a namespace JWT explicitly granting `*`. Issuer registry CRUD
remains bootstrap-admin-only even for wildcard issuers. See
`../../observability/README.md`.
