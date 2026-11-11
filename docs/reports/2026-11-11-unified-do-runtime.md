# Unified DO runtime entrypoint

Local implementation; not committed or deployed.

`cmd/do-runtime` is now a thin entrypoint into `internal/doruntimeapp`.
One Go process renders the workerd configuration, then owns the native child,
output gate, paging registry, owner renewal, telemetry and drain/join lifecycle.
The standalone `cmd/do-supervisor` and `deploy/do-runtime-gated.sh` are removed
from source/build/container packaging. Both README languages describe the new
process model. Service/StatefulSet/PVC identities and telemetry service identity
are retained. Object-store prefixes and protocol owner identities are unchanged.

The default command enables the output gate. `-render-only` remains diagnostic.
`-gate=false` explicitly preserves the preexisting non-durable fixture path for
binding-auth/network tests and the existing ungated development profiles; it is
not suitable for RPO=0 production. Restore maintenance options are available on
the same do-runtime executable.

## Verification

- Native launcher binding test passed, as did internal/doruntimeapp,
  internal/dosupervisor and internal/doruntime suites, with required SQLite tags.
- Lifecycle tests moved with the implementation; race tests and vet passed.
- Real workerd local-filesystem-bucket E2E: native SQLite export/import,
  activation exclusion during import, active-shard rejection, reexport after
  cold restart; multi-class independent epochs and three cold/idle cycles;
  five idle-retire/reopen cycles with16 concurrent reads; retirement after
  owner change; two-agent/two-runtime fleet paging and two crash takeovers.
- Fleet takeover epochs1→2→3, values10→11→12; cold SQLite logical size16920576
  bytes, allocated135168 bytes. Each takeover waited31seconds for lease expiry.
- A second transfer run omits CELLHIVE_DO_GATE_URL to exercise derivation from
  the unified launcher's gate listener; see `/tmp/do-unified-derived-gate.log`.
- Helm lint/render, Kustomize rpo0 render, chart mirror, shell syntax and
  whitespace checks passed. No image build or production rollout performed.

Evidence: `/tmp/do-unified-{tests,race,transfer,multiclass,paging,idle,derived-gate}.log`.
Disposable tests retain artifacts under paths printed by their logs. Existing
production services and migration volumes were not modified.
