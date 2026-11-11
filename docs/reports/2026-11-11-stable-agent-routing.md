# Stable agent routing and unified runtime release

Ordinary workerd access now uses a ready-only ClusterIP Service. Owner, peer and log-registration addresses use ordinal-specific ClusterIP Services. Existing headless discovery and StatefulSet/PVC names remain unchanged. Per-node services permit peer-assisted startup recovery before readiness and select exactly one ordinal; session/epoch checks remain authoritative.

Reason: during SG Pod replacement the agent address changed from 10.0.16.82 to 10.0.19.197. DNS was current, while live workerd sockets still attempted the retired IP. This demonstrates stale-address retention, but does not uniquely attribute every historical SDK deadline failure to it. A stable Service avoids cached Pod-IP changes; it does not preserve an already-established TCP connection or justify replaying ambiguous writes.

This release also includes the previously uncommitted unified DO entrypoint, bounded local runtime telemetry spool and Go OTLP consumer, response-body cleanup, bounded unhealthy-runtime drain, shared cold-load admission, documentation and diagrams. The default spool directory follows CELLHIVE_RUNTIME_DIR/logs, avoiding non-root failures from attempting to create /.cellhive. The workerd polling fairness correction is committed separately as 1c2613d97; optimized binary SHA256 afd36efa583587fd8fe1574507006f4bf3324693f82ce1398a33920d5a1b3a5f.

Fresh local validation:
- make test, make js-test, make build and make vet passed; entrypoint tests and vet repeated after spool-default correction.
- Race tests for runtime spool, workerd pool and unified DO entrypoint passed.
- Helm routing contract passed for 1, 2 and 3 agents.
- Real-process session recovery passed: 80 acknowledged values, interrupted recovery, empty replacement disk refusal, owner SIGSTOP/takeover and old-session fencing.
- Multi-class DO E2E passed three cold/idle cycles with independent epochs and isolated manifests.
- Actual DO/log-poll network recovery changed backend IP and recovered in about 2 seconds without runtime restart. Fixture ran as root because host JS bind mounts are not readable by the image UID; this is not production permission acceptance. The final image must also be verified as its non-root user.

Deployment and live Pod-replacement acceptance are recorded separately in the operations release report. No full provider fault-injection or 10GiB E2E rerun is claimed. The separate USW lease_replaced observation remains open.

Deployment completed: SG Helm 131 / USW Helm 118, same immutable image `sha256:127f15ba5a9cb98d458a2988a69d1e3f2e3d8e794edce515defc38fee280b3db`. All components Ready and sampled new-Pod restart counts zero. SG public KV 12/12 success, first 1675.6ms then 110.9–159.8ms; runtime sockets used fixed Service addresses, telemetry cursor progressed with zero sampled drops/errors. USW runtime-to-agent Service health passed; no public business E2E is claimed. Operations report: `/opt/cellhive-helm/reports/2026-11-11-stable-routing-runtime-release.md`.
