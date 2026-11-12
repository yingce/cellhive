# Admin owner forwarding stalled on stable Services

SG and USW per-ordinal cell-agent Services exposed 7001 but not 8082.
Authenticated admin forwarding preserves the caller credential and contacts the
owner admin listener on 8082. Landing on a non-owner therefore timed out after
30 seconds with 502; landing on the owner returned normally.

USW vwork-81/comfly-design reproduced the exact query: node 0 failed after
30.497s; node 1 returned 200 in 0.482s. The domains/routes endpoints reproduced
the same node-dependent failure. SG reproduced the transport issue too, but
this namespace's releases are empty there.

Fix: add 8082 to each per-ordinal Service in the source and operational charts.
The 1/2/3-replica chart test failed before the fix and passed afterward. Live
pre-deploy diff contained only the additional ports on the two Services in each
region. No image or authority reclamation code was shipped.

Deployed SG Helm 133, USW Helm 120. Post-fix USW node 0 query: 200/0.491s;
node 1: 200/0.640s. Both returned 22 releases, active version 22. Domains/routes
also returned 200. SG both-node queries passed (0.165–0.249s).

Operational incident during upgrade: the deployment wrapper compared the
StatefulSet's stale currentRevision with updateRevision, causing an unnecessary
agent-1 replacement in each region. Subsequent replacements were stopped;
agent-0 was preserved. Both replacement Pods reached Ready. The operational
script now compares actual Pod controller-revision-hash/image with the desired
revision/image. Regression covers current Pods and stale revision/image.

The readback blocker is resolved. No business deployment was reissued and no
end-to-end publication claim is made. Changes remain uncommitted.
