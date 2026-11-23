# Recovery discovery read cost mitigation

The elected recovery runner previously discovered every historical node-log every five seconds. Provider logical List resolves authority records with GET, then ListNode reads their contents again. This change bounds global/per-node discovery snapshots to one minute, measured from read start. Each pass still acquires election and reads known-node leases. Session or liveness changes refresh the node immediately; loss of election invalidates discovery. Recovery itself rereads authoritative session state. Failed discovery never advances its successful refresh timestamp.

Unknown nodes and late-published records are discovered by the periodic reconciliation; their background discovery can take one interval plus execution time. Request-triggered synchronous recovery is unchanged. This is a bounded mitigation, not elimination of historical scans or all background COS traffic. No storage format or authority root validation changes.

Validation: make test, make build, make vet and recovery/nodelog race tests passed. A counting test seeds 100 sealed records and verifies 11 warm five-second passes issue no node-log GET/List; verifies session change, lease expiry, failed refresh retry and leadership reacquisition. Existing real follower-spool test additionally verifies a formerly live node's acknowledged segment is recovered on the next warm pass after lease expiry.

Production measurement below records the SG mitigation; residual GET traffic remains.

## SG rollout (verified)

Source `24266ea6` pushed to main. Clean commit archive built with the prior verified workerd binary (SHA256 `afd36efa583587fd8fe1574507006f4bf3324693f82ce1398a33920d5a1b3a5f`). SG revision 139 updates only cell-agent, digest `sha256:2711ef37d22119face06c1e0381601c2b73b7851fa5ddef84ed23fae150831dd`. Both running agents match; agent binary SHA256 `1f050b8711b3e8e3741c3e9821080340f7c75b362eebf77618f4e4444ac9ba14`. Post-upgrade Helm diff is empty. USW unchanged. No data migration, deletion or Queue disable.

Existing fixture Worker send, automatic consumption and ACK drain passed. The 601-second observation completed: all 11 readiness/restart/Queue samples passed, all observed service containers ready with zero restarts. Steady agent logs contained no recovery errors or ERROR-level entries; existing purge retention, timer failures/exhaustion and one deferred epoch fold remain. Rollout produced transient control-peer failures; steady logs retain existing purge and timer warnings. Artifact directory: `/var/tmp/cellhive-sg-recovery-20261123/`.

## Request measurement

Two-agent totals, actual bucket GET counters (including Range GET): before release 120 seconds: 82.852 GET/s, 2.045 List/s, zero restored files. Initial post-rollout 180 seconds: 52.176 GET/s, 3.267 List/s, six restored files. Subsequent stable 180 seconds: 49.922 GET/s, 1.847 List/s, zero restored files. Stable GET rate is 39.75% lower than the pre-release window. Workload was not held constant; this is an observed aggregate decrease, not exact per-caller attribution or a guarantee of billing savings. Remaining approximately 50 GET/s requires separate background-loop attribution.
