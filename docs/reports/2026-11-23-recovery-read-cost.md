# Recovery discovery read cost mitigation

The elected recovery runner previously discovered every historical node-log every five seconds. Provider logical List resolves authority records with GET, then ListNode reads their contents again. This change bounds global/per-node discovery snapshots to one minute, measured from read start. Each pass still acquires election and reads known-node leases. Session or liveness changes refresh the node immediately; loss of election invalidates discovery. Recovery itself rereads authoritative session state. Failed discovery never advances its successful refresh timestamp.

Unknown nodes and late-published records are discovered by the periodic reconciliation; their background discovery can take one interval plus execution time. Request-triggered synchronous recovery is unchanged. This is a bounded mitigation, not elimination of historical scans or all background COS traffic. No storage format or authority root validation changes.

Validation: make test, make build, make vet and recovery/nodelog race tests passed. A counting test seeds 100 sealed records and verifies 11 warm five-second passes issue no node-log GET/List; verifies session change, lease expiry, failed refresh retry and leadership reacquisition. Existing real follower-spool test additionally verifies a formerly live node's acknowledged segment is recovered on the next warm pass after lease expiry.

Production deployment and GET reduction pending measurement.
