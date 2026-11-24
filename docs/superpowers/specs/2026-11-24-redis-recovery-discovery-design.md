# Redis recovery discovery

Status: locally implemented and verified; not deployed. Extends Redis authority design without moving durable node-log bodies out of object storage.

## Contract

Redis coordination mode maintains a generation-guarded pending-session directory. Each entry carries an immutable discovery hint (node/session/follower coordinates), not recovery authority. Before creating a COS open session, register the hint in Redis. Registration failure prevents Open and therefore prevents a fleet acknowledgement. Retrying an existing session registers again before checking the immutable COS coordinates. Extra hints are safe; missing hints for acknowledged sessions are forbidden.

Discovery reads Redis only, including the initial hint body, with no COS List or GET for live-session discovery. Startup also uses pending discovery and explicitly point-checks the proposed session ID in COS, so sealed IDs cannot be reused. Actual recovery retains fresh lease validation and authoritative COS Get/CAS. Only an observed COS sealed record permits removing a hint. A crash after registration and before COS publication leaves a harmless candidate; absence never removes it because a delayed publisher could still create the record. Repeated attempts may check that candidate but cannot acknowledge missing business data. A crash after sealing and before hint removal is reconciled by a subsequent authoritative Get. Redis failure never falls back to bucket discovery.

Object mode retains existing discovery. Diagnostic full-history listing retains COS semantics. Redis entries use the same authority generation guard and contain no credentials.

## Migration

Initial offline coordination initialization seeds all nonsealed node-log hints before activating the generation. Existing Redis installations must stop all writers and run `cell-agent coordination-recovery-init --offline` before starting this binary. The command enumerates COS once, validates record identity and status, seeds candidates, and writes an index-version marker last. Retrying a failed offline seed is safe. The completed migration upgrades the bucket binding to version 2, which previous v1 binaries reject on startup. New runtime startup requires version 2 and the marker; it never silently accepts an empty/uninitialized directory. Old binaries must not write after migration, because they do not register sessions. Deployment is a separate maintenance cutover, not part of local implementation verification.

## Verification

Exercise prepublication registration failure, crash before COS creation, failed post-seal removal, retry of an existing open session, immutable coordinate mismatch, absent marker, corrupt seed, generation loss, Redis outage, full historical seed, and discovery after new Manager construction. Assert actual Redis discovery performs zero COS List/GET while explicit Get still reads COS. Run nodelog/recovery/coordination/agent tests and relevant race tests.

## Epoch GC follow-up

GC candidate scheduling should be triggered by epoch transitions and successful Fold, with durable delayed retries across restart/owner transfer. A transition alone does not prove an old epoch deletable. Keep restore-chain proof, previous-epoch retention, one-hour upload-age grace, owner/session fencing and exact scanned-object deletion. This change does not alter GC execution yet; a follow-up must define atomic event publication and pending-candidate completion before replacing periodic OwnedScopes traversal.
