# Session recovery production rollout — 2026-10-24

Source: `960bc11` (pushed). Image built from that clean commit, excluding the independent OTLP workspace changes: `sha256:01797c07c6ccefb32ce0956e81b36e1f20e2e299c23f463b3749815417d25c79`.

The committed tree passed `make test` and the two-process session recovery E2E (8.27 seconds, 80 values verified). Earlier vet/build/race evidence is in `session-recovery-2026-10-24.md`.

## USW — deployed

Before deployment, 19 historical v2 sessions had stale follower URLs. The retained session log was located on the peer PVC for each session. An operations helper checked the current node/instance/address, refused the live owner session, sealed only the explicitly mapped historical session, validated the entire contiguous tail and every LTX CRC/epoch, and used the recovery manager to persist coverage. All 19 sessions were sealed with 12791 covered fragments. Spools were retained.

Stopped all old cell-agent Pods while retaining PVCs, waited for lease expiry, then deployed using `/opt/cellhive-helm/bin/cellhive-helm usw upgrade`. Helm revision 42 is deployed; all six application Pods have the expected imageID, are Ready, and have zero restarts. Final Helm diff is empty. Both live leases and both current sessions use the new v3 recovery protocol with explicit follower identities; no historical unsealed sessions remained in the post-upgrade inventory.

Real HTTP verification wrote two values before shutdown through different nodes and read each value through both nodes after upgrade. New PUT/GET pairs passed through both nodes. The public demo function returned HTTP 200 with ok=true. These are rollout checks, not a fresh latency benchmark. A single startup waker warning reported an already-deleted resource incarnation; no recovery/protocol/identity errors were observed in the checked window.

The public demo response includes sensitive environment variables. Stored verification output is redacted; fixing that demo is separate work.

## SG — not deployed

SG had 39 historical unsealed sessions. Two lack retained session directories on either current PVC: owner node 0 session `1792424274221850178`, and owner node 1 session `1792293594155783575`. This does not itself prove loss of acknowledged data, but no proof currently permits sealing them as empty. The new startup gate would remain unavailable on these records. SG therefore remains on the previous image, with existing service running. Another retained session is approximately 20 GB and still requires recovery.

The previously identified session `1792144743099984471` was fully verified through final 22395 on its retained follower; all 22395 segments were uploaded successfully and the record was read back sealed with `covered_seq=22395` at 13:46 UTC. Its retained follower spool remains intact. No manual sealed-state rewrite or spool deletion was used.

Detailed per-session mappings, helper source, inventories, and command results are in `/opt/cellhive-helm/reports/session-recovery-2026-10-24/`.

## Follow-up: authorized legacy loss and pinned control takeover

The operator subsequently accepted partial historical data loss. Exact SG sessions `cellhive-sg-cell-agent-0/1792424274221850178` and `cellhive-sg-cell-agent-1/1792293594155783575` were closed through guarded CAS after backing up original logical and physical records in `diag/session-loss-20261024/`. Their records carry `recovery_resolution.kind=operator_accepted_missing_tail`; neither has invented coverage. All other 37 historical sessions were recovered with covered_seq totaling 1,518,075 fragments. This is distinct from claiming that all those fragments were previously missing from the bucket.

The 20GB file belongs to session `1792172483597123213` (started 2026-10-16 17:41 UTC). Its physical header inventory is mostly `vwork-40/__invocations__/main` (20,646,701,068 bytes), plus `vwork-24/__queue__/events` (20,582,308 bytes). Old invocation records may be redundant; the unsealed record required conservative recovery but did not prove every byte needed replay. Recovery respected cutoff 1,144,936; one physical frame beyond that cutoff was not included. Original spools remain retained.

Source `85debc1` fixes TailPage's false truncation error at whole-fragment byte boundaries and preserves typed loss-resolution metadata. SG first migrated successfully at Helm revision 54; its pre-upgrade values and post-upgrade writes passed real HTTP checks through both nodes. The USW revision 43 canary then found a separate pinned-control bug: resource registration returned 403 although a read-only bucket restore showed txid 4518 with the registration. New owner node 1 was serving an older pinned local DB at txid 4516.

Source `a9ab5c0` separates ownership invalidation from normal eviction. Invalidate/TryInvalidate bypass residency Pin while respecting request/open gates; a busy or failed stale-file invalidation blocks claim. The timer takeover path also invalidates pinned obsolete replicas. Related race tests, make test and make vet pass. The process E2E now retains B's old control DB before restart and makes B take over: the old binary fails with an empty resource list; the fixed binary restores registrations and 80/80 acknowledged KV values (8.31s). Image built from the clean committed archive: `sha256:21854979b1dc2b8a9b5f28a91a5f0ef577d257c2608376862b13240c3603b320`.

Detailed operations evidence, exact audit records, read-only scope inventory, maintenance sources/tests and deployment logs are retained in `/opt/cellhive-helm/reports/session-recovery-2026-10-24/`.

### Final deployment readback

Final `a9ab5c0` image is deployed in SG revision 55 and USW revision 45. Each region has two cell-agents, six Ready application Pods, zero restarts and exact expected imageIDs; final Helm diffs are empty. Current sessions use v3 follower identities. SG and USW real HTTP pre/post values and new PUT/GET pass through both nodes. USW's original resource registration was recovered without recreating it; a further node-0 restart forced control ownership onto node 1 (epoch 2944), and both the registry and unique pre-restart KV values remained correct. Temporary single-node USW staging was removed; runtime Pods were refreshed to load both log registration addresses. A known deleted-resource waker warning remains in the USW log window and is outside this session/pinned-replica fix.
