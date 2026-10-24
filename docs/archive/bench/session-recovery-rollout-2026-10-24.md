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
