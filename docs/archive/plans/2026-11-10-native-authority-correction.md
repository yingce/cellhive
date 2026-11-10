> 历史实施记录，已归档。进度、待办和运行环境描述只代表当时状态；当前契约及验收以 [known-issues](../../known-issues.md)、[decisions](../../decisions.md) 和对应报告为准。

# Capped append root and sealed generations

Implementation is in progress; deterministic and race tests cover the core protocol. Final provider and runtime acceptance remain pending.

## Authority and layout

Keep the logical authority key itself as a permanent native append object (root). Never delete, truncate, overwrite, or reset it, including logical Delete. Root position zero serializes first creation independently of clocks. Store fixed-size suffix-framed root descriptors containing format/version, generation number, random lifetime identity, selected segment identity, and CRC. The first root record additionally contains the bounded initial value, so a crash immediately after winning append-at-zero can be recovered without an independently mutable marker. Subsequent descriptors are small and contain no owner payload.

Generation segment keys are unique within that root lifetime, e.g. logical.g<generation>. Normal value and logical tombstone frames append to the current segment. Tokens identify root lifetime, generation, segment instance and end position. A segment tail is either a value/tombstone or a terminal SEAL. SEAL contains the complete next value/tombstone, next generation/instance, expected root descriptor end position and deterministic successor genesis bytes. It is never a token-bearing ordinary value. Physical keys are never reused.

Use a hard root byte cap and hard segment byte cap. Reserve enough segment bytes for one maximum-size seal before accepting a normal append. Roll on time or capacity, allowing frequent writes to move to a fresh segment before its cap rather than waiting for a new hour. Reject overlarge individual values before any mutation. Reserve root descriptor capacity BEFORE sealing: if no next descriptor fits, fail closed without sealing. Existing reads and same-generation writes that still fit remain available; eventual capacity exhaustion is explicit, not silent truncation.

## Initial create

Append a bootstrap record with the initial value to root position zero. Exactly one contender wins, even across different wall-clock windows. This is first creation's linearization point. Anyone can materialize its deterministic genesis at segment position zero, then read/append normally. A loser may observe the value or tombstone but cannot independently create another generation. A failed append with uncertain network outcome has normal ambiguous-commit semantics; recovery reads the root.

## Normal mutation and rollover

Normal CAS/create-after-tombstone/delete appends at the exact verified current segment position. Logical delete appends a tombstone; it never destroys root identity. Recreating a tombstone consumes that tombstone's exact position.

For rollover, append SEAL to the OLD segment at the caller's exact token position. This append is the mutation's linearization point. An intervening old writer wins that same position and makes sealing fail; a successful seal prevents the old writer from winning. Time windows decide when rollover is desirable, never which object is authoritative.

After sealing, a helper creates the next segment from the seal's deterministic genesis using append-at-zero. Concurrent helpers produce identical bytes; on conflict they verify the genesis with a bounded prefix read (the tail may already contain subsequent writes). Only after genesis exists does a helper append the next descriptor to the root at the exact expected root position. All helpers append identical descriptors. A descriptor conflict causes a root reread, not an unconditional overwrite. A root already beyond the target means publication finished; helpers cannot append an old descriptor at the newer position.

A reader finding SEAL must help finish publication or return a transient error; it must not return the pre-seal value. A rollover caller returns the successor genesis token after publication. Subsequent concurrent mutation may already have invalidated that token, which is ordinary CAS behavior.

## Reads and crash recovery

Read only the root suffix, then the selected segment suffix, plus bounded payload range reads. Cold reads do not need List or traversal from the initial generation. The first bootstrap record is needed only while generation zero is current or to recover its initial materialization. A reader whose selected segment was retired rereads the root. A bounded retry budget under perpetual rollover returns explicit contention instead of an unbounded loop.

Crash before seal: old value remains authoritative.
Crash after seal but before successor creation: root still selects the sealed segment; any helper reconstructs successor from seal.
Crash after successor creation but before root advance: same recovery; verify existing genesis and publish descriptor.
Crash after root advance: new generation is authoritative; retirement is optional cleanup.
A partial/invalid frame is corruption and must fail closed. This assumes native append atomically commits complete frames, as required by the existing append authority.

## ABA and safe retirement: important requirement

Simply DELETEing retired segments is NOT safe with arbitrarily delayed helpers. Counterexample: helper pauses before append-at-zero; generation advances and its segment is deleted; helper recreates its old genesis; a writer paused before appending at that genesis's end now succeeds with its old position, falsely acknowledging an obsolete mutation. Read-before-append checks cannot prevent this.

Recommended reclamation: replace each retired segment with a permanent ONE-BYTE retirement sentinel, using ordinary PUT only AFTER root advancement is verified. This is safe unconditional overwrite because that key is terminal, never selected again, and never reused. All valid writer positions are >= minimum framed record size, so none equals one; genesis append-at-zero also cannot succeed. Reserve sentinel length so it can never be a legal position. Readers encountering the sentinel reread root. An arbitrary delayed retirement PUT writes the same sentinel to the same permanently retired key and cannot damage a current generation. The latest root remains unchanged. This retires bulky data while retaining one tiny object per generation; it introduces no separate marker key or LTX marker. Existing delete/admin flows must not physically delete these stubs or reset the root while concurrent callers exist.

There can be arbitrarily many retired full segments after repeated crashes before cleanup. Maintenance may enumerate physical generations and compact those proven below the authoritative root generation; no hot-path List is needed. A successful rollover can compact its predecessor. Root capacity bounds the total number of generations even if cleanup never runs. Do not claim a strict two-segment physical footprint under crashes without completing maintenance.

An alternative is retaining original sealed segments until external quiescence proves no delayed request/helper exists; this has worse space cost. Physical deletion and immediate reclamation without stubs requires an additional verified fencing primitive or such quiescence proof.

## Quantified limits

The implementation uses a 64-byte descriptor and 128 MiB root cap. The bootstrap occupies 41+initial value bytes. The exact transition limit is floor((134217728-bootstrapBytes)/64), approximately 2,097,152 transitions: 239 years hourly, 4 years minutely, or 24 days secondly. Epoch switches inside a segment append ordinary value frames, NOT root descriptors. Capacity rollovers increase descriptor consumption. Actual constants and bootstrap/padding must determine the precise reported remaining-transition count.

Read transfer is bounded by suffix probe plus maximum allowed value/seal size, independently of accumulated root history. Root size and generation count have explicit lifetime bounds. Full retired payload cleanup is recoverable but not guaranteed instantaneous after crashes. Permanent retirement stubs cost one object per generation, plus provider metadata/billing; this tradeoff must be documented. At root exhaustion there is no transparent online reset: migration requires explicit externally enforced quiescence or a stronger provider conditional primitive.

## Required deterministic tests

- Intervening old-generation CAS beats seal: rollover rejects and preserves acknowledged value.
- Seal beats old CAS: old CAS rejects; reader recovers new value.
- Initial creates with divergent clocks: exactly one wins root append-at-zero.
- Crash at each stage: after bootstrap, seal, genesis creation, root publication.
- Concurrent identical helpers: one descriptor advance, same genesis, no duplicate mutation.
- Delayed helper after retirement: append-at-zero fails against sentinel.
- Delayed old CAS after retirement: append at old position fails against sentinel.
- Tombstone recreate and delete across rollover: one winner and no resurrection.
- Root/segment caps checked before mutation, reserved seal capacity, progress via capacity rollover.
- Long idle reads remain suffix/range-only with no List.
- Reader races retirement: retry from root; never return obsolete authority.
- Lifetime tokens and legacy input reject rather than unsafe migration.
