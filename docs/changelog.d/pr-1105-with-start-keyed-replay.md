## Phase 3.51 — with-start keyed committed-replay validation ordering (issue #1092 follow-up)

`signal_with_start_workflow` and `update_with_start_workflow` (the HTTP handlers
for the atomic start-or-attach + signal (#244) / + update (#479) primitives) ran
their fresh-start-only validation — the #610 payload/schema gate, and for
update-with-start the #373 `start_input` schema gate and the #684 semantic
validator — **before** their idempotency dedup. So a retry of an
*already-committed keyed with-start* — which must replay to its documented no-op
— was instead rejected `400`/`422` whenever validation had **tightened** between
the original delivery and the retry (a schema published/made stricter, a lowered
payload cap, or a validator that would now reject the same args). At-least-once
webhook/event retries broke precisely when a schema change landed. Fixed with an
early, **read-only** committed-replay probe at each handler's edge (keyed
requests only) that short-circuits to the documented replay response BEFORE any
fresh-start-only *rejection* — validation AND the #377 admission gate; a probe
MISS falls through to the untouched authoritative in-lock path, which reserves
the exactly-once signal/update and remains the source of truth for the
concurrent-first-delivery race. Mirrors #808 (plain start route) and #1092 (plain
signal route).

- **Core (`execution.rs`)**: promoted the two committed-replay lookups the
  in-lock dedup already uses to `pub` so the probe and the authoritative dedup
  share one source of truth (they can never drift): `lookup_idempotent_signal_dedupe`,
  `lookup_idempotent_update_dedupe`, and the `UpdateDedupeRow` it returns (now a
  `pub` struct with `pub` fields, deriving `Debug, Clone`).
- **`signal-with-start` handler**: new `probe_committed_sws_replay` (plugin
  `api.rs`) fans out across shards via `lookup_idempotent_signal_dedupe`; on a hit
  it returns `200 { signal_delivered: false }` with a best-effort SUCCEEDED audit
  (intentional asymmetry with the fresh-start arm's 503-on-audit-failure: the
  original start was already audited). It runs right after signal-payload
  extraction, keyed-only, before the #610 signal-payload schema gate.
- **`update-with-start` handler**: the `update_id` derivation (UUIDv5 over the DNS
  namespace of the idempotency key) and `probe_committed_uws_replay` are hoisted
  **above the found-shard scan and the #377 admission gate** (mirroring
  signal-with-start, whose probe already precedes both), not merely above the #373
  gate. The probe fans out via `lookup_idempotent_update_dedupe` and returns the
  resolved `UpdateWithStartOutcome` (`update_admitted: false`) — which flows
  through the handler's existing `Ok` arm (polling for the cached update result,
  writing the SUCCEEDED audit), giving a committed replay the SAME behavior as a
  fresh admission. The #377 admission gate, the #373 gate, and the #610+#684 blocks
  are all guarded `if probe_outcome.is_none()` (on a hit the handler short-circuits
  before the gate; on a miss the gate runs unconditionally for genuinely-fresh
  work); the authoritative call becomes
  `match probe_outcome { Some(o) => Ok(o), None => authoritative }`. This closes a
  P1 review finding: pre-hoist, update-with-start ran the admission gate BEFORE the
  probe, so a committed keyed replay retried while an operator had RAISED a gate
  returned a spurious `503` instead of the cached `200`/`update_admitted:false` —
  divergent from its own signal-with-start sibling and from the #808 invariant.
- **Fail-closed probes (P2 review finding)**: both probes previously swallowed any
  per-shard conn-acquire or lookup error into a dedup MISS, then fell through to
  fresh-start validation — reintroducing the bug under a transient owning-shard
  error. Both now track a `had_error` flag across the fan-out (three-state: hit /
  clean-miss / inconclusive): on a hit they short-circuit; a conn/lookup `Err`
  sets the flag and keeps scanning other shards (a later shard may hold the hit);
  after a no-hit loop they fail **closed** with a `503` when `had_error`, else
  return the clean-miss sentinel so the caller runs the authoritative in-lock path.
  `probe_committed_sws_replay` still returns `Option<Response>` (`Some` now also
  carries the `503`); `probe_committed_uws_replay` returns
  `Result<Option<UpdateWithStartOutcome>, Response>` (`Err` = `503`). Deliberately
  more conservative than #808's plain-start probe — the sibling found-shard scan
  in both handlers already fails closed on the same error class, and fail-closed
  preserves the never-reject invariant; a keyed client retries `503` idempotently.

**No new `WorkflowEvent` variant, no migration, no route/contract change.** The
in-lock exactly-once machinery is untouched — the probe is an additive read-only
fast path for committed replays only, never a replacement (two simultaneous first
deliveries both miss the probe and are serialized by the in-lock `ON CONFLICT`
reserve). The non-keyed and fresh-keyed paths are byte-for-byte unchanged (the
probe is keyed-only; a fresh key misses and the existing validation +
authoritative path runs). Tests (`autumn-harvest-plugin/tests/interface_schema_integration.rs`,
Docker-backed on Linux in CI, manifest line 85; ran green against local Postgres
16): the 4 committed-replay RED→GREEN proofs —
`sws_committed_keyed_replay_after_attach_short_circuits_before_schema_gate`,
`sws_committed_keyed_replay_after_terminal_fresh_start_short_circuits`,
`uws_committed_keyed_replay_after_running_short_circuits_before_validation`,
`uws_committed_keyed_replay_validator_not_rerun` — plus the two admission-gate
RED→GREEN proofs `uws_committed_keyed_replay_bypasses_a_raised_admission_gate`
(new; fails with `503` pre-hoist) and its symmetric guard
`sws_committed_keyed_replay_bypasses_a_raised_admission_gate` (both raise a
`WorkflowName` gate on the router's own `gate_cache` and assert `200`, not `503`),
plus the guard tests `sws_fresh_keyed_malformed_still_400`,
`uws_fresh_keyed_malformed_still_400`, `sws_uws_unkeyed_behavior_unchanged`,
`uws_paused_fresh_keyed_still_409` proving the fresh/unkeyed/paused paths are
preserved. Full 28-test suite green, no regression.
