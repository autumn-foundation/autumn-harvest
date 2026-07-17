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
fresh-start-only validation; a probe MISS falls through to the untouched
authoritative in-lock path, which reserves the exactly-once signal/update and
remains the source of truth for the concurrent-first-delivery race. Mirrors #808
(plain start route) and #1092 (plain signal route).

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
- **`update-with-start` handler**: the `update_id` derivation (UUIDv5 over the OID
  namespace of the idempotency key) is hoisted above the #373 gate; new
  `probe_committed_uws_replay` fans out via `lookup_idempotent_update_dedupe` and
  returns the resolved `UpdateWithStartOutcome` (`update_admitted: false`) — which
  flows through the handler's existing `Ok` arm (polling for the cached update
  result, writing the SUCCEEDED audit), giving a committed replay the SAME
  behavior as a fresh admission. The #373 and #610+#684 blocks are guarded
  `if probe_outcome.is_none()`; the authoritative call becomes
  `match probe_outcome { Some(o) => Ok(o), None => authoritative }`.

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
`uws_committed_keyed_replay_validator_not_rerun` — plus the guard tests
`sws_fresh_keyed_malformed_still_400`, `uws_fresh_keyed_malformed_still_400`,
`sws_uws_unkeyed_behavior_unchanged`, `uws_paused_fresh_keyed_still_409`
proving the fresh/unkeyed/paused paths are preserved. Full 26-test suite green,
no regression.
