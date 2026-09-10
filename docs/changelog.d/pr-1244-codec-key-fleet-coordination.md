## Phase — Codec key rotation: structural fleet-wide preconditions (issue #1244)

Follow-up to #948 / #1242. Two correctness preconditions for codec key
rotation were real but enforced only by operator discipline. This closes both
structurally.

**Durable key state.** New migration `harvest_codec_key_state` (one row per
shard, mirroring `harvest_workers`): `state` is `active` / `retiring` /
`retired`, with a partial unique index enforcing at most one `active` row.
`activate_codec_key` (new) writes it — the previous active key, if any,
becomes `retiring` with a timestamp — and flips the local registry, the same
zero-restart-window guarantee `set_active_key` always gave. Every process
refreshes its own view via `refresh_active_codec_key`, folded into
`enforce_timeouts_once` right beside the re-encryption sweep: shard-local, on
the connection already held, never able to break the rest of the tick.

**Retirement's write fence is now structural.** `retire_codec_key` gains a
`staleness_window` and a `recheck_delay` parameter. `FleetWriteFence::NotConfirmed`
(the default) requires every expected shard's durable key state to show the
key `"retiring"` for at least `staleness_window` — closing the "another live
writer" hazard #948 could only ask an operator to attest to, because once that
window safely exceeds twice the deployment's scanner-tick interval, no
conforming writer can still be encoding under the outgoing key. The census now
runs twice, `recheck_delay` apart, narrowing (not eliminating — no transaction
lifetime tracking) the "uncommitted append" hazard: a row that commits between
the two passes is still caught before anything finalizes.
`FleetWriteFence::ConfirmedByOperator` remains as the escape hatch for a
single-process embedder, skipping the durable wait but still running (and
rechecking) the census.

**Reader-capability handshake before activation.** `activate_codec_key`
refuses while any live worker (heartbeat within a caller-supplied liveness
window) does not advertise support for the version-2 envelope, closing the
other #948-documented hazard: a pre-upgrade reader silently passing a keyed
envelope through unchanged instead of decoding it. Every worker advertises its
own capability automatically — `payload_codec::advertise_codec_capability`
merges `codec_envelope_version` into `harvest_workers.labels` on every
`register_worker`/`heartbeat_worker` call, reusing the existing capability-label
column (issue #382) rather than adding a new one. A pre-#1244 binary never
writes the label at all, so it reads as version 1 and blocks activation by
default — fail closed.

**No new `WorkflowEvent` variant, no change to the append-only invariant, one
migration.** `retire_codec_key`'s signature changes (two new required
parameters) and `FleetWriteFence::NotConfirmed`'s behavior changes from an
unconditional refusal to the structural gate described above; every existing
caller either passes the escape hatch (unaffected beyond the two new
parameters) or adopts the structural path.

Tests: `payload_codec.rs` gains 4 unit tests for the capability-label merge
(fresh object, preserves operator labels, overwrites a forged/stale value,
tolerates a non-object `labels`). `codec_rotation_db_tests.rs` gains 8
DB-backed tests: the staleness window blocking a genuine zero census; a
purely-local flip never satisfying the structural gate, with the escape hatch
still working; the escape hatch still durably recording the retirement (a
Codex-round regression test — see below); the recheck catching a row that
commits between the two census passes (AC5's second interleaving); the
reader-capability handshake blocking/allowing/ignoring-a-stale-worker; and the
bounded-staleness refresh picking up another process's activation.

**Review fixes.** Four independent review passes (correctness/concurrency,
security, docs/STE, simplification) ran against the initial implementation.
Security and simplification passes found no defects requiring a code change.
The correctness pass found one real bug, fixed before this shipped:
`retire_codec_key`'s `FleetWriteFence::ConfirmedByOperator` escape hatch was
skipping the durable "retired" bookkeeping write along with the
staleness-window wait it is documented to skip, so a key retired through the
escape hatch stayed durably `"retiring"` forever. It also flagged a
documented (not code-fixed) limitation: `activate_codec_key` does not
coordinate across concurrent calls activating *different* keys, so two
uncoordinated concurrent rotations can leave different shards durably active
on different keys — now called out in both the rustdoc and the runbook. The
docs pass found a stray pre-#1244 runbook claim that the re-encryption
sweep's compare-and-swap "loses a race against ... a heartbeat checkpoint",
which CLAUDE.md's Engine Invariants section already established is not a
`harvest_events` writer — corrected.
