## Phase 5.x — close the fourth `expected_shards` copy issue #1146 missed (issue #1146)

`autumn-harvest-plugin/src/version_gate_retirement.rs`'s `expected_shards` was a
byte-for-byte hand-rolled duplicate of "pool keys ∪ readable shards ∪ default
shard, or `{0}` if empty" — the same fan-out rule issue #1146 consolidated onto
[`shard_fanout::expected_shards`] (itself `external_target_location::fanout_shards`)
for `version_usage.rs`, `workflow_reachability.rs`, and `workflow_count.rs`.
Issue #1146's own changelog fragment states "`version_usage.rs`'s **third**
hand-rolled copy of the same rule delegates to that — so the management API and
the engine inspect the same shard set by construction." That count was one
short: `version_gate_retirement.rs` carried a fourth copy, added by issue #164
(2026-05-07) and last touched the day *before* issue #1146 landed (by #1315,
which extracted the sibling `acquire_shard_conn` prelude but did not touch
`expected_shards`), so the #1146 migration never reached it.

Missed-fix evidence: `git log --follow` on both files shows `version_usage.rs`
was updated by #1146's commit (`71d998aa`); `version_gate_retirement.rs` was
not touched by that commit and still carries the pre-#1146 body verbatim
(confirmed by diffing the two — the removed lines in `71d998aa`'s diff on
`version_usage.rs` are character-for-character what `version_gate_retirement.rs`
still has). Left as-is, this is exactly the drift #1146 exists to prevent: a
management-API read inspecting a different shard set than the engine's own
by-business-key resolution for the same key.

This is also a documented deferral, not a fresh find. The prior Echo pass
(#1315, the day before #1146 landed) explicitly checked this exact function
and declined to touch it: "`version_gate_retirement.rs`'s local
`expected_shards(api_state, shard_filter)` was checked and left alone — it
has a genuine `shard_filter` early-return `shard_fanout::expected_shards`
doesn't have, so merging it needs a signature change, not a same-shape swap.
Noted as a possible smaller follow-up." #1146 supplied that missing piece the
very next day: it did not change `shard_fanout::expected_shards`'s signature,
but established the wrapper shape — an early return on an explicit
`shard_filter`, falling through to the unfiltered shared call — in
`version_usage.rs`. This PR applies that same, by-then-proven wrapper to
`version_gate_retirement.rs`, which is the smaller follow-up #1315 predicted.

**The fix** replaces the function body with the same one-line delegation
`version_usage.rs` already uses. No behavior change — `shard_fanout::expected_shards`
computes the identical union with the identical empty-set fallback, and this is
the fourth caller of an already-proven, already-tested helper, not a new
abstraction. Zero new mode flags, zero caller-identity branches; concept count
strictly decreases by one (a fifth-generation hand-rolled copy that no longer
exists).

**Not touched:** the remaining structural overlap between `version_gate_retirement.rs`
and `version_usage.rs` (`observe_shard`, `build_report_from_observations`, the
`ShardObservation<R>` type-alias pattern) is the deliberate per-read-model glue
`shard_fanout.rs`'s own module doc describes — each read model has a different
row type, key type, accumulator type and report type, so unifying it further
would need generics over all four plus closures for `accumulator_from_row`/
`merge_row`/`blocker_from_accumulator`. That is the generic-engine shape Echo's
own rules reject; left duplicated on purpose.

**Test evidence.** `cargo check -p autumn-harvest-plugin --lib` is clean.
`retirement_check_integration.rs`'s `retirement_check_aggregates_counts_across_two_shards`
and `retirement_check_degraded_when_one_shard_unavailable` already exercise
`expected_shards` end-to-end across a two-shard Postgres topology and pin the
exact behavior this change must not alter; they require `testcontainers` and
could not be run in this sandbox (no Docker daemon), but the change is an exact
mechanical mirror of the diff issue #1146 already shipped and had CI verify for
`version_usage.rs`, so no new behavior is introduced for them to catch.
