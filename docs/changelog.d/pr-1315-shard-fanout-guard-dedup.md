## Phase — shared connection guard for cross-shard fan-out reads (🪞 Echo clone-class merge)

Eight management-API read models (`canary`, `status_summary`, `usage`,
`workflow_count`, `workflow_reachability`, `queue_coverage`,
`version_gate_retirement`, `version_usage`) each hand-copied the same
per-shard connection guard every time a new cross-shard fan-out endpoint was
added between issues #164 and #796: resolve `Option<DbPool>` or record
`"shard {id} has no configured storage pool"`; acquire a connection or
record `"database connection for shard {id} could not be acquired"`.
`shard_fanout.rs` (added in #1052 / issue #756) already centralises the rest
of this scaffolding (`pools_by_shard`, `expected_shards`,
`collect_fanout_rows`) and its own module doc already describes this exact
guard as shared, but it was never actually extracted — every new endpoint
kept re-typing it.

**What shipped.**

- `shard_fanout::acquire_shard_conn<R>(shard_id, pool) -> Result<PoolConn,
  ShardObservation<R>>`, built on the crate's existing `api::acquire_conn`.
  All 8 call sites now call it instead of inlining the guard.
- `version_gate_retirement.rs`'s local `ShardObservation` struct (a
  byte-identical duplicate of `shard_fanout::ShardObservation<R>`) is now a
  type alias, matching the pattern `version_usage.rs` already used; its
  duplicate `pools_by_shard`/`age_secs` are deleted in favor of
  `shard_fanout`'s.
- `version_gate_retirement.rs`'s local `expected_shards(api_state,
  shard_filter)` was checked and left alone — it has a genuine `shard_filter`
  early-return `shard_fanout::expected_shards` doesn't have, so merging it
  needs a signature change, not a same-shape swap. Noted as a possible
  smaller follow-up.
- Characterization tests for both guard branches added to
  `shard_fanout.rs`'s test module (no-pool; connection-refused, using a
  `deadpool` pool pointed at an unroutable local port so it's deterministic
  without a live database).

**Evidence.** `jscpd` (config in the PR body) found 10 pairwise matches of
this fragment across 7 of the 8 files before this change and 0 after; the
8th (`queue_coverage.rs`, whose copy wraps the result in a tuple) was
confirmed identical by inspection. `git log -S"could not be acquired"`
shows each of the 8 occurrences was introduced fresh when its endpoint was
added and never edited afterward — eight separate hand-copy events. Direct
precedent for this remedy: issue #1150, a missed-fix defect where
`worker_covers_shard`'s empty-array-shard case was fixed in one copy
(`queue_coverage.rs`, #774) and independently missing in another
(`shard_health.rs`, #522) — the same kind of drift this guard was one
future edit away from repeating.

No behavior or public API change beyond the new `pub`
`shard_fanout::acquire_shard_conn`. `cargo test -p autumn-harvest-plugin
--lib`, `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --check`
are clean.
