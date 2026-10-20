## Phase — shared result-to-observation mapping for cross-shard fan-out reads (🪞 Echo clone-class merge)

`usage.rs`, `workflow_count.rs`, `version_usage.rs`, `version_gate_retirement.rs`
and `workflow_reachability.rs` each hand-copied the identical tail of their
`observe_shard` function: run the shard's own query, then
`match result { Ok(rows) => ShardObservation { shard_id, rows, error: None },
Err(e) => ShardObservation { shard_id, rows: Vec::new(), error:
Some(e.to_string()) } }`. `shard_fanout::acquire_shard_conn` (#1315) already
centralised the connection-acquisition prelude that precedes this match in
all five files; that extraction's own PR body noted the guard was
"hand-copied into eight read models" but left this result-mapping epilogue
untouched in each of them.

**What shipped.**

- `shard_fanout::observe_shard_result<R>(shard_id, HarvestResult<Vec<R>>) ->
  ShardObservation<R>`, a pure function with no DB access.
- All five call sites now call it instead of inlining the match. Each
  `observe_shard` keeps its own name, signature and surrounding doc comment —
  only the shared tail changed, each site's own extra setup (e.g.
  `filters.shard_id = Some(shard_id)`) stays where it was, before the call.
- Two files (`version_usage.rs`, `version_gate_retirement.rs`) carry no other
  change: their bodies were already down to this exact tail after #1315.
- `preflight.rs`'s `observe_shard_availability`, `queue_coverage.rs`'s
  `observe_shard` (returns a `(ShardObservation, BTreeSet)` tuple with an
  early "nothing pending" branch) and `shard_health.rs`'s `observe_shard`
  (returns `ShardHealthRow` directly via a different connection path, no
  `ShardObservation` at all) were checked and left alone — none shares this
  exact `Ok(rows)`/`Err(e)` shape, so folding them in would need a mode
  branch on return type, not a same-shape swap.
- Characterization tests for both branches (`Ok` with rows, `Ok` with an
  empty vec, `Err`) added to `shard_fanout.rs`'s test module — pure,
  no DB and no pool needed.

**Evidence.** `jscpd` (rust format, `--min-tokens 40`) found this exact
match/construct shape duplicated across these five files (and only these
five — `queue_coverage.rs` and `shard_health.rs` diverge, confirmed by
reading each). `git log` shows all five files, plus `shard_fanout.rs`
itself, were touched together in #1315, which documents the shared
"resolve a shard query result into a fan-out `ShardObservation`" rule by
name (issues #164 through #596) while extracting only its first half.

No behavior or public API change beyond the new `pub`
`shard_fanout::observe_shard_result`. `cargo test -p autumn-harvest-plugin
--lib shard_fanout::`, `cargo clippy -p autumn-harvest-plugin --all-targets
-- -D warnings`, and `cargo fmt --check` are clean.
