## Phase — source-aware shard coverage for `GET /workers` fan-out (issue #1213)

Filed from Codex round 7 on #1207 (itself part of #961): `GET
/workers?shard_id=B` could return a worker registered with the empty
(auto/legacy) `shard_assignments` shape in shard A's database, even though
that shape means "covers whatever shard the row was read from" (#1150), not
"covers every shard".

**Root cause.** `list_workers_handler` and `drain_preview_handler` both fan
out to every shard's own database (`observe_shards` / a plain
`pool.iter_shards()` loop) and, for each shard's rows, applied
`apply_worker_filters`'s shard retain using the **caller's requested**
`shard_id` — not the shard each batch of rows was actually read from. Since
`shard_assignments_cover` treats an empty array as covering *any* requested
shard, a shard-A row with `shard_assignments: []` was evaluated against
shard B's request and incorrectly kept.

**What shipped.**

- `autumn_harvest::workers::shard_assignments_cover_from_source(assignments,
  source_shard_id, requested_shard_id)`: the empty-array case now covers the
  request only when `source_shard_id == requested_shard_id`; a non-empty list
  is still evaluated by membership against `requested_shard_id` regardless of
  source (a multi-shard worker's row is replicated identically into every
  shard it's assigned to, so that half of the contract didn't need to change).
  `shard_assignments_cover(assignments, shard_id)` — the single-shard
  convenience form every other consumer (`fleet_health.by_shard`, queue
  coverage, preflight) already calls correctly with the row's own source —
  is now defined in terms of it (`source == requested`), so there is still
  exactly one predicate.
- `list_workers_handler`: the per-shard query no longer forwards the
  requested `shard_id` into `list_workers`'s in-process filter (it isn't
  shard-invariant, despite the old comment claiming it was). Each shard's
  batch of rows is instead filtered with
  `shard_assignments_cover_from_source` using that shard's own id as the
  source, before the union is deduped.
- `drain_preview_handler` had the identical bug pattern (same fan-out loop,
  same `..filters.clone()` forwarding) and is fixed the same way.

**Test evidence.**

- `autumn-harvest/src/workers.rs`: unit tests for
  `shard_assignments_cover_from_source` covering the source-mismatch empty
  case, the source-independent non-empty case, the malformed-value case, and
  a parity check against `shard_assignments_cover` when source equals the
  request.
- `autumn-harvest-plugin/tests/worker_shard_filter_integration.rs` (new,
  two-live-shard-database fixture, matching the issue's suggested test
  shape): a worker registered `shard_assignments: []` in shard 0 only is
  excluded from `GET /workers?shard_id=1` and `GET
  /workers/drain-preview?shard_id=1`, while still included in
  `?shard_id=0`; a genuinely shard-1-narrowed worker is included in
  `?shard_id=1` throughout.

No public API or response-shape change. `cargo test -p autumn-harvest --lib`,
`cargo test -p autumn-harvest-plugin`, `cargo clippy --all-targets -- -D
warnings`, and `cargo fmt --check` are clean.
