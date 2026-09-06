## Phase — fleet health `by_shard` counts empty-assignment workers (issue #1208)

Split out of PR #1207 (issue #961). `GET /workers/health`'s `by_shard`
tally dropped a worker advertising the empty (auto/legacy)
`shard_assignments` shape: an empty array names no bucket, so a healthy
worker polling shard 7 could leave `by_shard[7] == 0`.

**Root cause.** `workers_health` fans out to every shard, dedups the union
by `worker_id` (freshest heartbeat wins), then tallies `by_shard` from the
literal `shard_assignments` array. The empty-array shape means "covers
whatever shard the row was read from" (issue #1150), but dedup discarded
that source shard before the tally ran, so an empty-array row had no bucket
left to attribute to. The other five instances of this class were closed by
routing through `shard_assignments_cover` (PR #1207), but that predicate
answers a membership question — "does this worker cover shard N?" — not
an attribution question — "which bucket does this row increment?" — so a
predicate swap could not fix this one.

**What shipped.**

- `dedup_worker_sources_by_freshest(Vec<(i32, WorkerRow)>)`: the same
  freshest-wins dedup as `dedup_workers_by_freshest`, but keeps the shard id
  each surviving row was read from. `dedup_workers_by_freshest` is now
  defined in terms of it (tagging every row with a throwaway `0` and
  dropping the tag on return), so there is one dedup rule, not two.
- `tally_by_shard(&[(i32, WorkerRow)])`: a pure, unit-testable merge-layer
  helper. An empty array is attributed to the row's source shard; a
  non-empty array is attributed to every shard it names, regardless of
  source, exactly as before; a malformed (non-array) value is attributed to
  nothing.
- `workers_health` tags each fanned-out row with the shard `observe_shards`
  read it from, dedups with the source-aware function, and tallies
  `by_shard` from the result.

No public API or response-shape change; `by_shard` only gains counts it was
previously dropping.

**Test evidence.**

- `autumn-harvest-plugin/src/api.rs` unit tests: an empty-assignment worker
  attributed to its source shard; a non-empty-assignment worker attributed
  literally regardless of source; a malformed value attributed to nothing;
  a multi-shard worker's replicated rows deduped before the literal tally
  runs so it isn't double-counted; the dedup keeps the freshest row's source
  shard, order-independent.
- `autumn-harvest-plugin/tests/fanout_degradation_integration.rs` (new
  cases, two-live-shard-database fixture): `GET /workers/health` counts an
  empty-assignment worker under the shard it was actually read from and an
  explicit-assignment worker under its literal claim, with no phantom count
  on the shard it happened to be read from; a malformed assignment
  populates no `by_shard` bucket at all.

`cargo test -p autumn-harvest-plugin --lib`, `cargo clippy --all-targets --
-D warnings`, and `cargo fmt --check` are clean. The two new integration
tests are compile-checked in this sandbox (no Docker) and run Docker-backed
in CI, matching repo convention.
