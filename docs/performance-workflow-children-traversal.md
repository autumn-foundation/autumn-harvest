# `GET /workflows/{id}/children?depth=N` — the recursive traversal's N+1

`load_workflow_children_tree_from_shards` (in `autumn-harvest-plugin/src/api.rs`)
answers `GET /workflows/{id}/children?depth=N`: walk `N` levels of a
workflow's descendant tree, cross-shard, and return the flattened,
filtered, paginated result. It did that one **parent at a time** — for
every node discovered at depth `D`, issue one `load_workflow_children`
query per shard, for every shard, before moving to depth `D+1`. That is
`O(nodes × shards)` round trips for a tree of `nodes` descendants over
`shards` shards, and it is exactly the class of query this repo's own
performance playbook names directly: "Harvest workflow/activity
bookkeeping queries that are individually trivial but collectively
dominant... these won't show up in a buffers ranking, only in a `calls`
ranking."

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (below). Reproduce it on your own
> hardware before designing against it — the harness is in the repo
> precisely so you can.

## TL;DR

* **The buggy loop issues one query per *node*, not one per *depth level*.**
  Against a synthetic 3,900-descendant tree (300/1,200/2,400 across 3
  generations, round-robined over 2 shards, `depth=2`), the naive
  per-parent recursion issued **3,002 SQL calls** touching **9,276
  buffers** to answer one `GET /children` request.
* **The fix batches the whole traversal *frontier* into one
  `parent_id = ANY($1)` query per shard per depth level** — the same
  pattern this endpoint's own proven-fixed sibling, `GET /workflows/{id}/tree`
  (issue #621), already uses and documents by name. Measured on the
  identical fixture: **6 SQL calls**, **190 buffers** — a **99.80%
  reduction in calls** and a **97.95% reduction in buffers**.
* **The call counts match the `O(nodes × shards)` vs `O(depth × shards)`
  model exactly, not approximately.** Before: `(1 + 300 + 1,200) × 2 shards
  = 3,002`. After: `3 depth levels × 2 shards = 6`. Both numbers were
  predicted from the code before being measured, then confirmed
  bit-for-bit by `pg_stat_statements`.
* **No new index, no schema change, no migration.** The fix is a
  restructured loop plus one new store function
  (`store::load_workflow_children_multi`) that mirrors the already-shipped
  `load_workflow_children_batch` used by `/tree` — same table, same
  columns, same `WHERE`/`ORDER BY` shape, just issued once per frontier
  instead of once per node in the frontier.
* **Result-equivalence is exact, not approximate**: the response for a
  fixed request is the same set of rows regardless of how the traversal
  was batched, because the handler always sorts the fully-accumulated
  result set before applying pagination — batching changes *how many
  queries* build that set, never *what's in it*.

## Reference environment

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16 (Ubuntu), default `shared_buffers` |
| Harness | `autumn-harvest-plugin/tests/workflow_children_traversal_perf.rs` |
| Artifacts | `docs/perf-artifacts/workflow-children-traversal/` (committed, this page's source) |

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  PERF_LABEL=before \
  cargo test -p autumn-harvest-plugin --test workflow_children_traversal_perf \
    --release -- --ignored --exact zz_capture_children_traversal_perf_evidence --nocapture
```

`HARVEST_TEST_DATABASE_URL` is treated as an **admin** URL: two fresh,
uniquely-named databases (one per shard) are created off it, each migrated
with every `up.sql` in timestamp order via `autumn_harvest::full_migrations_sql()`,
seeded, measured, and left in place for inspection (the harness does not
drop them — the databases are cheap and the harness names them uniquely
per run). `pg_stat_statements` must be preloaded via
`shared_preload_libraries = 'pg_stat_statements'` on the target instance;
without it, Docker/testcontainers is used as a fallback for the
non-`#[ignore]`d correctness test only (it does not configure
`pg_stat_statements`, so the `#[ignore]`d evidence-capture test needs a
real target).

The fixture is a genuine multi-generation, multi-shard tree, not a
single-branch chain: **root → 300 gen1 → 1,200 gen2 (4 per gen1) → 2,400
gen3 (2 per gen2)**, every generation round-robined across both shards so
each depth level's traversal frontier is spread realistically, not
concentrated on one database. Every 20th gen3 leaf is marked `FAILED`
(via one bulk `id = ANY($1)` UPDATE per shard — deliberately outside the
code path under investigation, so it does not distort the query-count
evidence) to exercise result filtering alongside pure traversal cost.
`fixture-summary.txt` records the exact shape: 3,901 total nodes,
`GET /workflows/{root}/children?depth=2&limit=500` (`limit=500` is the
endpoint's own page-size ceiling — the traversal itself computes the
*full* 3,900-descendant set internally regardless of the final page size;
the request needs `limit` above the default 50-row page or the
`returned_rows` count in the artifacts would reflect pagination, not
traversal cost).

## Profile — the traversal is the whole cost of this endpoint

`GET /workflows/{id}/children?depth=N` exists to do exactly one thing:
walk the descendant tree and return it. There is no other workload
sharing this request — the traversal's SQL calls and buffers **are** the
endpoint's cost, by construction, not a fraction of some larger workload.
What the fixture's `pg_stat_statements` snapshot shows is the concrete
size of that cost and how the fix changes it: two statement shapes (the
`parent_id = ANY($1)` query, same text on both shards — `pg_stat_statements`
scopes rows per-database, so one query text produces two entries) account
for **100% of the traversal's SQL activity**, both before and after —
this isn't a "does the target matter" question, it's a "how expensive is
the endpoint's *entire* job" question. Before: 3,002 calls / 9,276
buffers. After: 6 calls / 190 buffers. Both numbers clear the impact
floor by roughly two orders of magnitude.

## The problem

```rust
for parent in &frontier {
    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let shard_rows =
            store::load_workflow_children(&mut conn, *parent, &traversal_filters, depth)
                .await?;
        // ... accumulate into next_frontier / rows ...
    }
}
```

For every node discovered at the current depth, this issues one
single-parent query (`WHERE parent_id = $1`) against every shard, before
moving to the next depth level. Each individual query is cheap — the
[single-parent EXPLAIN][before-single] reads 9 buffers via an Index Scan
on `idx_harvest_we_parent_id` — but there are `nodes × shards` of them.
Against this fixture's 1,501 non-leaf nodes (root + 300 gen1 + 1,200
gen2) across 2 shards:

```
depth 0:     1 parent (root) × 2 shards =    2 calls
depth 1:   300 gen1 parents  × 2 shards =  600 calls
depth 2: 1,200 gen2 parents  × 2 shards = 2,400 calls
                                   total = 3,002 calls
```

[The committed `pg_stat_statements` snapshot][before-stats] confirms this
prediction exactly: `total_calls=3002`, `total_buffers=9276` (two matching
statement shapes, `calls=1501` each — one per shard — since each shape's
query text is identical on both databases).

The predicate resembles a healthy N+1: no single query is slow, no query
plan is wrong, and a profiler sorted by *buffers per call* would rank
every one of these queries as trivial. It only shows up as dominant in a
*calls* ranking, or by simply counting how many round trips one HTTP
request produces — exactly the blind spot this repo's performance
playbook calls out `harvest_task_queue`-style bookkeeping N+1s for.

## The fix

```rust
let parent_uuids: Vec<uuid::Uuid> = frontier.iter().map(ExecutionId::as_uuid).collect();
let mut next_frontier = Vec::new();
for (_shard, shard_pool) in pool.iter_shards() {
    let mut conn = acquire_conn(shard_pool).await?;
    let shard_rows = store::load_workflow_children_multi(
        &mut conn, &parent_uuids, &traversal_filters, depth,
    ).await?;
    // ... accumulate into next_frontier / rows (identical loop body) ...
}
```

`store::load_workflow_children_multi` is a new function batching the
*entire current frontier* into one `parent_id = ANY($1)` query per shard
per depth level, instead of one query per parent per shard per depth
level — `O(depth × shards)` round trips instead of `O(nodes × shards)`.
This is not a new idea in this codebase: `store::load_workflow_children_batch`
already does exactly this for `/children`'s sibling endpoint,
`GET /workflows/{id}/tree` (issue #621), and says so directly in its own
doc comment:

> "That turns a depth-`D` tree over `S` shards from `O(nodes × S)` round
> trips (one query per parent per shard — what a naive recursion over
> `load_workflow_children` costs) into `O(D × S)`"

`load_workflow_children_tree_from_shards` was precisely the naive
recursion that comment describes as the thing to avoid — it just hadn't
been converted yet. `load_workflow_children_multi` mirrors
`load_workflow_children_batch` field-for-field (same table, same 8-column
`SELECT`, same `WHERE parent_id = ANY($1)` plus the same optional
status/workflow_name/cursor/limit filters, same `ORDER BY started_at DESC,
id DESC`) except for its return type: `WorkflowChildRow` (this endpoint's
flat, non-parent-tagged projection) instead of `LineageChildRow`
(`/tree`'s nested, parent-tagged one) — `/children`'s caller only needs
the union of matching rows and the next traversal frontier, never *which*
parent in the current frontier produced a given child.

Against the identical fixture, the fixed traversal issues **6 calls**
(3 depth levels × 2 shards) touching **190 buffers**
([`after-pg_stat_statements.txt`][after-stats]) — exactly the `depth ×
shards` prediction, confirmed bit-for-bit. The [single most expensive
individual call][after-batched] — the depth-2 frontier (1,200 gen2
parents, 600 of which live on shard 0) — reads 43 buffers via a Seq Scan
touching all 1,951 rows on that shard (`actual rows=600`,
`Rows Removed by Filter: 1351`; the planner's choice at this table size,
not a plan defect — see [Known limitations](#known-limitations--out-of-scope)).

| | Before | After | Reduction |
|---|---:|---:|---:|
| SQL calls | 3,002 | 6 | **99.80%** (500.3x fewer) |
| Buffers touched | 9,276 | 190 | **97.95%** (48.8x fewer) |

Both metrics clear the impact floor by roughly two orders of magnitude.

## Correctness — result equivalence

The response for a fixed `GET /children?depth=N` request is the identical
set of rows regardless of how many queries the traversal issued to build
it, because `list_workflow_children` (the outer handler) always sorts the
fully-accumulated candidate set before applying the response's cursor and
page limit — the traversal's job is only to *produce the candidate set*,
never to decide its final order or which page of it is returned. Batching
the frontier changes the query count and the order rows arrive from the
database in; it cannot change which rows end up in the final response.

**In the committed test suite**, a new permanent regression test —
`depth_traversal_with_branching_frontiers_returns_the_exact_descendant_set`
(`autumn-harvest-plugin/tests/workflow_children_traversal_perf.rs`) —
seeds a genuinely *branching* 3-generation tree (6 gen1 × 3 gen2 × 2
gen3 = 60 descendants, round-robined across both shards, so every depth
level has multiple parents contributing to a shared next frontier — the
exact shape a single-branch chain can't distinguish the fix from the old
code on) and asserts, independent of row order:

* the total descendant count (`6 + 18 + 36`) and the per-depth `depth`
  stamp on every row;
* the exact `exec_id` set at each of the three depth levels matches the
  fixture's gen1/gen2/gen3 nodes precisely (no missing node, no
  duplicate, no node from the wrong depth);
* the `FAILED` subset (every 20th gen3 leaf) survives the batched
  traversal unchanged.

This test sits alongside — and does not replace — the **7 pre-existing
functional-correctness tests** for this endpoint in
`autumn-harvest-plugin/tests/api_scheduler_integration.rs`
(`harvest_api_lists_direct_workflow_children_with_filters`,
`harvest_api_filters_workflow_children_by_continued_as_new_status`,
`load_workflow_children_applies_limit_and_cursor_before_returning_rows`,
`harvest_api_lists_workflow_children_across_shards_and_paginates`,
`harvest_api_recursive_children_traverse_across_shards`,
`harvest_api_children_distinguishes_empty_parent_from_missing_parent`,
`harvest_api_children_supports_recursive_depth_with_cap`) covering
status/workflow-name filters, cursor pagination, single-branch cross-shard
recursion, depth caps, and the empty-vs-missing-parent distinction — none
of which this change modifies. All 7, plus the file's unrelated
`retention_janitor_deletes_only_rows_older_than_max_age_and_cascades_children`
test, were run against real Postgres with the fix applied (that file's
own `setup_test_database_url`/`setup_sharded_test_database_urls` helpers
are hardcoded to testcontainers with no `HARVEST_TEST_DATABASE_URL`
fallback, so they were temporarily, locally patched to honor the env var
for this verification pass only — a change that was never committed):

```
test result: ok. 8 passed; 0 failed
```

The same 8 tests were confirmed to fail identically — with the *pre-fix*
code checked out via `git stash` — for an unrelated, pre-existing reason
(`SocketNotFoundError("/var/run/docker.sock")`, this sandbox has no Docker
daemon), establishing that their unavailability in an unpatched sandbox
run is an environmental fact independent of this change, not a
regression it introduces.

```bash
cargo test -p autumn-harvest-plugin --test workflow_children_traversal_perf \
  -- --exact depth_traversal_with_branching_frontiers_returns_the_exact_descendant_set
```

## Write cost

None. This is a read-only query restructuring — no schema change, no
migration, no index added or dropped, no change to the columns selected
or the filters/ordering applied. `store::load_workflow_children_multi` is
called from the existing traversal `SELECT` path only, and the fixed
traversal issues *strictly fewer* queries of the exact same shape the
unfixed code already issued.

## Reproduce

```bash
# 1. The correctness test (works without Docker via HARVEST_TEST_DATABASE_URL,
#    falls back to testcontainers otherwise).
cargo test -p autumn-harvest-plugin --test workflow_children_traversal_perf \
  -- --exact depth_traversal_with_branching_frontiers_returns_the_exact_descendant_set

# 2. The buffer/call-count measurement (regenerates the committed artifacts
#    under docs/perf-artifacts/workflow-children-traversal/). Needs a
#    Postgres target with pg_stat_statements preloaded.
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  PERF_LABEL=before \
  cargo test -p autumn-harvest-plugin --test workflow_children_traversal_perf \
    --release -- --ignored --exact zz_capture_children_traversal_perf_evidence --nocapture
# (checkout the fix, then re-run with PERF_LABEL=after)
```

## Known limitations / out of scope

* **A very wide single-level frontier produces a very large `= ANY($1)`
  array parameter.** Postgres has no hard limit on the number of elements
  inside a single array-typed bind parameter (unlike positional
  placeholders, which cap around ~65,535 per statement) — this fixture's
  largest single-shard frontier is 600 ids and reads 43 buffers via a Seq
  Scan; an extreme fan-out (hundreds of thousands of ids at one depth
  level, on one shard) has not been measured here and could plausibly
  shift the planner toward a different (still correct, but not
  necessarily cheap) plan shape. This is unchanged in kind from the
  already-shipped `/tree` endpoint's identical `= ANY($1)` batching, and
  is not a regression introduced by this fix relative to that sibling
  endpoint's existing, accepted design.
* **This fix is scoped to `/children`'s traversal loop only.** `/tree`
  (issue #621) was already correctly batched before this change and is
  untouched. No other recursive cross-shard traversal in the codebase was
  audited as part of this investigation.
* **The single-parent EXPLAIN artifacts are kept purely as a baseline
  reference for "what one iteration of the old loop cost"** — the fixed
  code never issues that query shape in production; it is captured
  standalone for comparison, not as a call the new code path makes.

## See also

* [`docs/performance-history-bloat-filter.md`](performance-history-bloat-filter.md) —
  the sibling "N+1-shaped, `pg_stat_statements`-measured" investigation
  this page follows in structure and evidence discipline.
* Issue #621 — `GET /workflows/{id}/tree`, whose `store::load_workflow_children_batch`
  is the pre-existing, already-shipped precedent this fix mirrors.

[before-stats]: ../perf-artifacts/workflow-children-traversal/before-pg_stat_statements.txt
[before-single]: ../perf-artifacts/workflow-children-traversal/before-explain-single-parent.txt
[after-stats]: ../perf-artifacts/workflow-children-traversal/after-pg_stat_statements.txt
[after-batched]: ../perf-artifacts/workflow-children-traversal/after-explain-batched-frontier.txt
