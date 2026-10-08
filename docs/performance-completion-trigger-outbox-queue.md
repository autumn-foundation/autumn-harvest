# The completion-trigger outbox relay's per-row `harvest_schedules` lookup

`enforce_completion_triggers_outbox` (`autumn-harvest/src/completion_trigger.rs`)
is the scanner that relays a batch of up to `OUTBOX_CLAIM_BATCH_LIMIT` (50)
cross-shard completion-trigger targets on every poll tick (`timeout.rs`'s
periodic sweep calls it). For each task whose `queue_name` column is `NULL`,
the scan called `resolve_target_queue` on that task's own turn through the
loop: one `SELECT queue_name FROM harvest_schedules WHERE workflow_name = $1`
against the default shard, plus -- for a non-default target shard -- its own
connection checkout from the default shard's pool.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (below). Reproduce it on your own
> hardware before designing against it.

## TL;DR

* **`queue_name` is `NULL` on the common path.** `CompletionTrigger::new`
  defaults it to `None`; a caller must explicitly call
  `.with_queue_name(...)` to override it. A fan-in deployment -- many source
  executions whose completions all route through the same downstream
  trigger -- fills a claim batch with rows that share a `target_shard` and,
  commonly, a `target_workflow_name` too.
* **The fix adds `resolve_target_queues_batch`**: one round trip resolving
  every distinct `target_workflow_name` needing a lookup in the batch, via
  `workflow_name = ANY($1)` against a single connection checked out from the
  default shard's pool. The per-row `resolve_target_queue` path stays as the
  fallback for any name the batch attempt could not resolve (no
  default-shard pool, or the batch query itself failing), so a degenerate
  case behaves exactly as it did before this change.
* **`harvest_schedules_workflow_name_unique`** (issue #91's migration) means
  the batched query returns at most one row per name -- the same cardinality
  `resolve_target_queue`'s own `.first()` relies on -- so the per-name answer
  is unchanged; only the round-trip count moves.
* **Profile**: at the n=50 headline scenario, the `harvest_schedules` lookup
  is 16.3% of the scan tick's own statement count and 20.5% of its buffers --
  over the 5%/5% floor, and the third-largest cost after the two genuinely
  inherent per-row costs (the `FOR UPDATE SKIP LOCKED` claim and the outbox
  `DELETE`, ~38.5% each -- one claim and one delete per relayed row, which no
  batching removes).
* **`lookup_calls` goes from `n` to exactly `1`** at every swept size (5, 20,
  50) -- the textbook O(n) → O(1) statement-count shape. This alone clears
  the impact floor ("elimination of an N+1" needs no other justification).
* **No new index, no schema change, no migration.**
* **Result-equivalence is exact**:
  `outbox_scan_resolves_the_same_queue_with_or_without_batching` drives a
  real scan over both a schedule-matched row and a schedule-less row and
  reads back the queue each started on.

## Reference environment

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest --features db,testing --test integration -- \
  --ignored --exact \
  completion_trigger_outbox_queue_perf::zz_capture_completion_trigger_outbox_queue_perf_evidence \
  --nocapture
```

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16.13 (Ubuntu), default `shared_buffers`, `pg_stat_statements` preloaded |
| Harness | `autumn-harvest/tests/integration/completion_trigger_outbox_queue_perf.rs` |

`HARVEST_TEST_DATABASE_URL` is an admin URL: two fresh, uniquely-named
databases (one standing in for the default shard, one for the cross-shard
target) are created and migrated per measurement point, matching the
convention in `activity_enqueue_batch_perf.rs`. With the variable unset the
harness falls back to a `postgres:16` testcontainer.

## Workload

The fixture, seeded per measurement point:

* **`harvest_schedules`**: 300 unrelated DAG-kind rows (realistic
  cardinality for an operator-configured table, not a toy size) plus 3
  workflow-kind rows carrying a `queue_name` override.
* **The outbox batch**: `n` rows (5, 20, 50 -- the last is the scanner's own
  claim cap, `OUTBOX_CLAIM_BATCH_LIMIT`), every one with `queue_name = NULL`
  and `next_attempt_at = NULL` (fresh tier), `target_shard = 1` (the outbox
  exists only for cross-shard relay), cycling through 5 distinct
  `target_workflow_name`s -- 3 of which match a `harvest_schedules` row
  (exercising the match branch), 2 of which do not (exercising the
  `default_workflow_queue()` fallback branch). This fan-in skew -- many rows,
  few distinct names -- is what a real "many completions route through one
  downstream trigger" deployment produces.

`enforce_completion_triggers_outbox` is driven directly: the real public
entry point under test, exactly as `timeout.rs`'s periodic sweep calls it.

## Profile

Top statements for the n=50 headline point, `pg_stat_statements` reset
immediately before the one measured call. Full listing:
[`docs/perf-artifacts/completion-trigger-outbox-queue/before-profile-n50.txt`](perf-artifacts/completion-trigger-outbox-queue/before-profile-n50.txt).

| Statement | calls | % of calls | buffers | % of buffers |
|:--|--:|--:|--:|--:|
| `SELECT id FROM harvest_completion_trigger_outbox ... FOR UPDATE SKIP LOCKED` (per-row claim) | 50 | 16.3% | 150 | 38.5% |
| `DELETE FROM harvest_completion_trigger_outbox` (per-row, on success) | 50 | 16.3% | 150 | 38.5% |
| `SELECT queue_name FROM harvest_schedules WHERE workflow_name = $1` (**this fix's target**) | 50 | 16.3% | 80 | 20.5% |
| `BEGIN` / `SELECT $1` / everything else | ~150 | ~50% | ~10 | ~2.5% |

The claim and the delete are genuinely inherent: relaying `n` rows costs `n`
claims and `n` deletes no matter what, and this investigation does not touch
either. The `harvest_schedules` lookup is the largest cost this fix *can*
remove, comfortably over the 5%/5% floor on both dimensions within the scan
tick's own workload.

## The mechanism

The join key isn't the issue -- `harvest_schedules_workflow_name_unique`
already backs an index scan (see [Plan](#plan) below) -- the issue is that
the scan issues the query **once per row** instead of once per tick:

```rust
// Before: one resolve_target_queue(...) call per task in the loop.
let queue_name = if let Some(ref q) = task.queue_name {
    q.clone()
} else {
    resolve_target_queue(&mut target_conn, &task.target_workflow_name, target_shard).await
};
```

## The fix

```rust
// After: one batched resolve before the loop, consulted per task.
let resolved_queues = if !names_needing_lookup.is_empty()
    && let Some(sp) = sharded_pool.as_ref()
    && let Ok(mut default_conn) = sp.pool_for(sp.default_shard()).get().await
{
    resolve_target_queues_batch(&mut default_conn, &names_needing_lookup).await
} else {
    std::collections::HashMap::new()
};

// ...in the loop:
let queue_name = if let Some(ref q) = task.queue_name {
    q.clone()
} else if let Some(q) = resolved_queues.get(&task.target_workflow_name) {
    q.clone()
} else {
    resolve_target_queue(&mut target_conn, &task.target_workflow_name, target_shard).await
};
```

`resolve_target_queues_batch` issues one
`workflow_name = ANY($1)` query over every distinct name in the batch
needing a lookup, then backfills any name absent from the result (no
matching schedule row) with `default_workflow_queue()` -- the identical
answer `resolve_target_queue` reaches for that case via its own
`.optional()` returning `Ok(None)`.

## Plan

Before (per-row), `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)`:

```
Limit  (cost=0.15..8.17 rows=1 width=27) (actual rows=1 loops=1)
  Buffers: shared hit=2
  ->  Index Scan using harvest_schedules_workflow_name_unique on harvest_schedules
        Index Cond: (workflow_name = '...')
        Buffers: shared hit=2
```

After (batched, same fixture, 5 names -- 3 matched, 2 not):

```
Seq Scan on harvest_schedules  (cost=0.00..11.95 rows=5 width=64) (actual rows=3 loops=1)
  Filter: (workflow_name = ANY ('{...5 names...}'::text[]))
  Rows Removed by Filter: 300
  Buffers: shared hit=7
```

The planner chooses a Seq Scan over 5 index probes at this table size (303
rows) -- expected and fine: `harvest_schedules` is operator-configured
(cron/workflow schedules), not a per-execution table, so hundreds of rows is
already a realistic upper end, and either plan shape costs the same 7
buffers regardless of `n`. **This fix's claim is about call count, not
about which plan the lookup itself takes** -- see
[Measurement](#measurement). Full output:
[`before-explain-per-row-lookup.txt`](perf-artifacts/completion-trigger-outbox-queue/before-explain-per-row-lookup.txt),
[`after-explain-batched-lookup.txt`](perf-artifacts/completion-trigger-outbox-queue/after-explain-batched-lookup.txt).

## Measurement

`pg_stat_statements` reset immediately before each measured call, one fresh
pair of uniquely-named, fully-migrated databases per (`n`, label) point.
Full artifacts under
[`docs/perf-artifacts/completion-trigger-outbox-queue/`](perf-artifacts/completion-trigger-outbox-queue/).

`lookup_calls`/`lookup_buffers` isolate the `harvest_schedules` lookup
statement shape (matches both the per-row and the batched query text) -- the
node this fix changes. `total_calls`/`total_buffers` cover everything
`enforce_completion_triggers_outbox` issued for the scan. `wal_bytes` is
`pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')` read immediately before and
after the call, differenced -- reported per the "WAL bytes required for any
write-path claim" rule, though this is a read-path fix and the numbers are
noisy run to run (autovacuum/checkpointer share the same WAL stream); no
write-path claim is made here.

| n | lookup_calls (before → after) | lookup_buffers (before → after) | total_calls (before → after) | total_buffers (before → after) |
|--:|:--|:--|:--|:--|
| 5 | 5 → **1** | 8 → 7 | 36 → 28 | 46 → 45 |
| 20 | 20 → **1** | 32 → 7 | 126 → 88 | 160 → 135 |
| 50 | 50 → **1** | 80 → 7 | 306 → 208 | 390 → 317 |

| | before | after | Δ |
|:--|--:|--:|:--|
| `lookup_calls` @ n=5 | 5 | 1 | **-80%** |
| `lookup_calls` @ n=20 | 20 | 1 | **-95%** |
| `lookup_calls` @ n=50 | 50 | 1 | **-98%** |
| `lookup_buffers` @ n=50 | 80 | 7 | **-91.25%** |
| `total_calls` @ n=50 | 306 | 208 | **-32.0%** |
| `total_buffers` @ n=50 | 390 | 317 | **-18.7%** |

**`lookup_calls` goes from `n` to exactly `1` at every swept size** -- the
O(n) → O(1) statement-count shape, demonstrated at three input sizes as the
impact floor asks for a plan/shape claim. This alone clears the impact floor.

**`lookup_buffers` is flat at 7 regardless of `n`** after the fix, since the
batched query's cost depends on `harvest_schedules`'s own cardinality, not
on how many outbox rows are asking -- the same reasoning
`activity-fanout-enqueue` gives for its own flat post-fix number, just on
the read side here instead of the write side.

**`total_calls`/`total_buffers` also drop** (-32.0%/-18.7% at n=50), a
secondary effect of the same fix rolled up against the whole scan tick's
cost, not an independent claim -- the claim and delete statements this fix
does not touch still dominate both totals.

Tool: `pg_stat_statements` (`calls`, `shared_blks_hit + shared_blks_read`)
and `pg_wal_lsn_diff`, captured via `pg_stat_statements_reset(0, dbid, 0)`
immediately before each measured call.

## Equivalence

`outbox_scan_resolves_the_same_queue_with_or_without_batching` seeds one
outbox row targeting a workflow with a matching `harvest_schedules` row and
one targeting a workflow with none, runs one real
`enforce_completion_triggers_outbox` scan, and reads back the `queue_name`
each started execution actually landed on: the schedule-matched row must
start on that schedule's queue, the schedule-less row must start on
`default_workflow_queue()` ("default"). Both assertions passed identically
before and after the fix was applied (the harness's evidence-capture run
also confirms `processed == n` at every swept size on both sides -- every
row relays successfully either way).

`outbox_scan_on_no_pending_rows_is_a_no_op` covers the empty-batch edge
case: no outbox rows, `enforce_completion_triggers_outbox` returns `Ok(0)`.

Isolation/visibility: unchanged. The batched lookup runs on its own
connection from the default shard's pool, exactly as the per-row lookup
already did for a non-default target shard (see `resolve_target_queue`'s
own cross-shard branch) -- no transaction boundary moved.

## Write cost

No new index, no schema change, no migration. The batched query is a plain
`SELECT`; nothing about this fix changes what gets written or when.

## A note on the fallback path

The per-row `resolve_target_queue` call stays in the code, unconditionally,
as the fallback for any name the batch could not resolve -- deliberately,
not as leftover dead code. Two cases reach it: `sharded_pool` has no default
shard pool configured (should not happen once `enforce_completion_triggers_outbox`
is running at all, since a `None` `sharded_pool` already fails every row's
target-pool lookup before reaching the queue-name resolution), or the batch
query itself errors. Both are already-degenerate states this scan handles
elsewhere by backing off and continuing; falling back to the exact
pre-existing per-row behavior for just the affected names is strictly safer
than propagating a batch failure into a scan-wide error.

## Reproduce

```bash
# Equivalence tests (fast, always-run):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p autumn-harvest --features db,testing --test integration -- \
  outbox_scan_resolves_the_same_queue_with_or_without_batching \
  outbox_scan_on_no_pending_rows_is_a_no_op

# Full evidence capture (sweeps n=5/20/50; a few seconds per point):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  PERF_LABEL=after \
  cargo test -p autumn-harvest --features db,testing --test integration -- \
  --ignored --exact \
  completion_trigger_outbox_queue_perf::zz_capture_completion_trigger_outbox_queue_perf_evidence \
  --nocapture
```

Reproducing the **before** label needs the pre-fix
`enforce_completion_triggers_outbox` (the `resolve_target_queues_batch`
call site did not exist yet): check out this PR's first "Ledger RED" commit
(harness present, source unchanged) and run the same command with
`PERF_LABEL=before`. Unlike `activity-fanout-enqueue`, the two code paths
here are not both present as separate public functions on the same
commit -- the fix is a direct change to `enforce_completion_triggers_outbox`
-- so reproducing "before" means checking out that earlier commit, not
picking a different label at the same commit.

## See also

* `autumn-harvest/src/completion_trigger.rs` -- `resolve_target_queue`,
  `resolve_target_queues_batch`, `enforce_completion_triggers_outbox`.
* `autumn-harvest/tests/integration/completion_trigger_outbox_queue_perf.rs`
  -- the harness and evidence-capture test.
* `docs/perf-artifacts/completion-trigger-outbox-queue/` -- committed
  before/after `pg_stat_statements` sweeps, profile dumps, and `EXPLAIN`
  output.
* `docs/completion-triggers.md` -- completion-trigger feature reference.
