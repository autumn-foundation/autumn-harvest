# The workflow-start outbox relay's per-row delivery-mark round trip

`drain_workflow_start_outbox_batch` (`autumn-harvest-plugin/src/outbox.rs`) is
the core of the workflow-start outbox relay: the periodic drain that turns a
durably-persisted `harvest_workflow_outbox` row into a real Harvest workflow
start (`spawn_workflow_start_outbox_relay` ticks it on `poll_interval_ms`,
and `POST /admin/outbox/flush`-style manual drains reuse the same batch
loop). It claims one batch of due rows (`batch_size`, default 32) and, for
each row, dispatches the workflow start, then records the outcome.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (below). Reproduce it on your own
> hardware before designing against it.

## TL;DR

* **Dispatch cannot batch.** Each row starts a distinct workflow execution
  via `start_or_load_workflow_execution_with_metrics_and_codecs` -- real,
  per-row business logic, not a query shape to fold together.
* **The mark that follows dispatch can.** It was a single fixed-shape
  `UPDATE harvest_workflow_outbox SET ... WHERE id = $1 AND claimed_by =
  $2`, issued once per row instead of once per batch -- differing only in
  its bound values (`delivered_execution_id`, or `last_error` +
  `next_attempt_at`).
* **Profile**: at a 50-row batch drain, the per-row mark is 90.9% of the
  drain's own statement calls and 46.0% of its buffers -- both comfortably
  over the 5%/5% floor.
* **The fix**: one `UPDATE ... FROM UNNEST($1::bigint[], ...)` call per
  outcome (delivered, failed) per drain, instead of one call per row.
  `UNNEST` keeps the statement text fixed regardless of batch size, so this
  stays one prepared-statement shape.
* **Mark-step statement count goes from `n` to `ceil(n / 8)` per claim
  round** (`OUTBOX_MARK_FLUSH_EVERY`), not exactly one call per round --
  see the Fix section's correction 3 for why a single end-of-batch flush
  regressed claim-release latency and how chunking bounds it. Still the
  textbook O(n) → O(n/k) shape for a fixed k=8, measured at three input
  sizes: 1, 3, and 7 mark calls at n=5, 20, 50
  ([`after-sweep-chunked.txt`](perf-artifacts/outbox-start-relay/after-sweep-chunked.txt)).
  This alone clears the impact floor.
* **The mark statement's own buffers also drop 28.2%** at n=50 (568 →
  408), independently clearing the "≥20% reduction in buffers for a
  statement that is ≥5% of the workload" floor too.
* **No new index, no schema change, no migration.** No write-cost section
  beyond the statement-count/WAL numbers already reported.
* **The exactly-once "outbox" admission-bypass gate (issue #618) is
  preserved exactly**: it now checks membership in the batched `RETURNING`
  set instead of a per-row affected-row count -- same meaning, same
  concurrent-reclaim guarantee. Covered by both new unit tests and the
  pre-existing `outbox_bypass_counted_exactly_once_across_reflush` /
  `outbox_relay_is_exempt_and_counts_the_bypass` integration tests, which
  pass unchanged.

## Reference environment

```bash
HARVEST_TEST_DATABASE_URL=postgres://harvest_test:harvest_test@localhost:5432/harvest_test \
  cargo test -p autumn-harvest-plugin --test outbox_start_relay_perf -- \
  --ignored --nocapture \
  zz_capture_outbox_start_relay_perf_evidence
```

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16 (Ubuntu package), default `shared_buffers`, `pg_stat_statements` preloaded |
| Harness | `autumn-harvest-plugin/tests/outbox_start_relay_perf.rs` |

`HARVEST_TEST_DATABASE_URL` is an admin URL: two fresh, uniquely-named
databases (one standing in for the app database, one for the Harvest
database) are created and migrated per measurement point, mirroring the
relay's real split-database deployment (`outbox_integration.rs`'s
`setup_split_test_databases` convention, adapted to a local admin
connection instead of a `postgres:16` testcontainer).

## Workload

Every measurement point enqueues `n` real `WorkflowStartRequest`s via the
public `enqueue_workflow_start_outbox`, each with a distinct `workflow_id`
so every row dispatches as a genuinely new execution, then drives the real
public entry point under test: `flush_workflow_start_outbox`, exactly as
`spawn_workflow_start_outbox_relay`'s periodic tick calls it. No
`HandlerRegistry` is installed, matching the outbox relay's own
"exempt-by-design" posture (issue #618): dispatch succeeds purely off
`start_or_load_workflow_execution_with_metrics_and_codecs`.

## Profile

Top statements for the n=50 point (two claim rounds, since `batch_size`
defaults to 32), `pg_stat_statements` reset immediately before the measured
call. Full listings:
[`baseline-sweep.txt`](perf-artifacts/outbox-start-relay/baseline-sweep.txt),
[`after-sweep.txt`](perf-artifacts/outbox-start-relay/after-sweep.txt).

| Statement | calls | % of calls | buffers | % of buffers |
|:--|--:|--:|--:|--:|
| Per-row mark `UPDATE` (**baseline, this fix's target**) | 50 | 90.9% | 568 | 46.0% |
| Claim `WITH due AS (...) UPDATE ... RETURNING` | 2 | 3.6% | 668 | 54.0% |
| `SELECT $1` / `SET TIME ZONE` / `SET CLIENT_ENCODING` | 3 | 5.5% | 0 | 0.0% |

The claim statement is genuinely inherent -- relaying a batch costs one
claim round per `batch_size` rows no matter what, and this investigation
does not touch it. The per-row mark is the largest cost this fix *can*
remove, and it is the majority of the drain's own statement count.

## Mechanism

```rust
// Before: one mark_outbox_row_delivered/_failed call per row, interleaved
// with dispatch inside the loop.
for row in rows {
    match dispatch_workflow_start_request(state, &row.request()).await {
        Ok(exec_id) => {
            let marked = mark_outbox_row_delivered(&mut app_conn, row.id, &claimant, exec_id).await?;
            // ... gate the bypass counter on `marked == 1` ...
        }
        Err(error) => {
            mark_outbox_row_failed(&mut app_conn, &row, &claimant, &config, &error.to_string()).await?;
        }
    }
}
```

## Fix

```rust
// After: dispatch still runs per row (it cannot batch), but the outcomes
// are collected and marked in chunked batched UPDATEs, one per outcome
// per chunk, instead of once per row.
for row in rows {
    match dispatch_workflow_start_request(state, &row.request()).await {
        Ok(exec_id) => delivered_marks.push((row.id, exec_id)),
        Err(error) => {
            let deadline = Instant::now() + Duration::from_millis(retry_delay_ms(&config, &row));
            failed_marks.push((row.id, error.to_string(), deadline));
        }
    }
    if delivered_marks.len() + failed_marks.len() >= OUTBOX_MARK_FLUSH_EVERY {
        delivered += flush_outbox_marks(&mut app_conn, &claimant, &mut delivered_marks, &mut failed_marks, outbox_metrics.as_ref()).await?;
    }
}
delivered += flush_outbox_marks(&mut app_conn, &claimant, &mut delivered_marks, &mut failed_marks, outbox_metrics.as_ref()).await?;
```

`mark_outbox_rows_delivered_batch` and `mark_outbox_rows_failed_batch`, both
called from `flush_outbox_marks`, bind one array per column and join via
`FROM UNNEST($1::bigint[], $2::text[], ...) AS v(id, ...)`, instead of a
literal `VALUES (...), (...), ...` list whose text would grow a distinct
shape per batch size.

Three review-round corrections (Codex, on the PR) landed after the numbers
above were captured. The first two do not change statement count or
buffers; the third does, at batch sizes above `OUTBOX_MARK_FLUSH_EVERY` --
see below.

1. The admission-bypass metric is recorded right after the delivered
   batch mark commits, not after both marks have run. The two marks are
   separate statements, not one transaction; recording only after both
   would have let a failure in the failed-row mark permanently drop the
   count for rows the delivered mark had already durably committed
   (`delivered_at` no longer `NULL`, so no later flush retries them).
2. `mark_outbox_rows_failed_batch` takes a per-row deadline (a monotonic
   `Instant`, captured when that row's own dispatch failed), not a raw
   delay applied when the batched mark finally runs. Without this, a row
   that failed early in a batch whose later dispatches ran slowly would
   get a retry deadline skewed later by however long the rest of the
   batch took, instead of its own configured backoff.
3. The marks flush every `OUTBOX_MARK_FLUSH_EVERY` (8) dispatch outcomes,
   not only once after the whole batch has dispatched. Deferring every
   mark to the end of the batch meant a row's claim -- unreachable by any
   relay's reclaim, and for a failed row, unreachable for its own retry --
   stayed held until the slowest dispatch in the *entire* batch finished,
   not just its own. That regressed the pre-batching behavior, where each
   row released immediately after its own dispatch. Chunking bounds the
   wait to a handful of dispatches, at the cost of more mark-step calls
   per claim round once a round exceeds 8 rows. The `baseline-sweep.txt`
   / `after-sweep.txt` captures above predate this change and no longer
   match the checked-out code at n=20 and n=50; re-measured post-chunking
   numbers are in
   [`after-sweep-chunked.txt`](perf-artifacts/outbox-start-relay/after-sweep-chunked.txt):
   mark calls go from 1/1/2 (n=5/20/50, single end-of-batch flush) to
   1/3/7 (chunked). n=5 is unaffected -- its one 5-row round never
   reaches the 8-outcome threshold. Both floor criteria from the
   single-flush measurement still clear at every swept size; see that
   file for the recomputed deltas.

Flushing every chunk, rather than waiting for the whole batch, is safe
under the same idempotency the relay already relies on. A crash between
dispatch and a chunk's mark leaves that chunk's rows reclaimable past
`claim_ttl_ms`. `dispatch_workflow_start_request`'s `start_or_load` path
already returns the same existing execution on that retry -- its own doc
comment already names this exact recovery path for a single failed mark.
Batching only widens how many rows in one chunk share that same recovery
path on a crash mid-chunk; it does not introduce a new failure mode.

## Plan

Both before and after, the mark uses an Index Scan on the primary key --
the plan shape is unchanged, only the call count moves. At production
scale (500k background rows plus a live 32-row claimed batch, `ANALYZE`d),
the batched form correctly uses a Nested Loop + Memoize + Index Scan
against the same `harvest_workflow_outbox_pkey`, not a sequential scan:

```
Update on harvest_workflow_outbox outbox  (actual rows=0 loops=1)
  Buffers: shared hit=327 dirtied=1 written=1
  ->  Nested Loop  (actual rows=32 loops=1)
        ->  Function Scan on v  (actual rows=32 loops=1)
        ->  Memoize  (actual rows=1 loops=32)
              ->  Index Scan using harvest_workflow_outbox_pkey on outbox
                    Index Cond: (outbox.id = v.id)
                    Filter: (outbox.claimed_by = 'explain-claimant'::text)
```

Full output, plus the single-row baseline at the same table size (17
buffers/row) and a small-table (50-row) capture of both shapes:
[`before-explain-single-row-mark-500k-table.txt`](perf-artifacts/outbox-start-relay/before-explain-single-row-mark-500k-table.txt),
[`after-explain-batched-mark-n32-500k-table.txt`](perf-artifacts/outbox-start-relay/after-explain-batched-mark-n32-500k-table.txt),
[`before-explain-single-row-mark.txt`](perf-artifacts/outbox-start-relay/before-explain-single-row-mark.txt),
[`after-explain-batched-mark-n49.txt`](perf-artifacts/outbox-start-relay/after-explain-batched-mark-n49.txt).
32 rows batched costs 327 buffers (~10.2/row) against the 500k-row table,
essentially the same per-row cost as the single-row form (17 buffers/row,
measured separately since the first EXPLAIN's own side effects consumed
the fixture) -- confirming the fix removes round trips, not per-row I/O.

## Measurement

`pg_stat_statements` reset immediately before each measured call, one
fresh pair of uniquely-named, fully-migrated databases per (`n`) point.
Full artifacts under
[`docs/perf-artifacts/outbox-start-relay/`](perf-artifacts/outbox-start-relay/).

| n | mark_calls (before → after) | mark_buffers (before → after) | total_calls (before → after) | total_buffers (before → after) |
|--:|:--|:--|:--|:--|
| 5 | 5 → **1** | n/a → 41 | 8 → 4 | 117 → 103 |
| 20 | 20 → **1** | n/a → 165 | 23 → 4 | 451 → 394 |
| 50 | 50 → **2** | 568 → 415 | 55 → 7 | 1236 → 1078 |

(`mark_buffers` before n=5/n=20 is omitted: the harness's statement
classifier, fixed after the baseline capture, double-counted the claim
statement into that specific sub-total at those two sizes in the original
run. `total_calls`/`total_buffers` are unaffected -- they sum every
statement regardless of classification -- and the n=50 point's breakdown
is independently confirmed against the distinct top-statement listing.)

| | before | after | Δ |
|:--|--:|--:|:--|
| `mark_calls` @ n=50 | 50 | 2 | **-96.0%** |
| `mark_buffers` @ n=50 | 568 | 415 | **-26.9%** |
| `total_calls` @ n=5 | 8 | 4 | **-50.0%** |
| `total_calls` @ n=20 | 23 | 4 | **-82.6%** |
| `total_calls` @ n=50 | 55 | 7 | **-87.3%** |
| `total_buffers` @ n=50 | 1236 | 1078 | **-12.8%** |

**`mark_calls` goes from `n` to the claim-round count at every swept
size** -- the O(n) → O(1) statement-count shape, demonstrated at three
input sizes. This alone clears the impact floor. **`mark_buffers` also
drops 26.9%** at n=50, independently clearing the 20%-buffers floor for a
statement that was 46.0% of the workload's buffers.

Tool: `pg_stat_statements` (`calls`, `shared_blks_hit + shared_blks_read`),
captured via `pg_stat_statements_reset(0, dbid, 0)` immediately before each
measured call.

**This table predates the Fix section's correction 3** (chunked
flushing, `OUTBOX_MARK_FLUSH_EVERY=8`) and no longer matches the
checked-out code's `mark_calls` at n=20 and n=50 -- both floor criteria
still clear post-chunking, but at different numbers. See
[`after-sweep-chunked.txt`](perf-artifacts/outbox-start-relay/after-sweep-chunked.txt)
for the re-measurement.

## Equivalence

Result-set identity: neither mark statement changes which rows get
delivered or which get retried, only how many round trips that costs. Two
new unit tests drive the batched functions directly against a real
Postgres and read back every affected column:

* `mark_outbox_rows_delivered_batch_reports_affected_ids_and_keeps_rows_distinct`
  seeds two claimed rows, one owned by the claimant under test and one
  claimed by a concurrent worker, marks both in ONE batched call, and
  asserts: only the owned row's id comes back in the affected set; the
  owned row gets its OWN `exec_id`, not the other row's; the row the
  claimant does not own is left completely untouched, claim and all.
* `mark_outbox_rows_failed_batch_keeps_rows_distinct` seeds two owned rows
  with different retry delays in ONE batched call and asserts each keeps
  its own `last_error` and that the row given the longer delay retries
  later than the one given the shorter delay -- proving the `UNNEST`
  row-pairing does not swap values between rows.

The pre-existing integration coverage passes unchanged against the fix:
`outbox_relay_is_exempt_and_counts_the_bypass` and
`outbox_bypass_counted_exactly_once_across_reflush`
(`admission_gate_authoritative_localpg.rs`) exercise the exactly-once
admission-bypass gate this fix rewired from a per-row affected-row count to
a per-row `RETURNING`-set membership check, and both still pass. The
`outbox_integration.rs` suite (delivery, retry-on-failure, full-failed-batch
draining, timezone-independent retry delay) is untouched by this change and
covers the same public entry points.

Edge cases: an empty claimed batch returns early from both batched mark
functions without issuing a statement (`rows.is_empty()` guard), same as
the old per-row loop issuing zero marks for zero rows. A batch that is all
delivered, all failed, or a genuine mix all resolve correctly, since the
two batched calls are independent and each is a no-op when its own list is
empty.

Isolation/visibility: unchanged. Neither the old nor the new code wraps the
claim-dispatch-mark sequence in an explicit transaction; each statement
still auto-commits on its own connection, exactly as before.

## Write cost

No new index, no schema change, no migration. The fix changes only how
many `UPDATE` statements the drain issues against the existing
`harvest_workflow_outbox` table and its existing primary key -- fewer
statements, not a new write path.

## Reproduce

```bash
# Baseline vs after: check out the commit before/after this fix and run:
HARVEST_TEST_DATABASE_URL=postgres://harvest_test:harvest_test@localhost:5432/harvest_test \
  cargo test -p autumn-harvest-plugin --test outbox_start_relay_perf -- \
  --ignored --nocapture zz_capture_outbox_start_relay_perf_evidence

# Unit tests and the localpg admission-gate suite (need both the outbox app
# migration and the full harvest core migration bundle applied to
# HARVEST_TEST_DATABASE_URL directly, not just an admin role -- these connect
# to it without creating a fresh database):
psql "$HARVEST_TEST_DATABASE_URL" \
  -f autumn-harvest-plugin/migrations/app/20260409010000_harvest_workflow_outbox/up.sql
for d in autumn-harvest/migrations/*/; do psql "$HARVEST_TEST_DATABASE_URL" -f "${d}up.sql"; done
cargo test -p autumn-harvest-plugin --lib outbox::
cargo test -p autumn-harvest-plugin --test admission_gate_authoritative_localpg
```
