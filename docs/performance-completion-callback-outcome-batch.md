# The completion-callback scanner's per-row outcome-write N+1

`fire_due_on_conn` (`autumn-harvest/src/completion_callback.rs`) is the tick
that `timeout.rs`'s periodic sweep drives via `fire_due_completion_deliveries`
to relay completion-callback HTTP deliveries. It claims up to
`COMPLETION_DELIVERY_FIRE_BATCH_SIZE` (100) due rows in one statement,
dispatches every claimed delivery's HTTP POST concurrently, then walked the
batch a second time and called `apply_outcome` -- one `UPDATE
harvest_completion_deliveries ... WHERE id = $id AND attempt = $attempt`
round trip -- on each row's own turn through the loop. A tick that resolves
N rows issued 1 batched claim plus N independent outcome-recording
statements. This is exactly the shape this persona's charter calls out
separately from request-driven costs: "Workflow/activity bookkeeping
queries (Harvest) that are individually trivial and collectively
dominant... find them by `calls`, not by the buffer ranking."

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (Postgres 16, default
> `shared_buffers`, `pg_stat_statements` preloaded). Reproduce it on your
> own hardware before designing against it.

## Workload

`completion_callback_outcome_batch_perf.rs` drives the real public entry
point, `fire_due_completion_deliveries`, against a fresh, uniquely-named,
fully-migrated database per measurement point (same pattern
`completion_trigger_outbox_queue_perf.rs` uses). The fixture per point:

* `n` due `harvest_completion_deliveries` rows (`PENDING`, `next_attempt_at`
  one second in the past), `n` in {10, 50, 100} -- the last is the scanner's
  own claim cap.
* A deterministic mock deliverer (`ParityDeliverer`) that returns a 2xx for
  even-indexed rows and a 500 for odd-indexed rows, derived from a numeric
  suffix baked into each row's `target_url` at seed time -- stable
  regardless of how the concurrent dispatch futures interleave. This
  produces a realistic mixed tick: roughly half `Delivered`, half
  `Backoff`, exercising both of the batched paths this fix adds.
* `retry_policy` configured with 5 max attempts, so every failing row is
  still under budget at attempt 1 and takes `Backoff`, never `DeadLetter`
  -- keeping the measured tick's writes concentrated in the two paths this
  investigation targets.

## Profile

Headline point (n=100), `pg_stat_statements` reset immediately before the
one measured call. Full listing:
[`docs/perf-artifacts/completion-callback-outcome-batch/baseline-profile-n100.txt`](perf-artifacts/completion-callback-outcome-batch/baseline-profile-n100.txt).

| Statement | calls | % of calls | buffers | % of buffers |
|:--|--:|--:|--:|--:|
| `UPDATE ... SET state = 'PENDING', next_attempt_at = ...` (per-row `Backoff`) | 50 | 47.6% | 716 | 27.3% |
| `UPDATE ... SET state = 'DELIVERED', ...` (per-row `Delivered`) | 50 | 47.6% | 654 | 25.0% |
| `WITH candidate AS (...) UPDATE ... RETURNING ...` (the claim) | 1 | 1.0% | 1229 | 46.9% |
| re-read claimed payloads (`SELECT id, payload ... WHERE id = ANY($1)`) | 1 | 1.0% | 16 | 0.6% |
| `SELECT to_regclass(...) IS NOT NULL` (table-existence probe) | 1 | 1.0% | 6 | 0.2% |
| `SELECT pg_wal_lsn_diff(...)` (harness instrumentation, not production) | 2 | 1.9% | 0 | 0.0% |

The two outcome-write statement shapes together are **95.2% of the tick's
statement calls and 52.3% of its buffers** -- both comfortably over the
5%/5% floor. The claim is genuinely inherent (one atomic `FOR UPDATE SKIP
LOCKED` statement per tick already, not touched by this fix); the outcome
writes are the target.

## The mechanism

```rust
// Before: one apply_outcome(...) call per row, for every action kind.
for ((row, _body, _headers), attempt_outcome) in dispatchable.into_iter().zip(attempt_outcomes) {
    let action = classify_outcome(&attempt_outcome, attempt, max_attempts, &retry_policy, seed, backoff_now);
    apply_outcome(conn, &row, action).await?;
    processed += 1;
}
```

`apply_outcome` issues its own `UPDATE ... WHERE id = $id AND attempt =
$attempt` for `Delivered` and for `Backoff` alike (`DeadLetter` additionally
wraps a `FOR UPDATE` re-read and a DLQ insert in a transaction). Nothing
about either statement's shape depends on the specific row -- only the bind
values differ -- so N rows resolving the same action kind cost N identical
round trips.

## The fix

```rust
// After: classify every row first, bucket by action kind, write once per bucket.
let mut delivered_rows: Vec<(Uuid, i32, u16)> = Vec::new();
let mut backoff_rows: Vec<BackoffOutcomeRow> = Vec::new();
let mut dead_letter_rows: Vec<(ClaimedDeliveryRow, OutcomeAction)> = Vec::new();

for (...) in ... {
    let action = classify_outcome(...);
    match action {
        OutcomeAction::Delivered { status } => delivered_rows.push((row.id, row.attempt, status)),
        OutcomeAction::Backoff { next_attempt_at, last_status, last_error } =>
            backoff_rows.push((row.id, row.attempt, next_attempt_at, last_status, last_error)),
        OutcomeAction::DeadLetter { .. } => dead_letter_rows.push((row, action)),
    }
    processed += 1;
}

apply_delivered_outcomes_batch(conn, &delivered_rows, backoff_now).await?;
apply_backoff_outcomes_batch(conn, &backoff_rows, backoff_now).await?;
for (row, action) in dead_letter_rows {
    apply_outcome(conn, &row, action).await?; // unchanged per-row path
}
```

`apply_delivered_outcomes_batch` and `apply_backoff_outcomes_batch` each
issue one `UPDATE ... FROM unnest($ids, $attempts, ...) AS v(...) WHERE
d.id = v.id AND d.attempt = v.attempt` -- a bulk update joined on exactly
the same `(id, attempt)` pair the per-row path filtered on via
`.find(row.id).filter(attempt.eq(row.attempt))`. A row whose `attempt` no
longer matches (superseded by a later reclaim) is silently excluded by the
join, the identical no-op the per-row `UPDATE` produced by matching zero
rows.

**`DeadLetter` is deliberately untouched.** Its per-row transaction (DR
fence-check + `FOR UPDATE` re-read of the current payload + DLQ insert,
documented on `dead_letter_entry_with_current_payload`) is the load-bearing
anti-PII-resurrection guarantee: it must see the *current* payload at write
time in case a concurrent `erase_workflow_payloads` tombstoned it after
claim. Collapsing that into a shared/batched transaction is a lock-ordering
and transaction-boundary change -- this agent's own operating rules flag
exactly that as "ask before," and it is out of scope for a statement-count
fix. `DeadLetter` is also the rare path (retry exhaustion), so leaving it
unbatched costs nothing in the common case this fix targets.

## Plan

At the harness's own 100-row-table fixture size, the batched `UPDATE` plans
as a `Seq Scan` + `Hash Join` against the `unnest` function scan -- the
planner correctly judges a full scan cheaper than ~100 index probes when
the *whole table* is only ~100 rows. That is a fixture-only artifact, not a
production plan: `docs/perf-artifacts/completion-callback-outcome-batch/`
carries both shapes at that scale for reference
([`baseline-explain-per-row-update.txt`](perf-artifacts/completion-callback-outcome-batch/baseline-explain-per-row-update.txt),
[`baseline-explain-batched-update.txt`](perf-artifacts/completion-callback-outcome-batch/baseline-explain-batched-update.txt)).

Re-run against a throwaway, production-shaped 500,100-row table (500,000
historical `DELIVERED` rows + the 100-row `INFLIGHT` batch under test, not
committed -- reproduce with the SQL in
[Reproduce](#reproduce)), the batched `UPDATE` instead plans as a
`Nested Loop` with an `Index Scan using harvest_completion_deliveries_pkey`
(`loops=100`) -- the same index the per-row path uses, at the same
cardinality. Buffers at that scale: 1730 for 100 sequential per-row
`UPDATE`s vs. 1820 for the one batched `UPDATE` covering the same 100 rows
-- within 5% of each other, not a regression on either admissible axis this
fix claims. The story here is statement count, not buffers-per-statement,
and the plan-shape check at realistic scale confirms the batched query
scales the same way the per-row query already did.

## Measurement

`pg_stat_statements` reset immediately before each measured call, one
fresh database per (`n`, label) point. Full artifacts under
[`docs/perf-artifacts/completion-callback-outcome-batch/`](perf-artifacts/completion-callback-outcome-batch/).
`outcome_write_calls`/`outcome_write_buffers` isolate the outcome-recording
`UPDATE` statement(s) -- an `UPDATE` against `harvest_completion_deliveries`
that is not the claim (excludes the claim by its `candidate` CTE, which no
outcome-write statement has on either side of the fix).
`total_calls`/`total_buffers` cover everything the tick issued.

| n | outcome_write_calls (before → after) | outcome_write_buffers (before → after) | total_calls (before → after) | total_buffers (before → after) |
|--:|:--|:--|:--|:--|
| 10 | 10 → **2** | 131 → 99 | 15 → 7 | 260 → 228 |
| 50 | 50 → **2** | 678 → 522 | 55 → 7 | 1309 → 1153 |
| 100 | 100 → **2** | 1370 → 1012 | 105 → 7 | 2621 → 2263 |

| | before | after | Δ |
|:--|--:|--:|:--|
| `outcome_write_calls` @ n=100 | 100 | 2 | **-98%** |
| `outcome_write_buffers` @ n=100 | 1370 | 1012 | **-26.1%** |
| `total_calls` @ n=100 | 105 | 7 | **-93.3%** |
| `total_buffers` @ n=100 | 2621 | 2263 | **-13.7%** |
| `wal_bytes` @ n=100 | 184152 | 189168 | +2.7% |

**`outcome_write_calls` goes from `n` to exactly `2` at every swept size**
(one `Delivered` batch, one `Backoff` batch) -- the O(n) → O(1)
statement-count shape, demonstrated at three input sizes. This alone clears
the impact floor ("elimination of an N+1"). **`total_calls` is also flat at
7 regardless of `n`** after the fix (claim + payload re-read + 2 batched
outcome writes + table-existence probe + 2 harness-only WAL reads) -- the
whole tick's round-trip count no longer scales with backlog depth.

**`outcome_write_buffers` also drops 26.1% at n=100** -- a second,
independent way this clears the impact floor (≥20% buffer reduction on a
statement that was ≥5% of the tick's total buffers; here it was 52.3%). The
`Delivered`/`Backoff` batched `UPDATE`s scanning the whole (small) fixture
table cost less in aggregate than N separate index-probe `UPDATE`s once
per-statement overhead (parse/bind/plan/execute round trips) is netted out,
even though the large-table plan probe above shows the two approaches
converge in buffer cost once the table is production-sized and both use an
index. Take the buffer win at small scale as a bonus, not the headline
claim; the statement-count elimination is the finding that generalizes.

**`wal_bytes` rises slightly (+2.7%) at n=100.** Both statement shapes write
the same logical rows with the same column values; the small increase is
consistent with the batched `UPDATE`'s different physical write pattern
(one statement touching a broader tuple set at once) rather than any
additional logical write. No write-path win is claimed here; this line is
reported per the "WAL bytes required for any write-path claim" rule, in the
interest of not omitting an unfavorable number.

Tool: `pg_stat_statements` (`calls`, `shared_blks_hit + shared_blks_read`)
and `pg_wal_lsn_diff`, captured via `pg_stat_statements_reset(0, dbid, 0)`
immediately before each measured call.

## Equivalence

Three permanent (non-`#[ignore]`d) regression tests in
`completion_callback_outcome_batch_perf.rs`, run against the fixed code on
every normal test run:

* `scanner_records_identical_outcomes_for_a_mixed_delivered_and_backoff_batch`
  -- 21 rows (11 `Delivered`, 10 `Backoff`), asserts every row's `state`,
  `last_status`, `last_error`, `delivered_at`-is-set, and `attempt` land
  exactly where the per-row path would have put them.
* `scanner_handles_a_single_row_batch` -- the degenerate `unnest` case (one
  element per array), still resolves correctly.
* `scanner_dead_letters_alongside_a_batched_delivered_and_backoff_mix` --
  `max_attempts = 1` forces the odd-index rows to exhaust immediately;
  confirms `DeadLetter` still writes `FAILED` plus exactly one
  `harvest_dead_letters` row per exhausted delivery, unaffected by the
  `Delivered`/`Backoff` rows resolving in the same tick.

Also re-verified against the full pre-existing suite: all 3680 lib tests,
all 98 `completion*`-prefixed integration tests (real Postgres via
testcontainers, including `completion_callback_tests.rs`'s original 23
tests covering SSRF re-validation, HMAC signing, redaction, shard scoping,
redrive, and the erase-race-vs-dead-letter CAS), and all 18
`ci_run_coverage` guard tests, unchanged.

## Reproduce

```sh
export HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
# role must be able to CREATE DATABASE and reset pg_stat_statements;
# shared_preload_libraries must include pg_stat_statements.

# Sweep + profile (writes docs/perf-artifacts/completion-callback-outcome-batch/):
PERF_LABEL=after cargo test -p autumn-harvest --features db,testing --test integration -- \
  --ignored zz_capture_completion_callback_outcome_batch_perf_evidence --test-threads=1 --nocapture

# The three permanent equivalence tests:
cargo test -p autumn-harvest --features db,testing --test integration -- \
  completion_callback_outcome_batch_perf --test-threads=1
```

The production-shaped 500,100-row plan-shape probe (not committed -- a
throwaway fixture, reproduce directly against any Postgres 16 instance):

```sql
CREATE TABLE harvest_completion_deliveries (...);  -- see migrations/20260705000000_.../up.sql
INSERT INTO harvest_completion_deliveries (...)
SELECT ..., 'DELIVERED', ... FROM generate_series(1, 500000) i;  -- historical rows
INSERT INTO harvest_completion_deliveries (...)
SELECT ..., 'INFLIGHT', ... FROM generate_series(1, 100) i;      -- the claimed batch
ANALYZE harvest_completion_deliveries;
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)
UPDATE harvest_completion_deliveries d SET state = 'DELIVERED', ...
FROM unnest($ids::uuid[], $attempts::int4[], $statuses::int4[]) AS v(id, attempt, last_status)
WHERE d.id = v.id AND d.attempt = v.attempt;
```
