# The parent-close-cascade unfinished-update-handler check's N+1

`check_and_report_unfinished_handlers` is a best-effort, error-ignored
diagnostic: after a workflow reaches a terminal state, it re-loads that
execution's full `harvest_events` history and warns/emits a metric if the
history ends with an update handler `UpdateAdmitted` but never
`UpdateCompleted`/`UpdateFailed`. It runs strictly after the writing
transaction commits — it is not on the durability path.

`apply_parent_close_cascade` closes every child a `ParentClosePolicy` still
governs when its parent goes terminal, and returns the closed
`(ExecutionId, workflow_name)` pairs as `closed_children`. Every caller of
that cascade looped over `closed_children` and called
`check_and_report_unfinished_handlers` once per child — one
`SELECT * FROM harvest_events WHERE workflow_exec_id = $1 ORDER BY event_id`
per child, on every one of a parent's own terminal transitions. The same
shape recurs for the admission path's own `deferred_checks` (a
superseded-run cancellation can close more than one prior execution too).
Eighteen call sites carried this loop, across `worker.rs`, `timeout.rs`,
`execution.rs`, and `completion_trigger.rs`.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (below). Reproduce it on your own
> hardware before designing against it — the harness is in the repo
> precisely so you can.

## TL;DR

* **Eighteen call sites looped `check_and_report_unfinished_handlers` once
  per closed execution.** Fifteen share the exact `Vec<(ExecutionId,
  String)>` shape (`closed_children` from a parent-close cascade, or
  `deferred_checks` from the admission/cancel path) and batch directly.
  Three more were inspected and deliberately left alone: one wraps each
  check in its own savepoint for a documented isolation reason (`worker.rs`,
  the terminal-write commit-boundary path); two resolve per-execution
  cross-shard residence and may route different ids to different physical
  connections (`timeout.rs`'s cross-pool cancel-outbox sweep).
* **The fix is one new batched, chunked query** —
  `store::load_histories_undecoded_batch`, an `eq_any` load over
  `harvest_events` grouped by execution id in memory — and one new
  `execution::check_and_report_unfinished_handlers_batch` that reports each
  pair from the in-memory result. Both are read-only; no schema change, no
  index change (the existing `idx_harvest_events_exec (workflow_exec_id,
  event_id)` index already serves the batched query's `eq_any` + `ORDER BY`
  exactly as it served the single-execution query's `=` + `ORDER BY`).
  **Review finding on this PR (Codex, P2):** an unchunked `eq_any` over an
  unbounded cascade would materialize every event row of every requested
  execution into one `Vec` before grouping, turning peak memory from "one
  history" into "the sum of every history in the cascade." The shipped
  version chunks `exec_ids` into groups of `HISTORY_BATCH_CHUNK = 100`, one
  query per chunk, bounding peak memory back down to roughly one chunk's
  worth at the cost of `ceil(n / 100)` round trips instead of one.
* **Measured on a 400-child fixture**: the per-child loop issued **400**
  `harvest_events` statements touching **1,573** buffers
  (`shared_blks_hit + shared_blks_read`); the chunked batched call issues
  **4** statements (400 executions / 100 per chunk) touching **1,548**
  buffers — **calls 400→4 (-99.0%)**. This clears the impact floor via
  N+1 elimination: statement count drops from one-per-execution to a small
  constant, bounded by the chunk size regardless of cascade width. The
  buffer count barely moves at this size (**-1.6%**, short of the 20%
  floor) — chunking trades away most of the single-scan buffer win the
  unchunked version had, in exchange for the bounded-memory guarantee the
  review flagged as missing. For a cascade at or under the 100-execution
  chunk size (the common case per the original investigation, "dozens to
  hundreds" of children), the chunked form still collapses to a single
  query, matching the unchunked version's numbers exactly.
* **Result-equivalence is exact.** Both strategies report the identical
  sorted set of `(workflow_name, unfinished_update_handler_count)` pairs —
  asserted in both the always-run correctness test and the evidence-capture
  test, over a fixture that includes children with a genuinely unfinished
  update handler (every 15th child) and children with none.
* **No write-path cost.** This diagnostic never writes; there is nothing to
  measure on the WAL/ingest side, and no index was added.

## Reference environment

| | |
|:--|:--|
| Machine | linux / 4 logical CPUs |
| Postgres | 16.13 (Ubuntu), default `shared_buffers` |
| Harness | `autumn-harvest/tests/integration/parent_close_cascade_unfinished_handlers_perf.rs` |
| Artifacts | `docs/perf-artifacts/parent-close-cascade-unfinished-handlers/` (committed, this page's source) |

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/parent_close_cascade_unfinished_handlers_perf_repro.sh
```

`HARVEST_TEST_DATABASE_URL` is treated as an **admin** URL by
`claim_bench_support::db::setup_bench_db`, exactly as every other harness in
this crate treats it: a fresh, uniquely-named database is created and
migrated, `pg_stat_statements` is preloaded/enabled, and an idle lease
connection is held open for the life of the run. When unset, the harness
falls back to a `postgres:16` testcontainer with `pg_stat_statements`
preloaded, reclaimed entirely by the Docker daemon on exit.

## The fixture

400 closed children, each with a 30-event history (one `WorkflowStarted`,
28 `SignalReceived` filler events, and a terminal event) — a large
parent-close cascade (a wide fan-out workflow whose parent just went
terminal), not a toy input. Every 15th child's terminal event is replaced
with a trailing `UpdateAdmitted` and no matching completion/failure, the
exact shape the check exists to detect — 27 of the 400 children in this
fixture carry one. Every history is a deterministic function of its index,
so the fixture, and the before/after result sets it produces, are
byte-identical on every run.

## Measurement

Both strategies ran against the identical seeded fixture in the same
session, with `pg_stat_statements` reset between them so each capture is
attributable to only its own strategy.

| | Before (looped, N=400) | After (chunked batch, 100/chunk) | Δ |
|:--|--:|--:|--:|
| `harvest_events` SELECT calls | 400 | 4 | -99.00% |
| Total buffers (`shared_blks_hit + shared_blks_read`) | 1,573 | 1,548 | -1.59% |
| Unfinished-handler reports | 27 | 27 | identical (byte-for-byte) |

Raw `pg_stat_statements` rows and the full sorted result-row dumps are
committed at `docs/perf-artifacts/parent-close-cascade-unfinished-handlers/`
(`before.pg_stat_statements.txt`, `after.pg_stat_statements.txt`,
`before.result-rows.txt`, `after.result-rows.txt`).

The batched query's plan text itself changes shape as expected — `WHERE
workflow_exec_id = $1` becomes `WHERE workflow_exec_id = ANY($1) ORDER BY
workflow_exec_id ASC, event_id ASC` — but still leads on the same
`idx_harvest_events_exec (workflow_exec_id, event_id)` index the
single-execution form used. Postgres serves each 100-execution chunk as one
ordered index scan, four scans total for this 400-child fixture, instead of
400 independent point lookups.

## Equivalence

`batched_check_agrees_with_the_per_child_loop` (always-run, not
`#[ignore]`d) seeds 40 children with the same history generator, runs both
strategies with a test `MetricsRecorder` that captures every
`record_workflow_unfinished_handlers` call, and asserts the sorted
`(workflow_name, count)` pairs are identical between the two. The
evidence-capture test repeats the same assertion at the full 400-child
fixture. Both strategies read already-committed, immutable history (this
check never runs before the writing transaction commits), so there is no
concurrent-write window for the two captures to disagree over.

One deliberate error-isolation trade-off is documented on
`check_and_report_unfinished_handlers_batch` itself: the old per-pair loop
kept one pair's failure from affecting any other (each ran its own
independent query), while a batched failure is reported for the whole
batch. Every caller already discards this error (`let _ = ...`); the only
behavior change is that an undecodable `event_data` value would now drop
the whole batch's reports instead of just the one pair's — a state that has
never actually been observed on already-committed history, and the
fixture/integration suite exercising this path would fail first if it ever
were.

## What was deliberately left alone

- **`worker.rs`'s savepoint-wrapped loop** (`commit_terminal_failure_if_still_claimed`'s
  deferred-checks path): each check runs in its own `conn.transaction`
  savepoint specifically so a swallowed error there cannot leave the
  *enclosing* transaction aborted. Batching would remove that per-pair
  isolation — a transaction-boundary change, out of scope for this pass.
- **`timeout.rs`'s cross-pool cancel-outbox sweep** (two sites): each
  `deferred_checks` entry resolves its own shard residence and may be
  checked against a *different* physical connection/pool than its
  neighbors. There is no single connection to batch the query against
  without first grouping by resolved residence — a routing change, out of
  scope for this pass.
