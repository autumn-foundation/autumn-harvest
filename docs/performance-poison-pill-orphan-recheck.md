# `poison_pill::reclaim_orphaned_tasks` — one fewer round trip per requeued orphan

`poison_pill.rs`'s orphan-reclaim scanner already batches its candidate
scan into one statement (a `NOT EXISTS` anti-join against `harvest_workers`
in `orphaned_running_tasks_query`). This page targets the per-row loop
after it: for every candidate row, `requeue_orphan` (and
`quarantine_orphan`) re-verified the claiming worker was still dead with
its own dedicated `SELECT ... FROM harvest_workers WHERE worker_id = $1`,
immediately before the row's own write — the same "individually trivial,
collectively dominant" bookkeeping shape `docs/performance.md` and
`docs/performance-mutex-lease-reclaim.md` both name directly.

> **This is a reference measurement, not an SLO.** It was taken on one
> machine with one Postgres configuration (below). Reproduce it on your own
> hardware before designing against it — the harness is in the repo
> precisely so you can.

> **This page's first version was wrong, and this version explains why.**
> The initial fix folded the worker-liveness check into a single
> `UPDATE`/`SELECT ... FOR UPDATE` per row, and claimed a 3-statement-to-1
> reduction. An automated review caught a real race: under lock
> contention, that combined statement can read a stale, pre-wait snapshot
> of `harvest_workers`, wrongly concluding a resurrected worker is still
> dead. That was confirmed against a real Postgres, not just argued about
> — see "The bug the first version had" below. The fix shipped here keeps
> a separate row-lock statement and is a 3-to-2 reduction, not 3-to-1.

## 🎯 Workload

A poison-pill task crashes the worker **process** instead of returning a
clean `Err`, leaving every task it was concurrently running stuck
`RUNNING`. `max_concurrency` exists precisely so one worker can hold many
tasks at once — so one crash ordinarily produces *many* orphan rows behind
a *single* dead worker, not one orphan per dead worker. The re-check this
page targets is keyed on `worker_id`, not on the task row, so a crash that
orphans 200 tasks re-asks the identical "is this worker still dead"
question 200 times.

The harness is
`autumn-harvest/tests/integration/poison_pill_reclaim_perf.rs`, calling
`reclaim_orphaned_tasks` directly (the one real public entry point every
worker's periodic timeout tick calls) against a real Postgres 16 with
`pg_stat_statements` preloaded. It sweeps three fixture sizes (n=60/300/
1,500 orphaned `activity` tasks), round-robined across a **fixed** 3
distinct dead worker ids at every size, so the artifact demonstrates the
call-count shape's insensitivity to worker cardinality directly. All
seeded rows carry `crash_strikes = 0` against a quarantine threshold far
above 1, so every row takes the `Requeue` path — the common case, and the
one this page measures.

## 📈 Profile

`pg_stat_statements` call counts, pre-fix code, one sweep per size (`n` =
orphan count, 3 distinct dead workers at every size):

| n | worker-liveness re-check calls | candidate-scan calls | row-lock + write calls (`harvest_task_queue`) | total statements |
|--:|--:|--:|--:|--:|
| 60 | 60 | 1 | 120 | 181 |
| 300 | 300 | 1 | 600 | 901 |
| 1,500 | 1,500 | 1 | 3,000 | 4,501 |

The re-check scales at exactly **1 per orphan**, completely flat against
the fixed 3-worker cardinality — it is provably not amortized across the
crash that produced all of them. Per orphan, the loop pays three round
trips: the `SELECT ... FOR UPDATE` re-verifying the row, the dedicated
liveness `SELECT`, and the `UPDATE` itself. At the 1,500-row headline
size, this loop's own bookkeeping (4,500 of 4,501 total statements) **is**
the sweep's cost, not a fraction of it worth weighing against a floor —
the same conclusion `docs/performance-mutex-lease-reclaim.md` reached for
its sibling loop.

## The bug the first version had

The first attempt combined all three of the above into one statement per
row: a plain `UPDATE ... WHERE state='RUNNING' AND worker_id=$2 AND
crash_strikes=$3 AND NOT EXISTS (SELECT 1 FROM harvest_workers ...)`,
reasoning that an `UPDATE`'s own implicit row lock made the separate
`SELECT ... FOR UPDATE` redundant.

That reasoning is wrong for any condition that reaches *outside* the
locked row. Postgres's `EvalPlanQual` mechanism re-checks a concurrently
modified row by re-reading *that tuple* and re-evaluating the `WHERE`
clause — but everything else the `WHERE` clause touches, including a
`NOT EXISTS` against a different table, is still evaluated against the
snapshot the statement started with, taken *before* any wait. A worker
that resurrects (updates its `harvest_workers` heartbeat) while the
`UPDATE` is blocked waiting for a concurrent holder of the task row's lock
— for example, that worker's own `record_heartbeat` call touching the same
row — would be invisible to the `NOT EXISTS` check once the wait resolves.
The task would be wrongly requeued out from under a worker actually still
processing it: a duplicate-execution bug, not merely a missed
optimization.

This is not a theoretical concern. It was confirmed directly against a
real Postgres 16 with three concurrent `psql` sessions:

1. Session B opens a transaction and updates the target row (holding its
   lock, uncommitted).
2. Session A opens a transaction and runs the combined
   `UPDATE ... NOT EXISTS (...)`, which blocks on B's lock.
3. A third session updates and commits the `NOT EXISTS`-referenced row
   (simulating the worker's resurrection) while A is still blocked.
4. Session B commits, unblocking A.
5. Session A's `UPDATE` proceeds and reports the row updated — silently
   ignoring the resurrection that committed during its wait.

Splitting the row-lock acquisition into its own preceding statement (step
6 below) and re-running the same scenario correctly leaves the row
untouched: the second statement's fresh snapshot sees the resurrection.

## 💡 Hypothesis (revised)

A plain `SELECT ... FOR UPDATE` naming only `harvest_task_queue` absorbs
the wait instead. Once it returns, the row lock is already held for the
rest of the transaction, so the *next* statement can never itself need to
wait — and Postgres's `READ COMMITTED` mode starts every top-level
statement with a fresh snapshot as of its own start. That is what the
original three-statement code actually relied on for correctness: the
dedicated liveness `SELECT` was a separate statement, issued only after
the row-lock `SELECT ... FOR UPDATE` had already resolved any wait.

The safe optimization, then, is not "fold the liveness check into the row
lock" — it is "fold the liveness check into the *write that follows* the
row lock." `requeue_orphan` keeps its original `SELECT ... FOR UPDATE`
unchanged, then replaces the old liveness `SELECT` + `UPDATE` pair with
one combined `UPDATE ... WHERE ... AND NOT EXISTS (...) RETURNING`. That
statement is guaranteed never to block (the lock is already ours), so its
snapshot is guaranteed fresh relative to anything that resolved during the
first statement's wait.

`quarantine_orphan` cannot use even this reduced fold: it still needs the
row locked *before* inserting the dead-letter entry, so its
liveness check must remain the original, separate `worker_still_dead`
call between the row lock and the dead-letter write. Its statement count
is unchanged by this page.

This remains a different class of change from the `quota_reconcile`
sweep's per-row-transaction finding (issue #1511, a findings issue rather
than a PR): that one would share a single liveness read across *multiple
rows'* transactions, widening the race window between the check and each
row's own lock. This fix keeps exactly one lock and one fresh-snapshot
check per row, in the same transaction, changing nothing about when or
how often each row is locked.

## 🔧 Change

One new statement in `poison_pill.rs`, used only after the row lock is
already held:

```sql
-- requeue_orphan_stmt
UPDATE harvest_task_queue
SET state = 'PENDING', worker_id = NULL, started_at = NULL,
    sticky_worker_id = NULL, sticky_until = NULL, last_heartbeat_at = NULL,
    error = NULL, crash_strikes = $4, scheduled_at = NOW()
WHERE id = $1 AND state = 'RUNNING' AND worker_id = $2 AND crash_strikes = $3
  AND NOT EXISTS (
      SELECT 1 FROM harvest_workers w
      WHERE w.worker_id = $2
        AND w.last_heartbeat_at > NOW() - ($5::bigint * INTERVAL '1 second')
  )
RETURNING id
```

`requeue_orphan`'s shape is now: an unchanged `SELECT ... FOR UPDATE`
(row-lock only, no cross-table condition), then this one combined
statement in place of the old liveness `SELECT` + `UPDATE` pair. Three
statements become two. `quarantine_orphan` is unchanged from before this
page: `SELECT ... FOR UPDATE`, `worker_still_dead`, the dead-letter
insert, the `FAILED` `UPDATE` — four statements, same as always.

**No new index, no schema change, no migration.**
`orphaned_running_tasks_query` and `stuck_running_tasks_query` (the
candidate scans) are untouched.

## 📊 Measurement

Same harness, same three fixture sizes, before vs. after:

| n | total statements (before) | total statements (after) | Δ | worker-liveness calls (before → after) |
|--:|--:|--:|--:|--:|
| 60 | 181 | 121 | **-33.1%** | 60 → **0** |
| 300 | 901 | 601 | **-33.3%** | 300 → **0** |
| 1,500 | 4,501 | 3,001 | **-33.3%** | 1,500 → **0** |

The dedicated worker-liveness statement disappears entirely for the
`Requeue` path — 0 calls at every size, not merely fewer — because its
condition is now evaluated inside the write that already needs the row's
own fresh snapshot, instead of as its own round trip. This clears the
impact floor under "elimination of an N+1": the liveness check's own
statement count goes from O(n) to a hard 0. Total sweep statements drop by
a third at every swept size, not just the headline one — smaller than
this page's first (incorrect) claim of two-thirds, because the row-lock
`SELECT ... FOR UPDATE` has to stay.

**Buffers are not the right lens here, and this page does not claim a
buffer win.** `worker_recheck_buffers` was already 0 at every pre-fix
size (`harvest_workers` is `worker_id`-PK-indexed and tiny; a single-row
lookup by primary key touches at most one cached page). This fix removes
a round trip for work Postgres was already doing for free, not I/O — the
same conclusion, for the same reason, that
`docs/performance-mutex-lease-reclaim.md` reached for its sibling fix.
The admissible evidence here is the statement/`calls` count itself.

Full artifacts:
`docs/perf-artifacts/poison-pill-orphan-recheck/{before,after}-sweep.txt`.

## ✅ Equivalence

`requeue_and_quarantine_semantics_are_unchanged_by_the_combined_statement`
(in the harness file, always-run, not `#[ignore]`d) seeds one dead-worker
orphan below the quarantine threshold, one at it, and one task whose
worker is still heartbeating, then asserts against the fixed code: the
below-threshold orphan is requeued to `PENDING` with its dead attempt's
`worker_id`, `started_at`, `sticky_worker_id`, `sticky_until`,
`last_heartbeat_at` and stale `error` all cleared and `crash_strikes`
incremented by exactly one; the at-threshold orphan is quarantined —
`FAILED`, `crash_strikes` at the threshold, exactly one
`harvest_dead_letters` row referencing it; and the live-worker task is
left completely untouched (still `RUNNING`, strikes unchanged). This
mirrors the three cases `poison_pill_tests.rs`'s Docker-backed suite
covers end to end; it exists in this harness as well because
`poison_pill_tests.rs` requires a Docker daemon (`testcontainers`) and
this sweep's own harness — like every sibling `*_perf.rs` file on this
page's parent document — needs to run without one.

The pre-existing `poison_pill_tests.rs` DB integration suite (orphan
requeue, quarantine at threshold, the live-worker-is-never-reclaimed
case, the auto-pause counter, replay-into-terminal rejection) passes
**unmodified** against the fixed code.

Neither test seeds the specific lock-contention race "The bug the first
version had" describes — that race needs a second, blocking transaction
mid-flight, which is a concurrency scenario, not a single-connection
fixture. It was verified by hand against a live Postgres (see that
section) rather than added as an automated regression test, since
reliably driving a real cross-session lock wait from a `#[tokio::test]`
without a flaky sleep-based race is its own small project. Flagged here
rather than silently skipped.

## 💸 Write cost

None — no index added, no schema change, no migration.

## 🔬 Reproduce

```bash
service postgresql start   # local Postgres 16 with pg_stat_statements
                            # in shared_preload_libraries

# Correctness (always-run):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
  cargo test -p autumn-harvest --features db --test integration -- \
  poison_pill_reclaim_perf --test-threads=1

# pg_stat_statements sweep (writes
# docs/perf-artifacts/poison-pill-orphan-recheck/<label>-sweep.txt):
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres \
  PERF_LABEL=after \
  cargo test -p autumn-harvest --features db --test integration -- \
  zz_capture_poison_pill_reclaim_perf_evidence --ignored --nocapture --test-threads=1
```

`HARVEST_TEST_DATABASE_URL` is treated as an **admin** URL: a fresh,
uniquely-named database is created off it per measured size, migrated via
`autumn_harvest::test_init_sql()`, seeded, and measured. Without it, the
harness falls back to a per-test testcontainers Postgres — sufficient for
the correctness test, but the `#[ignore]`d evidence-capture test needs a
target with `pg_stat_statements` preloaded.

To reproduce the "before" half, temporarily revert
`autumn-harvest/src/poison_pill.rs`'s `requeue_orphan_stmt` addition and
the `requeue_orphan` body to the commit before this page's fix (the RED
commit checks out cleanly against the pre-fix code, since it adds only
the harness).

## Reference environment

| | |
|:--|:--|
| Machine | linux / shared vCPU (wall-clock inadmissible; see below) |
| Postgres | 16 (Ubuntu), default `shared_buffers` |
| Harness | `autumn-harvest/tests/integration/poison_pill_reclaim_perf.rs` |
| Artifacts | `docs/perf-artifacts/poison-pill-orphan-recheck/` (committed, this page's source) |

**Wall-clock is not evidence on this page.** This machine is a shared
cloud vCPU; every number above is a deterministic `pg_stat_statements`
count, never a timer. No wall-clock figure is quoted anywhere on this
page.

## Verification

- `cargo fmt --all` — clean.
- `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` — no
  Tier A findings, no Tier B regressions in either changed file.
- `cargo clippy -p autumn-harvest --lib --test integration --features
  db,testing -- -D warnings` — clean.
- `cargo test -p autumn-harvest --features db,testing --lib` — full unit
  suite passes unchanged.
- `cargo test -p autumn-harvest --features db --test integration --
  poison_pill_reclaim_perf` — new harness passes.
- `poison_pill_tests.rs` (Docker-backed) passes unmodified against the
  fix, verified separately from an environment with a Docker daemon.
- The lock-contention race described above, reproduced and fixed by hand
  against a live Postgres with three concurrent `psql` sessions.

Checked for duplicate/overlapping work first: no open PR or issue touches
`poison_pill.rs`'s orphan-reclaim loop; issue #1511 (the `quota_reconcile`
per-row-transaction finding) is a different sweep and a different class
of change (see "Hypothesis" above).

## Known limitations

* **This page measures the `Requeue` path only.** `quarantine_orphan` is
  unchanged by this page — see "Hypothesis" above for why its shape does
  not admit the same fold.
* **The lock-contention race this page's first version introduced is
  covered by manual verification, not an automated regression test.**
  See "Equivalence" above.
* **The buffer flatness is specific to this fix's mechanism** (fewer
  round trips against an already-tiny, PK-indexed table), not a general
  finding about combining statements. A future fix elsewhere on this
  page's parent document should still measure buffers, in case its
  access pattern differs from this one's.
