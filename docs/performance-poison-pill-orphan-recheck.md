# `poison_pill::reclaim_orphaned_tasks` — one worker-liveness re-check per orphan, not per crashed worker

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
call-count shape's insensitivity to worker cardinality directly: if the
re-check were already O(distinct workers), the count would stay flat near
3 as `n` grows. All seeded rows carry `crash_strikes = 0` against a
quarantine threshold far above 1, so every row takes the `Requeue` path —
the common case, and the one this page measures.

## 📈 Profile

`pg_stat_statements` call counts, pre-fix code, one sweep per size (`n` =
orphan count, 3 distinct dead workers at every size):

| n | worker-liveness re-check calls | candidate-scan calls | row-lock + write calls (`harvest_task_queue`) | total statements |
|--:|--:|--:|--:|--:|
| 60 | 60 | 1 | 120 | 181 |
| 300 | 300 | 1 | 600 | 901 |
| 1,500 | 1,500 | 1 | 3,000 | 4,501 |

The re-check scales at exactly **1 per orphan**, completely flat against
the fixed 3-worker cardinality (60/300/1,500 vs. 3 distinct workers at
every size) — it is provably not amortized across the crash that produced
all of them. Per orphan, the loop pays three round trips: the `SELECT ...
FOR UPDATE` re-verifying the row, the dedicated liveness `SELECT`, and the
`UPDATE` itself. At the 1,500-row headline size, this loop's own
bookkeeping (4,500 of 4,501 total statements) **is** the sweep's cost, not
a fraction of it worth weighing against a floor — the same conclusion
`docs/performance-mutex-lease-reclaim.md` reached for its sibling loop.

## 💡 Hypothesis

Postgres's own row-level locking makes the separate `SELECT ... FOR
UPDATE` re-read redundant: an `UPDATE`'s `WHERE` clause is evaluated
against the current row under an implicit row lock, which is exactly the
"lock it, then check it is still what we think it is" property the old
`SELECT ... FOR UPDATE` step existed for. The worker-liveness check can
fold into that same `WHERE` clause as a `NOT EXISTS` subquery. This does
not change *when*, relative to the row's own lock, the liveness re-check
happens — it is still evaluated fresh, in the same transaction,
immediately before the write — it only removes the extra round trip.
`quarantine_orphan` needs the row locked *before* inserting the
dead-letter entry (which must happen before the row itself is written), so
its equivalent fold is a `SELECT ... FOR UPDATE` carrying the same `NOT
EXISTS`, not an `UPDATE`.

This is deliberately **not** the same class of change as the
`quota_reconcile` sweep's per-row-transaction finding (issue #1511, a
findings issue rather than a PR): that one would have shared a single
liveness read across *multiple rows' transactions*, widening the race
window between the check and each row's own lock. This fix combines
statements *within* the same row's own transaction, changing nothing
about when or how often each row is locked — the same distinction
`docs/performance-mutex-lease-reclaim.md` draws between its combined
statement (safe) and batching its per-key advisory lock away (not
attempted, would change locking semantics).

## 🔧 Change

Two new statements in `poison_pill.rs`:

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

```sql
-- quarantine_precheck_stmt
SELECT id FROM harvest_task_queue
WHERE id = $1 AND state = 'RUNNING' AND worker_id = $2 AND crash_strikes = $3
  AND NOT EXISTS (
      SELECT 1 FROM harvest_workers w
      WHERE w.worker_id = $2
        AND w.last_heartbeat_at > NOW() - ($4::bigint * INTERVAL '1 second')
  )
FOR UPDATE
```

`requeue_orphan` drops from three statements (`SELECT ... FOR UPDATE`,
the liveness `SELECT`, the `UPDATE`) to one. `quarantine_orphan` drops
its opening two (the same `SELECT ... FOR UPDATE` plus the liveness
`SELECT`) to one, keeping the row locked across the subsequent
dead-letter insert and `FAILED` write exactly as before. The standalone
`worker_still_dead` helper is deleted — nothing calls it anymore.

**No new index, no schema change, no migration.**
`orphaned_running_tasks_query` and `stuck_running_tasks_query` (the
candidate scans) are untouched.

## 📊 Measurement

Same harness, same three fixture sizes, before vs. after:

| n | total statements (before) | total statements (after) | Δ | worker-liveness calls (before) | worker-liveness calls (after) |
|--:|--:|--:|--:|--:|--:|
| 60 | 181 | 61 | **-66.3%** | 60 | **0** |
| 300 | 901 | 301 | **-66.6%** | 300 | **0** |
| 1,500 | 4,501 | 1,501 | **-66.7%** | 1,500 | **0** |

The dedicated worker-liveness statement disappears entirely — 0 calls at
every size, not merely fewer — because it no longer exists as a separate
statement; its condition is now evaluated as part of the row's own write.
`requeue_orphan`'s per-row round trips drop from 3 to 1, exactly as the
mechanism predicts. This clears the impact floor outright under
"elimination of an N+1": the worker-liveness check's own statement count
goes from O(n) to a hard 0, and total sweep statements drop by two-thirds
at every swept size, not just the headline one.

**Buffers are not the right lens here, and this page does not claim a
buffer win.** `worker_recheck_buffers` was already 0 at every pre-fix
size (`harvest_workers` is `worker_id`-PK-indexed and tiny; a single-row
lookup by primary key touches at most one cached page). This fix removes
round trips for work Postgres was already doing for free, not I/O — the
same conclusion, for the same reason, that
`docs/performance-mutex-lease-reclaim.md` reached for its sibling fix.
The admissible evidence here is the statement/`calls` count itself, which
needs no further justification per this repo's own performance playbook
("Statement count per unit of work ... going from O(n) statements to O(1)
... needs no other justification").

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

## Write cost

None — read-path (well, read-then-conditional-write) bookkeeping
restructuring only. No index added, no schema change, no migration.

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
`autumn-harvest/src/poison_pill.rs`'s `requeue_orphan_stmt` /
`quarantine_precheck_stmt` addition and the `requeue_orphan` /
`quarantine_orphan` bodies to the commit before this page's fix (the RED
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
- `cargo clippy -p autumn-harvest --features db,testing --all-targets --
  -D warnings` — clean.
- `cargo test -p autumn-harvest --features db --test integration --
  poison_pill_reclaim_perf poison_pill_tests --test-threads=1` — the new
  harness plus the full pre-existing orphan-reclaim DB suite, all pass.
  (`poison_pill_tests.rs` needs a Docker daemon; verified separately from
  an environment with one available, since this measurement environment
  does not have one.)

Checked for duplicate/overlapping work first: no open PR or issue touches
`poison_pill.rs`'s orphan-reclaim loop; issue #1511 (the `quota_reconcile`
per-row-transaction finding) is a different sweep, a different table, and
a different class of change (see "Hypothesis" above for why that one
needs a maintainer decision and this one does not).

## Known limitations

* **This page measures the `Requeue` path.** `quarantine_orphan` carries
  the identical fix (see "Change" above) but this page's sweep never
  quarantines — every seeded row's `crash_strikes` stays under the
  threshold, so the quarantine path's call-count reduction is not
  separately swept. It is the same statement-elimination mechanism
  applied to a second call site, verified for correctness (see
  "Equivalence") but not re-measured at scale, since `quarantine_orphan`
  does substantially more per-row work after the shared re-check (a
  dead-letter insert, an owning-workflow failure cascade) that this fix
  does not touch.
* **The buffer flatness is specific to this fix's mechanism** (fewer
  round trips against an already-tiny, PK-indexed table), not a general
  finding about combining statements. A future fix elsewhere on this
  page's parent document should still measure buffers, in case its
  access pattern differs from this one's.
