# The timeout-checker refill reads one page per pass

Issue #2126 found that the task-timeout refill read 9.3% of all buffers in
the e2e bench. The checker has four lanes, one per timeout reason. Before
this change, each lane read the same page of live rows with its own
statement. Now lanes at one position share one statement, so the page is
read once.

> **This is a reference measurement, not an SLO.** One machine, one Postgres
> configuration. Reproduce it before you design against it.

## TL;DR

At 100,000 live rows, over 8 passes:

* **Refill buffers fall 75%** (60,528 → 14,958). Buffers for all statements
  of the pass fall 56% (80,726 → 35,156).
* **Refill temp blocks fall 85%** (39,383 → 6,017). Three of four page spills
  go, and a later page no longer sorts on disk.
* **Refill server time falls 75%** (2,871 ms → 709 ms). JIT inlines 2 refills,
  not 16.
* **Clock reads fall 75%** (8 → 2, and 32 → 8 at 1,000 live rows).
* The passes return the same number of rows (4,000, and 160 at 1,000 live
  rows). Two tests check that each row keeps its reason (see below).
* No migration, no new index, no schema change, no change to the public API.

## Reference environment

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  PERF_LABEL=after cargo test -p autumn-harvest --features db,testing \
  --test integration -- --ignored --exact \
  timeout_refill_shared_page_perf::zz_capture_timeout_refill_evidence --nocapture
```

| | |
|:--|:--|
| Postgres | 16, `pg_stat_statements` preloaded, `work_mem = 4MB`, default JIT settings |
| Harness | `autumn-harvest/tests/integration/timeout_refill_shared_page_perf.rs` |
| Fixture | `N` live rows (30% `RUNNING`, 70% `PENDING`) and `2N` terminal rows. 1% of the live rows miss start-to-close, 1% miss schedule-to-start. Fixed seed. |
| Workload | 8 passes of `find_timed_out_tasks_batch` at the default batch of 500, from a new cursor |
| Artifacts | `docs/perf-artifacts/timeout-refill-shared-page/` (`before-*`, `after-*`) |

"Refill ms" is `total_exec_time` from `pg_stat_statements`, so it is server
time. "JIT inline" counts refills that JIT inlined or optimized.

## Result

| live rows | | refill calls | refill buffers | refill temp blocks | refill ms | JIT inline | clock reads | all buffers | rows found |
|--:|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| 1,000 | before | 32 | 1,216 | 0 | 40 | 0 | 32 | 1,912 | 160 |
| 1,000 | after | 8 | 304 | 0 | 12 | 0 | 8 | 1,000 | 160 |
| 100,000 | before | 32 | 60,528 | 39,383 | 2,871 | 16 | 8 | 80,726 | 4,000 |
| 100,000 | after | 8 | 14,958 | 6,017 | 709 | 2 | 2 | 35,156 | 4,000 |

## Change

`find_timed_out_tasks_batch` groups the lanes that refill in a pass. Lanes
with one sweep clock and one cursor form a group. One statement reads the
page. Each lane of the group tests its predicate once, in its own CTE. A
later lane of the group leaves to an earlier one each row that the earlier
one matches or holds. A lane outside the group keeps the `EXCEPT` rules of
issue #1795.

Lanes that start a sweep in one pass take one clock. So lanes that start in
step stay in step while no lane has a backlog. A lane that falls behind
refills on its own. On a sweep of one page, it joins the others again at its
next sweep. On a sweep of several pages, it can stay out of step.

A group stops at a lane that refills at another position. A later lane
needs that lane's claim after its refill, and the group reads first.

Two plan rules keep the shared statement cheap:

* Each predicate runs once per group. A second test of the schedule-to-start
  predicate for each later lane raises the plan cost of a full page over
  `jit_inline_above_cost`. JIT then takes about 2 s per refill on this
  machine.
* A later page also bounds the plain index key (`key >= $1`). The planner
  cannot estimate the row comparison `(key, id) > ($1, $2)`. Without the
  plain bound, it sorts a bitmap scan of every later row on disk. With it,
  an index scan stops at the page.

## Why each row keeps its reason

A group lane has an empty queue before the refill. After the refill, a later
lane leaves a row to it if the group lane matches the row or holds it. That
is true whether the group lane's sweep goes on or ends. So a group lane needs
no cursor in the shared statement, only its match set and the ids it held.

* `a_shared_refill_gives_each_row_its_first_reason` (`scanner_lease_tests`)
  runs a batch of 1 over pages of 64 rows, so lanes fall out of step. It ends
  each returned row, and checks its reason against the unbounded scan.
* `a_seeded_pass_gives_each_row_its_first_reason` (this harness) checks one
  pass over the fixture against the unbounded scan.

## Limits

* The fixture has no lane backlog, so the lanes stay in step. This is the
  best case for the change. A lane that falls behind costs one more refill
  per pass, as before.
* The first page of a sweep still spills above about 28,000 live rows at the
  default batch and 4 MB `work_mem` (the issue's `EXPLAIN` figure). JIT still
  inlines that refill on this fixture. Options B (a scoped `work_mem`) and C
  (a smaller page) of issue #2126 are not part of this change. A scoped
  `jit = off` would need the same transaction as option B.
* The `EXCEPT` steps run as hash set operations. Their memory is bounded by
  the page, not by `work_mem`.
