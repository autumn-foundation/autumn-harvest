# Ledger: the claim path on a seeded deep backlog (issue #1956)

Findings, not a fix. Issue #1956 profiled the e2e bench and found that the claim
CTE takes 22% of shared buffers at about 81 buffers per call. The issue could not
call that evidence: the bench drains queues of about 4k tasks, and no committed
fixture was production-shaped. This page re-runs Ledger against such a fixture.

No engine code, query, index or migration changes here. See
[why no fix ships](#why-no-fix-ships).

## TL;DR

* **Claim cost grows with backlog depth, not with work done.** On one seed and
  one workload, a 1M-row backlog costs **136,365 buffers per claim**. A 4k-row
  backlog costs 885. That is 154x the buffers for 250x the rows. The mean claim
  time grows from 17 ms to 15.3 s.
* **At 1M rows the claim spills to disk.** Each claim sorts about 868k eligible
  rows to return one. The sort writes 155 MB to disk (`work_mem` is 4 MB). Over
  the run, the claim wrote 6.2M temp blocks. The issue's tiny fixture wrote none.
* **The PAUSED-execution skip scans every execution on every claim.** In the
  plan, `harvest_workflow_executions` gets a `Seq Scan` with `state = 'PAUSED'`
  as its filter. At 1M rows it reads all 250k executions per claim. The e2e
  hook shows the same scan on the issue's own workload: 57,826 sequential scans
  and 38.1M tuples read for 1,440 executions.
* **The claim CTE is 99.3% of shared buffers at depth** (90.4% at 4k). Every
  other statement is below 0.3%. The FK `FOR KEY SHARE` lead from the issue is
  real but small at both depths.
* **JIT costs 2.4 s of a 15.6 s claim.** The plan cost is above
  `jit_above_cost`, and the planner plans each claim again.

## 🎯 Workload

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  autumn-harvest/scripts/deep_backlog_ledger_repro.sh
```

The script seeds a shallow (4k) and a deep (1M) fixture on seed 1956. On each it
runs 4 claimers for up to 2,000 claims or 600 s. A claimer calls
`queue::claim_task` over all 64 queues, completes the task, deletes it as the
hygiene sweep would, and enqueues a replacement. So the table keeps its depth.
The delete is the one measured statement that the engine does not issue. The harness then snapshots both
stats views and drops the database. Artifacts are in
[`docs/perf-artifacts/deep-backlog/`](perf-artifacts/deep-backlog/).

Server: PostgreSQL 16.15, `shared_buffers=128MB`, `work_mem=4MB`, `jit=on`, on a
4-core container. `fixture-summary.txt` records the full settings.

## 📈 Fixture

`tests/integration/deep_backlog_support.rs`. Every value derives from `md5` of
the seed, a tag and a row ordinal, so one seed gives one fixture, byte for byte.

| property | value at 1M | measured |
|:--|:--|:--|
| live task rows | 1,000,000 in 250,000 executions | exact |
| queues | 64, power skew `k=3`: head queue gets `(1/64)^(1/3)` | 25.1% (expected 25.0%) |
| concurrency keys | 4,096 on half the executions, `k=4`, cap 32 | hot key 12.5% (expected 12.5%) |
| states | 96.0% PENDING, 3.9% terminal, 1,024 RUNNING | RUNNING fills the fleet: 64 workers x 16 slots |
| RUNNING per key | at most the cap | 32 at most |
| dead tuples | 10% of the task queue | `n_dead_tup / (live + dead)` = 0.100 |
| heap | | 441.6 MiB |

The churn follows the task lifecycle. Released claims go
`PENDING -> RUNNING -> PENDING`. Reclaimed tasks go
`PENDING -> RUNNING -> COMPLETED -> deleted`. Every update changes `state`, so
none is HOT and each leaves dead index entries. Autovacuum is off on the fixture
tables, so the ratio holds while the workload runs. 10% models autovacuum lag:
the hygiene migration sets a 2% scale factor, which a healthy table stays near.

**A fixture that breaks an engine invariant measures the fixture.** The first
capture seeded 1% of rows as RUNNING with no caps: about 10k RUNNING rows, keys
far above their cap. Its deep run made 40 claims in 600 s. With RUNNING inside
the fleet and the caps, the same run makes 155. `running_rows_fit_the_fleet_and_the_key_caps`
now guards this.

## 🔬 Results

Per-call figures are from `pg_stat_statements` for the claim CTE. Per-claim
table figures divide the workload deltas of `pg_stat_user_tables` by the claim
calls.

| | shallow (4k) | deep (1M) | ratio |
|:--|--:|--:|--:|
| claims in the run | 2,003 in 18 s | 155 in 600 s | |
| claim CTE share of shared buffers | 90.4% | 99.3% | |
| claim CTE buffers per call | 885 | 136,365 | 154x |
| claim CTE mean time per call | 17 ms | 15,291 ms | 900x |
| claim CTE temp blocks written | 0 | 6,174,606 | |
| `harvest_task_queue` tuples read by seq scan, per claim | 4,000 | 1,000,000 | 250x |
| `harvest_workflow_executions` tuples read by seq scan, per claim | 1,000 | 250,000 | 250x |
| claim `UPDATE` buffers per call | 26 | 441 | 17x |

### The plan at 1M rows

From `deep-claim.explain.txt` (one claim, 15.6 s):

1. **Candidate scan and sort.** A `BitmapAnd` of `idx_harvest_tq_live_created`
   and `idx_harvest_tq_poll` feeds a bitmap heap scan that returns 868,423
   eligible rows (9.4 s). A `Sort` on the sticky `CASE` key orders them to
   return one row: `external merge  Disk: 155168kB`. This is the sort-elision
   defeat that `docs/performance.md` describes (issues #786 and #1177). Issue
   #1340 left its depth scaling open. At this fixture it is linear in buffers
   and worse than linear in time.
2. **`concurrency_pending_keys`.** A `Seq Scan` on `harvest_task_queue` reads
   454,617 keyed rows on every claim (359 ms). This is the per-claim sequential
   scan that `pg_stat_user_tables` shows.
3. **The PAUSED-execution skip.** A `Seq Scan` on `harvest_workflow_executions`
   with `Filter: (state = 'PAUSED')` removes all 250,000 rows (5,320 buffers).
   `idx_harvest_we_state` covers only `RUNNING`, so no index serves it.
4. **JIT.** 199 functions, 2.4 s of the 15.6 s.

At 4k rows the same plan sorts 3,573 rows in memory and runs in 19 ms.

### The e2e profile, with per-table counters

Issue #1956 could not capture `seq_tup_read`, because the e2e bench dropped its
databases first. With `HARVEST_BENCH_STATS_DIR` set, its teardown now writes
both views before the drop. One `throughput` cell at 1 shard (1,200 measured
workflows) gives:

| table | seq_scan | seq_tup_read |
|:--|--:|--:|
| `harvest_workflow_executions` | 57,826 | 38,115,332 |
| `harvest_task_queue` | 22 | 458 |

The claim CTE takes 30.2% of buffers at 149 buffers per call. The issue measured
22.3% at 81. An earlier run of this cell gave 22.7% at 85, so the share moves
from run to run. The executions scans read 38.1M tuples for 1,440 executions.
The PAUSED skip is one such scan per claim. Files:
`e2e-throughput-1shards-s0-*.txt`.

The snapshot is marked `PARTIAL`. Several pool sessions of the e2e fleet keep
writing after `fleet.stop()` until teardown ends them. That is an e2e harness
lifetime gap, outside this issue. The header of each file names the sessions.

## 💡 Leads (unproven, each needs a decision)

* **Index the PAUSED skip.** A partial index on `harvest_workflow_executions`
  `WHERE state = 'PAUSED'` would turn the per-claim sequential scan into an
  index probe. It is a schema change, so it needs a migration under
  `migration_lock_safety` and its own before/after.
* **Wire or extend seek-and-refine (#1340).** `queue::claim_task_batched`
  avoids the full sort for the concurrency gate. This fixture is the deep,
  skewed, keyed backlog its open questions name. Measure it here before a
  default-path decision.
* **Avoid the `concurrency_pending_keys` scan.** It reads every keyed pending
  row to find the keys, on every claim.
* **JIT on the claim path.** Ask whether `jit` should be off for the claim
  statement. That is an operator setting, not a query change.
* **The e2e fleet lifetime gap.** Pool sessions outlive `fleet.stop()`.

## Why no fix ships

Issue #1956 asks for the fixture, the harness and a Ledger re-run. Each lead
above changes the schema, the default claim path or an operator setting. Each
needs a human decision and its own before/after on this fixture.

## Limitations

* One machine, one server configuration, one seed. The fixture is
  deterministic, but timings on another box will differ. Buffers are the more
  stable figure.
* `shared_buffers` is 128 MB and the deep heap is 441.6 MiB, so the deep run
  reads from the OS cache. A larger `shared_buffers` moves time, not buffers.
* Autovacuum is off during the run, by design. A table that autovacuum keeps
  near 2% dead would read fewer dead tuples.
* The workload claims over all 64 queues with no build, capability or session
  filters. It does not exercise the dispatch-channel by-id claim.
* The deep run stops at its 600 s budget after 155 claims. The per-call figures
  rest on those 159 claim calls.

## See also

* [`docs/performance.md`](performance.md) — the claim-path measurement this page
  extends, and the sort-elision analysis.
* [`docs/performance-claim-batched-seek-and-refine.md`](performance-claim-batched-seek-and-refine.md) —
  issue #1340, whose depth question this fixture can now answer.
* [`docs/benchmarks.md`](benchmarks.md) — the e2e bench and
  `HARVEST_BENCH_STATS_DIR`.
