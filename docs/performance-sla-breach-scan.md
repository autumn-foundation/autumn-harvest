# The SLA-breach scan uses its partial index

`timeout::enforce_workflow_sla_breaches` runs on every timeout tick. Its
`UPDATE` flags runs that crossed their soft SLA. In steady state it flags
nothing. Before this change it still read every row of
`harvest_workflow_executions`. The partial index
`idx_harvest_executions_sla_deadline` was built for this scan and was never a
candidate.

> **This is a reference measurement, not an SLO.** One machine, one Postgres
> configuration. Reproduce it before you design against it.

## TL;DR

On the steady-state fixture (20% of runs carry an SLA, none pending):

| executions | before | after |
|--:|:--|:--|
| 10,000 | Seq Scan, 189 buffers | Index Scan, 1 buffer |
| 100,000 | Seq Scan, 1,887 buffers | Index Scan, 1 buffer |
| 1,000,000 | Seq Scan, 19,887 buffers | Index Scan, 1 buffer |

* The old plan reads every row (10,000, 100,000, 1,000,000 rows removed by the
  filter) to flag 0. The new plan reads none.
* The cost of a tick grows with the table before and stays flat after.
* No migration, no new index, no change to the schema or the public API.
* The flagged rows are identical before and after (see Equivalence).

## Why the index was unusable

The index predicate is:

```sql
WHERE sla_deadline_at IS NOT NULL AND sla_breached = FALSE AND state <> 'PAUSED'
  AND (completed_at IS NULL OR completed_at > sla_deadline_at)
```

The scan filters on `sla_deadline_at < COALESCE(completed_at, NOW())`. When
`completed_at` is NULL that is true for an overdue run, and the index clause is
true too. When `completed_at` is set, `COALESCE` returns it, so the filter is
`sla_deadline_at < completed_at`, which is the index clause. The filter implies
the index predicate. Postgres does not prove implications through `COALESCE`,
so it never picked the index. The three simple clauses did match.

The fix repeats the last index clause in the `WHERE`. The repeat adds no
condition: a NULL `completed_at` passes, and a set one passes exactly when the
original filter does.

## How much of the workload this is

In the `e2e_bench` profile the statement is small, because those tables hold
480 to 1,450 executions: 5.4% of buffers in `signal_roundtrip`, 1.8% in
`dispatch_latency` and 0.5% in `throughput` (`docs/perf-artifacts/sla-breach-scan/profile/`).
Its cost scales with the table. At 1,000,000 executions one tick reads 19,887
buffers, about 100 times one claim in the same profile (186 to 197 per call).
The scan runs once per timeout tick.

## Equivalence

`run.sh` loads the fixture, adds 14 boundary rows (`edge_rows.sql`) and runs the
scan inside a transaction that it rolls back. It writes which rows the scan
flags. `cmp` of `baseline/flagged_N.txt` and `after/flagged_N.txt` is identical
at all three sizes. The boundary rows are: running and overdue, running and not
yet due, completed late, completed on time, completed exactly at the deadline
(not flagged), completed one microsecond late (flagged), a terminal run with no
`completed_at`, PAUSED and overdue, already breached, no deadline, and the late
CANCELLED, TIMED_OUT, CONTINUED_AS_NEW and TERMINATED runs. The scan flags 8 of
the 14, which matches the expected column in every case. The existing
`sla_breach_tests` pass unchanged.

## Write cost

None. The change adds no index and no column, so insert, update and WAL cost
do not move. `idx_scan` on the index rose in the after runs, and the index is
16 kB on the fixture.

## Reproduce

```sh
# A Postgres 16 with every migration applied.
D="-h <host> -U postgres -d <db>"
cd docs/perf-artifacts/sla-breach-scan
./run.sh "$D" 1000000 baseline   # scan_body.sql: the old predicate
./run.sh "$D" 1000000 after      # scan_body_after.sql: the repeated clause
cmp baseline/flagged_1000000.txt after/flagged_1000000.txt
```

Artifacts: `docs/perf-artifacts/sla-breach-scan/`.
