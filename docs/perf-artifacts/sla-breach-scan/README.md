# SLA-breach scan seq-scans `harvest_workflow_executions`

`timeout::enforce_workflow_sla_breaches` runs on every timeout tick. Its `UPDATE`
reads the whole executions table to flag zero rows. The partial index
`idx_harvest_executions_sla_deadline` exists for it, but the planner cannot use it.

## Mechanism

The index predicate ends with `AND (completed_at IS NULL OR completed_at > sla_deadline_at)`.
The scan filters on `sla_deadline_at < COALESCE(completed_at, NOW())`.
That implies the index clause, but Postgres cannot prove it, so the index is never a candidate.

## Baseline and after

`EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` of the scan on the steady-state fixture
(`fixture.sql`: 20% of runs carry an SLA, nothing pending), 0 rows flagged:

| executions | plan | buffers (hit + read) | rows read |
|--:|:--|--:|--:|
| 10,000 | Seq Scan | 189 | 10,000 |
| 100,000 | Seq Scan | 1,887 | 100,000 |
| 1,000,000 | Seq Scan | 19,887 | 1,000,000 |

After the change (same fixture, same session):

| executions | plan | buffers (hit + read) | rows read |
|--:|:--|--:|--:|
| 10,000 | Index Scan | 1 | 0 |
| 100,000 | Index Scan | 1 | 0 |
| 1,000,000 | Index Scan | 1 | 0 |

The change repeats the last index clause in the scan's `WHERE`. `scan_body_after.sql` has it.
`cmp baseline/flagged_N.txt after/flagged_N.txt` is identical at all three sizes. The flagged ids
cover 14 boundary rows (`edge_rows.sql`), and 8 are flagged, as the expected column says.
`idx_scan` on the index rose in the after runs.

`profile/` holds the `pg_stat_statements` snapshots of the `e2e_bench` scenarios.

## Reproduce

```sh
# A Postgres 16 with every migration applied (psql -f each up.sql in order).
D="-h <host> -U postgres -d <db>"
./run.sh "$D" 1000000 baseline   # writes baseline/explain_N.txt and baseline/flagged_N.txt
./run.sh "$D" 1000000 after
cmp baseline/flagged_1000000.txt after/flagged_1000000.txt
```
