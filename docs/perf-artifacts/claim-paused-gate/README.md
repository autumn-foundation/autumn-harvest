# Claim PAUSED-run gate (issue #2212)

Measures `queue::claim_task` against a seeded fixture. The `baseline/` directory is the
measurement before the `idx_harvest_we_paused` migration. The `after/` directory is the
same harness with the index.

## Reproduce

```sh
# A Postgres 16 with every migration before the paused-index one applied.
D="-h <host> -U postgres -d <db>"
./run.sh "$D" 100000 50 baseline     # n executions, PAUSED-run tasks at the head, output dir
./run.sh "$D" 1000000 10 baseline
# Apply the migration, then repeat with `after` as the output dir.
cmp baseline/claimed_ids_50p_100000.txt after/claimed_ids_50p_100000.txt
psql $D -f write_cost.sql            # WAL bytes, once without and once with the index
```

`claim.sql` is the claim statement as Postgres logged it for the `e2e_bench` throughput run.
`fixture.sql` has no `random()`. Every row derives from its row number.

## Result (EXPLAIN (ANALYZE, BUFFERS), shared hit + read)

| PAUSED-run tasks at the head | executions | before | after |
|:--|--:|--:|--:|
| 10 | 10,000 | 258 | 204 |
| 10 | 100,000 | 254 | 168 |
| 10 | 1,000,000 | 256 | 201 |
| 50 | 100,000 | 2,056 | 352 |
| 50 | 300,000 | 5,720 | 586 |
| 50 | 1,000,000 | 6,515 | 1,289 |

The claimed ids of 120 sequential claims are byte-identical in every cell. No claimed id sits on
a PAUSED run.

## Write cost

Bulk updates, WAL bytes (`write_cost.sql`, 300k executions). The snapshots use
`pg_current_wal_insert_lsn()`, because the write position lags the insertion position and can
shift records between windows. The first two transitions move 5,000 rows each. The last moves
5,300: the fixture starts with 300 PAUSED runs, and the script pauses 5,000 more. Each cell is
two runs after a checkpoint, and `after/write_cost.txt` holds the raw output.

| transition | without index | with index | difference |
|:--|--:|--:|--:|
| RUNNING to COMPLETED (5,000 rows) | 86,608,136 / 86,610,528 | 86,610,608 / 86,612,672 | +2,000 B (+0.002%) |
| COMPLETED to PAUSED (5,000 rows) | 10,280,008 / 10,280,008 | 10,733,904 / 10,733,920 | +453,900 B (+4.4%, about 91 B per row) |
| PAUSED to RUNNING (5,300 rows) | 8,100,960 / 8,100,960 | 8,100,976 / 8,100,960 | 0 |

Only a transition into PAUSED pays: the new row version enters the partial index. Every other
transition leaves the row outside the index predicate.

## Migration

On the 1,000,000-execution fixture: `up.sql` 83 ms, `down.sql` 1 ms. The build holds `SHARE` on
`harvest_workflow_executions`. The revert holds `ACCESS EXCLUSIVE` for the drop. Both fail after a
5 s lock wait. The index is 48 kB at 1,000 PAUSED rows.

## Profile

`profile/` holds the `pg_stat_statements` snapshot of the `throughput` scenario at one shard
(1,200 workflows) and an excerpt of the sampled claim plan. The claim statement is 50.2% of
buffers and 2.7% of calls in that run (46.7% and 2.6% in an earlier run).
