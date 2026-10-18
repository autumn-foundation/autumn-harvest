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

5,000-row bulk updates, WAL bytes (`write_cost.sql`, 300k executions):

| transition | without index | with index |
|:--|--:|--:|
| RUNNING to COMPLETED | 82,430,680 | 82,448,848 |
| COMPLETED to PAUSED | 10,280,960 | 10,731,520 |
| PAUSED to RUNNING | 11,763,712 | 10,592,256 |

The first update after a checkpoint pays full-page writes, so read the columns as a comparison
and not as a per-row cost.

## Migration

On the 1,000,000-execution fixture: `up.sql` 83 ms, `down.sql` 1 ms. The build holds `SHARE` on
`harvest_workflow_executions`. The revert holds `ACCESS EXCLUSIVE` for the drop. Both fail after a
5 s lock wait. The index is 48 kB at 1,000 PAUSED rows.

## Profile

`profile/` holds the `pg_stat_statements` snapshot of the `throughput` scenario at one shard
(1,200 workflows) and an excerpt of the sampled claim plan. The claim statement is 50.2% of
buffers and 2.7% of calls in that run (46.7% and 2.6% in an earlier run).
