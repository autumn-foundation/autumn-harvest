## Performance — the retention candidate scan uses its index again (issue #2395)

The retention janitor selects terminal executions in six states, `TERMINATED`
included. Its partial index, `idx_harvest_we_retention_scan`, listed only five.
Postgres uses a partial index only when the query predicate implies the index
predicate, so each retention tick read the whole of
`harvest_workflow_executions`, on every shard.

Migration `20261029034500_harvest_retention_candidates_index` builds
`idx_harvest_we_retention_candidates` with all six states and drops the old
index. `MIGRATED` stays out, because a sealed row is not purgeable.

**Upgrade.** A plain `CREATE INDEX` takes `SHARE` on
`harvest_workflow_executions` and blocks state changes until the build ends.
On a large deployment, build the new index `CONCURRENTLY` first, then drop the
old one `CONCURRENTLY`. The migration header and the upgrade guide row give
both statements.

`retention_candidate_scan_index_tests` gates the plan. It fails when either
variant of the candidate scan cannot use the new index.

`retention::candidate_scan_sql` is now public, so the gate can plan the exact
text the janitor runs. No new `WorkflowEvent` variant and no replay impact.
