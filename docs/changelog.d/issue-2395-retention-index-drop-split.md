## Fix — the retention index migration no longer deadlocks (issue #2395)

Migration `20261029034500_harvest_retention_candidates_index` shipped in 0.8.0.
It built `idx_harvest_we_retention_candidates` and dropped
`idx_harvest_we_retention_scan` in one transaction. The plain build holds
`SHARE` on `harvest_workflow_executions` until the commit. The drop then asks
for `ACCESS EXCLUSIVE` on the same table.

A step transaction that read or locked an execution row waits behind the
build for its write. The drop then waits behind that step transaction. That
is a deadlock. Postgres aborted the migration, and the whole build rolled
back. On Postgres 16, one such step transaction was enough. An open reader
also blocked the drop until `lock_timeout` failed the migration. So on a busy
deployment, the migration could fail each time after it blocked every state
change for the full build.

`20261029034500` now builds the new index only. The new migration
`20261101012649_harvest_retention_scan_index_drop` drops the old index in its
own transaction. That drop holds no earlier lock, so it cannot deadlock. A
lost wait fails the drop only, and the committed build stays. A database that
applied the 0.8.0 form has no old index, so the new migration takes no lock.
The end schema is the same.

**Lint.** `migration_lock_safety` has a new rule, `lock-upgrade`. It fails a
plain `DROP INDEX` or `ALTER TABLE` of a hot table after a plain build of the
same table in one file. No annotation allows it. The other build-and-drop
migrations in the tree drop first, so none of them breaks the rule.

**Upgrade.** On a large deployment, build the new index `CONCURRENTLY` first,
as before, then run
`DROP INDEX CONCURRENTLY IF EXISTS idx_harvest_we_retention_scan;`. Both
migration headers give the statements. No new `WorkflowEvent` variant and no
replay impact.
