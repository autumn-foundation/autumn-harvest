## Tooling — Ledger census batch 6: the SQLite profiles (issue #2429)

The Ledger census now covers SQLite. The `ledger-census` feature registers a
SQLite auto-extension, so every connection that a test opens records its
statements. `ledger-census.py plan-sqlite` plans them with `EXPLAIN QUERY
PLAN` on the schema that the census recorded. It needs no database.

103 of the 109 SQLite profiles now link a plan page under `docs/ledger/plans/`.
Five of the rest send only DDL or pragmas, so they have nothing to plan. No
test reaches `OwnerDiskCache::purge_entity`.

## Fixed — a resolved race scanned the whole SQLite task table (issue #2429)

`ctx.race()` cancels each losing activity. On SQLite, that `UPDATE` matched
the task by `activity_id` alone, and no index covers that column. The task
table keeps the rows of finished runs, so each cancel read every task in the
file. The statement now names the run too, so `idx_harvest_tasks_exec` finds
the row.

## Fixed — each SQLite timer arm read every timer (issue #2429)

Each timer arm takes the next `arm_seq` from `MAX(arm_seq)` over all timers.
Fired timers stay until retention purges their run, and no index covered
the column, so each arm read the whole table. The base schema now builds
`idx_harvest_timers_arm_seq`, so the read is one index step. An existing file
gets the index on its next open.

The other SQLite scans are by design: a startup reclaim, a one-time
back-fill, the small schedule and writer-lock tables, a test fixture, and the
read-only SQL that an operator runs over one entity.
