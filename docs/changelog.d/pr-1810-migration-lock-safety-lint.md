## CI — online-migration lock-safety lint (issue #1810)

Safe online migrations were a convention, not a check. Only two migrations
set `lock_timeout`. `20260915231809` drops and rebuilds a unique index on
`harvest_workflow_executions` inside its transaction, and no test noticed.

**The lint.** `autumn-harvest/tests/integration/migration_lock_safety.rs`
reads every `up.sql` after `20260914165542`, in the core tree and both plugin
trees. It needs no database. On the hot tables (`harvest_task_queue`,
`harvest_events`, `harvest_workflow_executions`, `harvest_timers`,
`harvest_signals`, `harvest_audit_log`, `harvest_workflow_outbox`) it
enforces two rules:

- `lock-timeout`: a blocking lock needs a non-zero `lock_timeout` before it.
  `ALTER TABLE`, `LOCK`, `DROP TABLE`, `TRUNCATE`, trigger DDL, a foreign key
  `REFERENCES`, and plain index DDL all count.
- `blocking-index`: `CREATE INDEX`, `DROP INDEX` and `REINDEX` need
  `CONCURRENTLY`.

`concurrently-in-transaction` catches `CONCURRENTLY` without
`run_in_transaction = false`. The lexer skips comments and string literals,
and it scans `DO $$` bodies as code. A table that the same migration creates
is exempt. An index that no migration creates counts as hot.

**The escape hatch.** `-- lock-safety: allow <rule> #<issue> <reason>`,
directly above the statement. A malformed annotation and an unused annotation
both fail the build.

**Grandfathering.** Six shipped migrations after the cutoff break a rule,
`20260915231809` first. The `GRANDFATHERED` list names each one with the rule
and a reason. An entry newer than `20261002033903` fails the build, and so
does an entry that no longer matches a finding.

**CI.** The lint runs in the ungated `lint` job.
`the_lint_runs_in_the_ci_lint_job` pins the step there.

**Docs.** `docs/upgrading/online-migrations.md` is the author guide. It
covers the rules, `SET LOCAL lock_timeout`, the Diesel
`run_in_transaction = false` pattern for `CONCURRENTLY`, the guarded build,
and the annotation. A test lints every SQL example in the guide. The guide
states one limit: `test_init_sql()` applies every migration in one
`batch_execute`, so it cannot apply a `CONCURRENTLY` migration yet.

No migration, no `WorkflowEvent` variant, no runtime change.
