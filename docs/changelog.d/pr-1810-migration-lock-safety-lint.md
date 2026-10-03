## Tooling — online-migration lock-safety lint (issue #1810)

Safe online migrations were a convention, not a check. Only three migrations
set `lock_timeout`. `20260915231809` drops and rebuilds a unique index on
`harvest_workflow_executions` inside its transaction, and no test noticed.

**The lint.** `autumn-harvest/tests/integration/migration_lock_safety.rs`
reads every `up.sql` after `20260914165542`, in the core tree and both plugin
trees. It needs no database. It enforces two rules on the hot tables. These
are the tables that the engine touches on every claim or workflow step, plus
`harvest_audit_log` and `harvest_workflow_outbox`.

- `lock-timeout`: every blocking lock needs a non-zero `lock_timeout` in
  force. A later `RESET` or zero value ends the bound. Table DDL, `LOCK`,
  `TRUNCATE`, `CLUSTER`, `VACUUM FULL` and plain index DDL count. Trigger,
  policy, rule and `ALTER INDEX` DDL count. A foreign key `REFERENCES` and
  `PARTITION OF` count. Dropping a table or constraint whose foreign key
  references a hot table counts too.
- `blocking-index`: `CREATE INDEX`, `DROP INDEX` and `REINDEX` need
  `CONCURRENTLY`. An `ADD UNIQUE`, `PRIMARY KEY` or `EXCLUDE` constraint
  without `USING INDEX` builds an index, so it counts too.

`concurrently-in-transaction` catches `CONCURRENTLY` that cannot run. That is
a transactional migration, a `DO` block, or a file with a second statement.
Diesel sends each file as one batch, and Postgres runs a batch as one
implicit transaction.

The lexer skips comments and string literals. It lexes each `DO $$` body on
its own and scans it as code. The lint reads index and foreign-key history
from earlier migrations. A table that the same migration creates is exempt
from the `CREATE TABLE` on, unless the create has `IF NOT EXISTS`. An index
that no migration creates counts as hot.

**The escape hatch.** `-- lock-safety: allow <rule> #<issue> <reason>`,
directly above the statement. A malformed annotation and an unused annotation
both fail the build.

**Grandfathering.** Six shipped migrations after the cutoff break a rule,
`20260915231809` first. The `GRANDFATHERED` list names each one with the rule
and a reason. An entry newer than `20261002033903` fails the build, and so
does an entry that no longer matches a finding. `20261001190405` also drops a
hot index through `EXECUTE`, which the lint cannot see.

**CI.** The lint runs in an ungated step of the `lint` job.
`the_lint_runs_in_the_ci_lint_job` parses `ci.yml` to pin that step.

**Docs.** `docs/upgrading/online-migrations.md` is the author guide. It
covers the rules, `SET LOCAL lock_timeout`, the Diesel
`run_in_transaction = false` pattern for `CONCURRENTLY`, the guarded build,
and the annotation. A test lints every SQL example in the guide. The guide
states one limit: `test_init_sql()` applies every migration in one
`batch_execute`, so it cannot apply a `CONCURRENTLY` migration yet.

No migration, no `WorkflowEvent` variant, no runtime change.
