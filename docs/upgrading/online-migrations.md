# Lock-safe online migrations

Harvest migrations run against a live database. A migration that locks a hot
table stops the engine for as long as it holds the lock, or waits for it. A
waiting lock is as bad as a held one: every later write queues behind it.

The CI lint `migration_lock_safety` (issue #1810) checks every migration after
`20260914165542`. It runs in the `lint` job. Run it locally before you push:

```sh
cargo test -p autumn-harvest --no-default-features --features testing \
  --test integration migration_lock_safety::
```

The lint reads `up.sql` and `metadata.toml` only. It needs no database. Its
source is `autumn-harvest/tests/integration/migration_lock_safety.rs`.

---

## 1. Hot tables

Every workflow step reads or writes these tables:

- `harvest_audit_log`
- `harvest_events`
- `harvest_signals`
- `harvest_task_queue`
- `harvest_timers`
- `harvest_workflow_executions`
- `harvest_workflow_outbox` (application database)

A table that the same migration creates is not hot for that migration. No
session can hold a lock on it yet.

## 2. The rules

| Rule | Fails when | Fix |
|---|---|---|
| `lock-timeout` | A statement takes a blocking lock on a hot table, and no non-zero `lock_timeout` comes before it. | Put `SET LOCAL lock_timeout = '5s';` first. |
| `blocking-index` | A plain `CREATE INDEX`, `DROP INDEX` or `REINDEX` touches a hot table. | Use `CONCURRENTLY` (section 4), or the guarded build (section 5). |
| `concurrently-in-transaction` | `CONCURRENTLY` appears in a migration that runs in a transaction. | Add `metadata.toml` with `run_in_transaction = false`. |
| `bad-annotation` | A `-- lock-safety:` comment does not parse. | Fix the annotation (section 6). |
| `unused-annotation` | An annotation allows a rule that the statement below it does not break. | Remove the annotation. |

These statements take a blocking lock for `lock-timeout`:

- `ALTER TABLE`, in every form;
- `LOCK TABLE`, `DROP TABLE` and `TRUNCATE`;
- `CREATE TRIGGER` and `DROP TRIGGER`;
- `REFERENCES` on a hot table, in a new table or a new constraint. A foreign key
  takes `SHARE ROW EXCLUSIVE` on the table it references;
- a plain `CREATE INDEX` (`SHARE`), `DROP INDEX` (`ACCESS EXCLUSIVE`) or
  `REINDEX`.

The lint finds a `DROP INDEX` table from the migration that created the index.
An index that no migration creates counts as hot.

## 3. Bound the lock wait

```sql
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS example_note TEXT NULL;
```

Set the timeout before the first lock. A timeout set after the lock does not
count. A value of `0` or `DEFAULT` turns the timeout off, so it does not count.
Inside a `DO` block, `PERFORM set_config('lock_timeout', '5s', true)` also
counts.

`5s` is the bound that the existing lock-taking migrations use. When the
timeout fires, the migration fails and rolls back. Run it again.

`SET LOCAL` and `set_config(..., true)` do nothing outside a transaction. In a
migration with `run_in_transaction = false`, use `SET lock_timeout` instead.
End that migration with `RESET lock_timeout`, because the setting stays on the
connection.

## 4. Build indexes with `CONCURRENTLY`

`CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` take
`SHARE UPDATE EXCLUSIVE`. Reads and writes continue during the build. Postgres
rejects both inside a transaction block, so the migration must opt out of
Diesel's transaction.

`metadata.toml`, beside `up.sql`:

```toml
run_in_transaction = false
```

`up.sql`:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_events_example
    ON harvest_events (workflow_exec_id, timestamp);
```

Diesel and `harvest migrate` apply such a migration without a transaction.
Keep one statement in it. Nothing rolls back after a failure.

A failed concurrent build leaves an `INVALID` index. `IF NOT EXISTS` then
skips the build, and the invalid index stays. Before a retry, run
`DROP INDEX CONCURRENTLY IF EXISTS <index>` as a statement of its own.

**Limit.** `autumn_harvest::test_init_sql()` sends every `up.sql` in one
`batch_execute`. Postgres runs that call as one implicit transaction, so it
rejects `CONCURRENTLY`. A `run_in_transaction = false` migration therefore
also needs a change to that test bundle. Until the bundle supports it, use the
guarded build in section 5.

## 5. The guarded build

This is the pattern that most index migrations in this tree use. The migration
checks for a valid index with the same definition. It builds the index only
when the index is absent, and it fails when a different index has the name.
Operators with a large table prebuild the index out of band with
`CREATE INDEX CONCURRENTLY`, and the migration then does nothing.

`20261001192155_harvest_quota_reconcile_name_id_index/up.sql` is a full
example. A new guarded build bounds its lock wait and annotates the plain
build:

```sql
SET LOCAL lock_timeout = '5s';

DO $$
BEGIN
    IF to_regclass('idx_harvest_events_example') IS NULL THEN
        -- lock-safety: allow blocking-index #1234 operators prebuild it CONCURRENTLY
        CREATE INDEX idx_harvest_events_example
            ON harvest_events (workflow_exec_id, timestamp);
    END IF;
END $$;
```

Put the out-of-band recipe in the migration header and in the upgrade guide
row for the migration.

## 6. The escape hatch

An annotation allows one rule at one statement:

```sql
-- lock-safety: allow <rule> #<issue> <reason>
```

- Put it directly above the statement. Only `--` comment lines can sit
  between the annotation and the statement. A blank line ends the search.
- Allow one rule per annotation. A statement that breaks two rules needs two
  annotations.
- `<rule>` is `lock-timeout`, `blocking-index` or
  `concurrently-in-transaction`. Nothing allows `bad-annotation` or
  `unused-annotation`.
- Cite the issue that approves the exception, and give the reason.

A reviewer approves each annotation. It is the record that the lock is a
choice, not an accident.

## 7. Grandfathered migrations

Migrations up to and including `20260914165542` shipped before the lint, and
the lint does not read them. Some shipped migrations after the cutoff break a
rule. They cannot change, because a database may have applied them already.
The `GRANDFATHERED` list in the lint names each one, with the rule and the
reason. `20260915231809` is the first: it rebuilds a unique index on
`harvest_workflow_executions` inside its transaction.

The list cannot grow. An entry newer than the newest migration at the time the
lint landed fails the build, so a new migration uses an annotation. An entry
that no longer matches a finding also fails the build.

## 8. Known limits

- The lint does not see SQL that `EXECUTE` builds from a string.
- The lint scans a dollar-quoted string as code. A statement inside one can
  fail the lint. That error is on the safe side.
- Every `ALTER TABLE` form counts as a blocking lock. Some forms, such as
  `VALIDATE CONSTRAINT`, take a weaker lock. Set the timeout anyway.
- The lint does not check the size of the timeout. Keep it near `5s`.

## Related

- [`upgrading/0.7.0.md`](0.7.0.md) — the current upgrade guide.
- [`upgrading/0.5.0.md`](0.5.0.md) — the migration inventory, with the
  out-of-band index recipes.
- [`partitioned-events.md`](../partitioned-events.md) — index builds on the
  partitioned `harvest_events` layout.
