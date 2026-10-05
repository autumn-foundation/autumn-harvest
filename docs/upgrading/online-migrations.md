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

The engine reads or writes these tables on every task claim or workflow step.
A waiting `ACCESS EXCLUSIVE` lock on any of them stalls the fleet.

- `harvest_task_queue`, `harvest_events`, `harvest_workflow_executions`,
  `harvest_timers` and `harvest_signals`: the workflow step path.
- `harvest_workers`, `harvest_queue_pauses`, `harvest_activity_pauses`,
  `harvest_shard_generation` and `harvest_rate_limit_buckets`: every claim
  reads them.
- `harvest_audit_log`: each operator action writes it.
- `harvest_workflow_outbox`: the application writes it in its own
  transactions, in the application database.

A table that the same migration creates is not hot after its `CREATE TABLE`.
No session can hold a lock on it yet. The exemption needs a create that surely
runs: not `IF NOT EXISTS`, not inside a branch, and not in a function body.
A temporary table never counts, because a commit or the session end can drop
it. The exemption ends at a later `DROP TABLE`, `RENAME TO`, `SET SCHEMA`,
`DROP SCHEMA`, `DROP OWNED` or `ROLLBACK`, because the name can mean the hot
table again.
A drop or rename ends it with or without a schema in the name. A `search_path`
change ends it for a name without a schema. A `COMMIT` ends every exemption,
because other sessions can then see the new table and lock it. So does a call
of a routine from this file or an earlier migration, or code the lint cannot
read, because it may drop the table.
The lock must name the table exactly as the create does, schema included.

A partition of a hot table is hot too, because it takes the live writes of its
parent. An inheritance child of a hot table is hot for the same reason. A
parent of a hot table is hot too, because its DDL recurses to the child. The
lint learns each link from `PARTITION OF`, `ATTACH PARTITION`, `INHERIT` or
`INHERITS` in an earlier migration. A detach or a drop does not make a table
cold again. The
partition manager creates `harvest_events_p_*` and `harvest_events_legacy` at
run time, so the lint treats those names as hot.

## 2. The rules

| Rule | Fails when | Fix |
|---|---|---|
| `lock-timeout` | A statement takes a blocking lock on a hot table, and no non-zero `lock_timeout` is in force. | Put `SET LOCAL lock_timeout = '5s';` first. |
| `blocking-index` | A plain `CREATE INDEX`, `DROP INDEX` or `REINDEX` touches a hot table. So does `ALTER TABLE ... ADD` of a `UNIQUE`, `PRIMARY KEY` or `EXCLUDE` constraint without `USING INDEX` and an index name. `USING INDEX TABLESPACE` still builds an index. | Use `CONCURRENTLY` (section 4), or the guarded build (section 5). On `harvest_events`, `CONCURRENTLY` also counts, because the partitioned layout cannot run it. |
| `concurrently-in-transaction` | `CONCURRENTLY` runs in a transaction, shares its file with another statement, or sits in a `DO` block. | Put it alone in a file with `run_in_transaction = false` (section 4). |
| `bad-annotation` | A `-- lock-safety:` comment does not parse. | Fix the annotation (section 6). |
| `unused-annotation` | An annotation allows a rule that the statement below it does not break. | Remove the annotation. |

These statements take a blocking lock for `lock-timeout`:

- `ALTER TABLE`, in every form;
- `LOCK TABLE`, `DROP TABLE`, `TRUNCATE`, `CLUSTER` and `VACUUM FULL`.
  `CASCADE` also locks every table whose foreign key reaches the target, so
  it counts as a lock on an unknown table;
- `CREATE`, `ALTER` and `DROP TRIGGER`; `CREATE`, `ALTER` and `DROP POLICY`;
  `CREATE` and `DROP RULE`; `ALTER INDEX`;
- `REFERENCES` on a hot table, in a new table or a new constraint. A foreign key
  takes `SHARE ROW EXCLUSIVE` on the table it references;
- `DROP TABLE`, or `ALTER TABLE ... DROP CONSTRAINT`, on a table with a foreign
  key to a hot table. Postgres drops the key's triggers on the hot table too,
  under `ACCESS EXCLUSIVE`. `DROP COLUMN` and a column type change count too,
  because they drop or rebuild the key;
- `CREATE TABLE ... PARTITION OF` a hot table, which locks the parent;
- a plain `CREATE INDEX` (`SHARE`), `DROP INDEX` (`ACCESS EXCLUSIVE`) or
  `REINDEX`.

The lint reads the history of earlier migrations. It finds the table of a
`DROP INDEX` from the migration that created the index. The index sits in the
schema of its table. A name without a schema goes to the first schema of the
connection's `search_path`, which may not be `public`. So it matches only a
name without a schema, never `public.<name>`. A
`DROP INDEX` or `DROP TABLE` that surely runs makes the lint forget the index.
After a `search_path` change, an unqualified name has no known schema, so the
lint neither learns nor places it. A call of a routine whose body may change
`search_path` counts as a change when the call returns. That holds for a
routine from an earlier migration too. So does a body that runs code the lint
cannot read. The `"$user"` entry of the path follows the current role, so
`SET ROLE`, `SET SESSION AUTHORIZATION` and their resets count as a change.
It finds the foreign keys of a table from the migrations that added them. An
index that no migration creates counts as hot. So does a `REINDEX` of a schema,
a database or the system catalogs.

## 3. Bound the lock wait

```sql
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_workflow_executions
    ADD COLUMN IF NOT EXISTS example_note TEXT NULL;
```

Every lock on a hot table needs a bound in force when it runs. A timeout set
after the lock does not count. Each migration sets its own bound. A session
value from an earlier migration does not count, because a migration may run
alone on a new connection. A later `RESET lock_timeout`, `RESET ALL` or
zero value ends the bound for the locks after it, and so does a
`ROLLBACK TO SAVEPOINT`. `0` turns the timeout off, and so does a value under
1 ms, because Postgres rounds it to 0.
`DEFAULT` restores the server default, which is usually `0`, so the lint does
not accept it. `SET lock_timeout` must be its own statement. `ALTER ROLE ...
SET lock_timeout` does not change the current session. Inside a `DO` block,
`PERFORM pg_catalog.set_config('lock_timeout', '5s', true)` also counts. It
must be a bare `SELECT` or `PERFORM` of the call, with the `pg_catalog`
schema. The connection may start with a `search_path` that lists another
schema before `pg_catalog`, so an unqualified call may reach a user function.
The bound starts after the call, so it does not cover a lock that one of its
arguments takes. A query with a filter may never call
the function, so it does not count. A `set_config` in a schema other than
`pg_catalog` is a user function, so it ends the bound. The same call in a
function body does not count for the migration, because the body runs only
when something calls the function. That includes a `BEGIN ATOMIC` body and the
`RETURN` body of a SQL function. A lock in a function body needs its own
bound earlier in the body, because the migration's bound may not hold when
the function runs. A bound in a nested routine counts for that routine only. A `SET lock_timeout` clause in `CREATE FUNCTION` or
`CREATE PROCEDURE` also counts, because Postgres applies it on each call.
An `ALTER FUNCTION`, `ALTER PROCEDURE` or `ALTER ROUTINE` that resets or clears
that setting counts as a lock that no bound covers, because each later call
runs the body without it.
A clear in a routine body can outlive the call. So a call ends the bound
when the routine may clear it. A call reaches a body in the same file only
when an earlier `CREATE` has the same name, schema included, and the same
number of parameters. The lint does not compare parameter types, so a call
also ends the bound when any such `CREATE` may clear it. For the same reason,
a call never reaches a body in this file when an earlier migration created a
locking routine of that name. Any other `CALL` also
ends the bound, because the lint cannot read the body it reaches. Such a
`CALL`, and any call of a locking routine from an earlier migration, also
counts as a lock on an unknown table. A bound covers that lock only when the
routine is known to lock and known not to clear. The call must also name it
with the same schema and number of parameters. Otherwise the body may clear
the bound before it locks, so no bound covers the call. A routine that calls a
locking routine counts as a locking routine too. An `ALTER` that sets a
routine's timeout to a value that does not bound makes it a clearing routine.
A rename or a schema move carries what the lint knows to the new name, and no
outside bound covers a call by either name.
A call of a clearing routine that an earlier migration created ends the bound
too. Set the bound again after the call.

A routine can bound its own locks with a `SET lock_timeout` clause, or with a
setter that surely runs before each lock in its body. Postgres applies that
bound on each call. So a later call of that routine needs no outside bound.
The call must use the same schema and an accepted number of arguments. A
`VARIADIC` routine accepts any number of trailing arguments. The
routine must also call only routines of its file that bound their own locks.
An annotation does not make a lock bounded. Any other definition of the same
name, or any `ALTER` of it, removes the exemption for good. Code that the lint
cannot read removes it too, because that code may replace the routine.
The lint knows only the routines that migrations create. A routine of the same
name and arity that something else created may be the one that a call reaches.
A routine created in an uncalled body or in a branch may not exist. A later
`ROLLBACK` in the file may undo its `CREATE`. A call that runs now never
reaches such a routine, and later calls gain nothing from it.

An unqualified call after a `search_path` change, or in the body of a routine
with a `SET search_path` clause, ends the bound.
It may reach a routine in another schema. A session change
outlives its file, so the rule holds for every later migration too. A
top-level `SET LOCAL` change ends with its transaction, so it does not.

A change in the expression of an `EXECUTE` takes effect before the SQL runs.
A setter inside an `IF`, `CASE` or `LOOP`, or after a `RETURN`, `EXIT` or
`CONTINUE`, does not count, because it may not run. Nothing in a block with an
`EXCEPTION` handler counts, because the handler rolls the block back. A clear
inside a branch does count, because the branch may run.

`5s` is the bound that the existing lock-taking migrations use. When the
timeout fires, the migration fails and rolls back. Run it again.

Diesel and `harvest migrate` send each `up.sql` as one batch. Postgres runs a
multi-statement batch as one implicit transaction. `SET LOCAL` therefore holds
for the whole file, even with `run_in_transaction = false`. An explicit `COMMIT`,
`ROLLBACK` or `END` ends that transaction and its `SET LOCAL`. A `ROLLBACK` also
undoes a plain `SET` made since the transaction began. After a `COMMIT` or
`ROLLBACK`, the next statement opens a new implicit transaction. A `BEGIN`
takes over the open transaction, so it does not protect an earlier `SET` from
a later `ROLLBACK`. A `COMMIT` or `ROLLBACK` in a branch may not run, so it
never saves or restores a bound. After a `COMMIT` in a branch, a bound holds
only when it holds with the commit and without it.

## 4. Build indexes with `CONCURRENTLY`

`CREATE INDEX CONCURRENTLY` and `DROP INDEX CONCURRENTLY` take
`SHARE UPDATE EXCLUSIVE`. Reads and writes continue during the build. Postgres
rejects both inside a transaction block, a function or a `DO` block, so the
migration must opt out of Diesel's transaction.

`metadata.toml`, beside `up.sql`:

```toml
run_in_transaction = false
```

`up.sql`:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_tq_example
    ON harvest_task_queue (queue_name, scheduled_at);
```

The `CONCURRENTLY` statement must be the only statement in the file. Diesel
and `harvest migrate` send the file as one batch, and a batch of two or more
statements runs as one implicit transaction. A `SET lock_timeout` beside it
therefore breaks the migration. The lint flags it. `SHARE UPDATE EXCLUSIVE`
does not block reads or writes, so this statement needs no `lock_timeout`.

**`harvest_events` is different.** The opt-in partitioned layout turns it
into a partitioned parent. Postgres does not build or drop an index
`CONCURRENTLY` on a partitioned parent, so the lint flags both forms there as
`blocking-index`. Build the index on each partition first, then on the parent.
A partition created with `PARTITION BY` is a partitioned table too, and the
lint flags it the same way. `docs/partitioned-events.md` and
`20260905181020_harvest_usage_activity_lookback_index/up.sql` give the recipe.
Then annotate the statement. The lint also flags a concurrent `DROP INDEX` of
an index it cannot place, because that index may sit on the parent.

A failed concurrent build leaves an `INVALID` index. `IF NOT EXISTS` then
skips the build, and the invalid index stays. Before a retry, run
`DROP INDEX CONCURRENTLY IF EXISTS <index>` as a statement of its own.

**Limit.** `autumn_harvest::test_init_sql()` sends every `up.sql` in one
`batch_execute`. Postgres runs that call as one implicit transaction, so it
rejects `CONCURRENTLY`. A `run_in_transaction = false` migration therefore
also needs a change to that test bundle. Until the bundle supports it, use the
guarded build in section 5.

## 5. The guarded build

This is the pattern that most index migrations in this tree use. Operators
with a large table prebuild the index out of band with
`CREATE INDEX CONCURRENTLY`. The migration builds the index only when it is
absent. It fails when the existing index is `INVALID`, which is what a failed
concurrent build leaves.

A new guarded build bounds its lock wait and annotates the plain build. This
short form checks validity only:

```sql
SET LOCAL lock_timeout = '5s';

DO $$
DECLARE
    valid boolean;
BEGIN
    SELECT i.indisvalid INTO valid
      FROM pg_index i
     WHERE i.indexrelid = to_regclass('idx_harvest_events_example');
    IF valid IS NULL THEN
        -- lock-safety: allow blocking-index #1234 operators prebuild it CONCURRENTLY
        CREATE INDEX idx_harvest_events_example
            ON harvest_events (workflow_exec_id, timestamp);
    ELSIF NOT valid THEN
        RAISE EXCEPTION 'idx_harvest_events_example is INVALID. Drop it and rebuild it.';
    END IF;
END $$;
```

A real migration also compares the definition, so that an unrelated index
with the same name fails the migration.
`20261001192155_harvest_quota_reconcile_name_id_index/up.sql` shows that
check.

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
the lint does not read them. Only the ones on disk when the lint landed are
exempt, and `lock_safety_legacy.txt` lists them. A new migration with an older
version is still linted. Some shipped migrations after the cutoff break a
rule. They cannot change, because a database may have applied them already.
The `GRANDFATHERED` list in the lint names each one, with the rule and the
reason. `20260915231809` is the first: it rebuilds a unique index on
`harvest_workflow_executions` inside its transaction.

The list cannot grow. A digest pins its entries, so any new entry fails the
build, and a new migration uses an annotation. An entry newer than the newest
migration at the time the lint landed fails too. An entry that no longer
matches a finding also fails the build. An entry covers every finding of its
rule in its file, so a digest pins the SQL of each listed migration as well.

## 8. Known limits

- In a PL/pgSQL body, the lint scans the constant SQL that `EXECUTE` runs.
  That includes a `format()` template, where each placeholder is an unknown
  name. The lint cannot read SQL built with `||` or held in a variable, so
  such an `EXECUTE` counts as a lock on an unknown table. So does a template
  with `%s` anywhere in its text, even in a comment or a quoted name. So does
  a `%L` value that a `DO`, `EXECUTE` or function body runs as code.
- The lint scans a `DO` body, a function body and the SQL that `EXECUTE` runs
  as code, in any quote form. That includes `FOR ... IN EXECUTE`,
  `RETURN QUERY EXECUTE` and `OPEN ... FOR EXECUTE`. Any other string is data.
- The lint reads string literals as `standard_conforming_strings = on` does.
  A statement that may turn the setting off counts as a lock on an unknown
  table that no bound covers. That includes a `set_config` call whose name is
  not one literal. While it is off, each statement with a
  backslash counts the same way, later in the file and in later migrations,
  until a reset that surely runs. A reset in a function body or in a branch
  does not count. Nor does a `set_config` that may not be the built-in. A `ROLLBACK` restores the value from the start of its
  transaction. A top-level `SET LOCAL` ends at the commit. A local value in a
  routine body carries, because the body may run in any later transaction.
- The lint reads only PL/pgSQL and SQL. A `DO` body in another language, or a
  call of a routine in another language, counts as a lock on an unknown table.
  No bound covers that lock, because the code may clear the bound first. It
  also ends the bound for what comes after it. Text in such a body never sets
  a bound or makes a table new. The lint does not read the body of a routine
  in another language as SQL, because Postgres only stores it. Avoid such code
  in a migration.
- Code that the lint cannot read may change `search_path` or turn
  `standard_conforming_strings` off. The lint assumes so for the rest of that
  file, but not for later migrations. Otherwise one such call would leave
  every later migration unreadable.
- Every `ALTER TABLE` form counts as a blocking lock. Some forms, such as
  `VALIDATE CONSTRAINT`, take a weaker lock. Set the timeout anyway.
- The lint does not check the size of the timeout. Keep it near `5s`.
- `lock_timeout` bounds the wait for a lock, not the time the lock is held.
  `ADD CHECK` without `NOT VALID`, `SET NOT NULL` and a column type change
  hold `ACCESS EXCLUSIVE` for a full scan or rewrite. Add a constraint as
  `NOT VALID`, then run `VALIDATE CONSTRAINT`, as `20260902131705` does.

## Related

- [`upgrading/0.7.0.md`](0.7.0.md) — the current upgrade guide.
- [`upgrading/0.5.0.md`](0.5.0.md) — the migration inventory, with the
  out-of-band index recipes.
- [`partitioned-events.md`](../partitioned-events.md) — index builds on the
  partitioned `harvest_events` layout.
