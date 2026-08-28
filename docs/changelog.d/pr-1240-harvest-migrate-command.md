## Phase — `harvest migrate` for split/external Harvest databases (issue #1240)

Since 0.6.0 Autumn owns Harvest's migrations — but only the ones that live in
the **application** database. Under `harvest.mode = "split"` / `"external"`
Harvest storage is a separate database `plugin_migrations` cannot reach, and
outside the `dev` profile the plugin only *warns* about pending migrations
there. The documented remedy was a checked-out workspace plus the `diesel` CLI:

```bash
diesel migration run --database-url "$HARVEST_DATABASE_URL" \
  --migration-dir autumn-harvest/migrations
```

That is now a first-class command:

```bash
harvest migrate status [--check]      # read-only; --check gates a deploy
harvest migrate run    [--dry-run]    # apply every pending migration
```

**`autumn_harvest::migrate` (new, `db`-gated)** is the library half. `build.rs`
already emitted the concatenated `full_migrations_sql()` bundle for
testcontainers fixtures; it now also emits the same `up.sql` files kept apart
per migration, so a *pending subset* can be applied against a live database
rather than only a whole schema against an empty one. Both artifacts come from
the one `migrations/` listing, and a test pins their bodies to each other, so
neither can drift from `MIGRATIONS`.

Deliberately bug-for-bug compatible with `diesel_migrations`, because Autumn,
the plugin's startup path, and this command all write the same database: the
ledger is `__diesel_schema_migrations`, the version is the name's leading
component, and application is in ascending version order. A migration's
`metadata.toml` is honoured too — `run_in_transaction = false` (what a `CREATE
INDEX CONCURRENTLY` migration needs, since Postgres rejects that statement
inside a transaction block) applies the body without one, and the ledger row
then goes in *after* the body rather than before it, because a version recorded
for a migration that never finished is the one state no later run can repair.
The one key is read by a strict hand parser that refuses anything ambiguous
rather than pulling a TOML crate into the engine core; `build.rs` emits each
migration's metadata text so the embedded set and an `--include-dir` set go
through the same parser. No `down.sql` — a rollback under a live fleet is an
operator decision with data loss attached.

Two details worth naming:

- **The ledger row is inserted first**, inside each migration's transaction and
  before its DDL. A second migrator racing on the same database therefore blocks
  on that row rather than part-way through the schema change, and on commit sees
  a unique violation and skips the migration as already applied
  (`applied_concurrently` in the report) instead of replaying DDL. No advisory
  lock, so no new key in a keyspace shared with the claim path, `mutex`,
  `admission_gate` and the scheduler.
- **`status` never writes** — not even `CREATE TABLE IF NOT EXISTS` for the
  ledger. It probes with `to_regclass` and reports a ledger-less database as
  "never migrated", so the gate is safe to point at a database you are only
  inspecting.

A version recorded in the ledger that the binary does not know is *reported*,
never removed (`unrecognized`): the usual cause is a newer build having already
migrated the database, but it equally catches a DSN pointed at the wrong one. It
never fails the `--check` gate on its own. A duplicate version *across* supplied
sets is refused up front, before any connection: Autumn can resolve a collision
with a substitute version because it sees every plugin's set at once, and the
honest answer here — where one migration would be recorded and never run — is to
refuse.

Harvest's own set is embedded in the binary; sets that are not (the plugin's
connector dead-letter table, an application's own) are added with
`--include-dir`. `--database-url` repeats once per shard database, and a failing
target stops the run rather than leaving later shards migrated behind a database
that already failed. Failure messages carry the redacted DSN, never the
credential — this runs in deploy pipelines whose logs are widely readable.

Also fixed: the plugin's non-`dev` warning on a dedicated Harvest database said
"Run `autumn migrate`", the one command that cannot apply those migrations
(issue #1240's root cause). It now names `harvest migrate run`. The connector
chapter, operations guide, sharding runbook and 0.6.0 upgrade guide all point at
the new command.

Tests: 23 unit tests in `src/migrate.rs` (version extraction, plan
classification, directory reading, embedded-set/bundle agreement), 17 CLI tests
(argument mapping, `--include-dir` loading and collision refusal, text/JSON
rendering, the `--check` gate's counts and exit code, DSN redaction), and a new
Docker-backed suite `migrate_tests` (fresh-database apply, idempotent re-run,
`plan` writing nothing, an extra set applying alongside the embedded one, a
failing migration rolling back with its ledger row and the run resuming after a
fix, a `CREATE INDEX CONCURRENTLY` migration applying under
`run_in_transaction = false` and a failing one staying unrecorded, and an
unrecognized ledger row surviving). No new `WorkflowEvent` variant, no
migration.
