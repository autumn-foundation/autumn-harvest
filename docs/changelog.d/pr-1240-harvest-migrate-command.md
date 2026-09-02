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
The file is read with the same `toml` crate Diesel reads it with, so what this
applies and what `diesel migration run` applies can never disagree — rejections
included. A hand-rolled subset was tried first and leaked five separate ways
(an unknown key carrying an invalid value, a duplicate key, a line that is not
`key = value`, an unbalanced quoted key, non-ASCII whitespace), each of them a
file Diesel refuses and it accepted; `toml` is already in the workspace
lockfile via the plugin, so the graph gains an edge rather than a package.
`build.rs` emits each migration's metadata text so the embedded set and an
`--include-dir` set go through the same parser. No `down.sql` — a rollback under a live fleet is an
operator decision with data loss attached.

Two details worth naming:

- **The body runs before its ledger row is written**, which is Diesel's order
  and not merely a preference: a migration body may consult
  `__diesel_schema_migrations` and condition its DDL on its own version being
  absent. Record the version first and that body reads its own row through the
  transaction's own writes, skips the DDL, and still commits the version —
  an incomplete schema permanently marked applied, from a file
  `diesel migration run` applies correctly. So the ledger row is *not* what
  serializes concurrent migrators. A `SHARE UPDATE EXCLUSIVE` lock on the ledger
  table is: it conflicts with itself, so the second migrator waits before any
  DDL rather than part-way through the schema change. Holding it, the ledger is
  re-checked before the body runs, and a version already there is reported as
  `applied_concurrently` instead of replaying DDL. That check is a `SELECT`, so
  "already applied" is decided outright rather than inferred from a unique
  violation the body's own SQL could equally have raised. Still no advisory
  lock, so no new key in a keyspace shared with the claim path, `mutex`,
  `admission_gate` and the scheduler.

  The mode is picked for what it does *not* block. Plain readers, so `status` is
  unaffected — and `ROW EXCLUSIVE`, so it never blocks another migrator's ledger
  insert. That second one is the load-bearing part: Diesel takes no ledger lock
  and inserts *after* the body, so it locks the schema object first and the
  ledger second — the opposite order to this. `SHARE ROW EXCLUSIVE`, the obvious
  first choice, blocks `ROW EXCLUSIVE` and would close that cycle, letting
  Postgres abort one side and report a failed migration while the competing
  migrator was applying it correctly. What that choice gives up is real but
  smaller: a migrator that does not take this lock is not serialized by it, so
  racing Diesel can still mean both run a body and one fails on its own DDL —
  which is what Diesel-versus-Diesel already does. The guarantee is over
  concurrent `harvest migrate` runs, and it is not worth turning a survivable
  race into a deadlock to widen it.

  The lock is also **best-effort**, for a reason worth an operator's attention.
  Postgres requires `UPDATE`, `DELETE` or `TRUNCATE` on a table to lock it in
  any mode above `ROW EXCLUSIVE`, while Diesel's bookkeeping — read the ledger,
  insert a row — needs only `SELECT` and `INSERT`. A least-privilege role
  granted exactly those two on a ledger it does not own would be denied the
  lock before any migration body ran. Refusing a migration set that
  `diesel migration run` applies for that role, mid-deploy, is the worse
  outcome, so the run probes the privilege once and proceeds unlocked —
  Diesel's behaviour exactly. It is reported, not just logged — the
  `harvest` binary installs no tracing subscriber, so a warning logged through
  `tracing` alone would reach nobody.

  The report names what actually ran unserialized rather than asserting a
  blanket state. Two fields, in the text output and under `--format json`:

  | field | meaning |
  | --- | --- |
  | `applied_unserialized` | `{name, reason}` per migration applied without the lock held |
  | `ledger_lock_available` | whether the role could have taken the lock at all |

  The reason is **per migration**, not per run, because one run can contain
  both and they take different remedies:

  | `reason` | cause | remedy |
  | --- | --- | --- |
  | `ledger_lock_unavailable` | the role lacks `UPDATE`/`DELETE`/`TRUNCATE` on the ledger | grant one of them, or run migrators one at a time |
  | `no_transaction` | the migration declares `run_in_transaction = false` | no grant helps — run migrators one at a time |

  A run by an unprivileged role that also contains a non-transactional
  migration carries both reasons, and `harvest migrate run` prints each
  migration with its own: attributing the whole list to the missing grant would
  promise that granting it makes the run safe, which for the second kind is
  false. A body that ran unlocked and *then* lost the ledger insert to a
  concurrent migrator appears in both `applied_concurrently` and
  `applied_unserialized`, because the DDL really did execute here.
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

Connections are the CLI's own, not `AsyncPgConnection::establish`: that is
`NoTls` and could not reach a managed Harvest database at all, which is the
production shape this command exists for. The CLI builds the same
rustls-backed connector autumn-web's migration path uses (already in the
workspace lockfile at the same versions, so the graph is unified rather than
widened) and calls the module's `*_on_connection` entry points, keeping a
rustls stack out of the engine core. TLS is always verified — chain and
hostname — which is stricter than libpq's `require`; `sslmode=verify-ca` /
`verify-full` are rewritten to `require` for tokio-postgres, which fails to
*parse* those values, and that is not a downgrade because the connector
verifies unconditionally.

Harvest's own set is embedded in the binary; sets that are not (the plugin's
connector dead-letter table, an application's own) are added with
`--include-dir`. `--database-url` repeats once per shard database, and a failing
target stops the run rather than leaving later shards migrated behind a database
that already failed. Failure messages carry the redacted DSN, never the
credential — this runs in deploy pipelines whose logs are widely readable.

A failure never loses what it did. `apply_to_connection` returns a
`PartialMigration` pairing the error with everything committed before it —
migrations commit one at a time, so a failure at the fourth of six leaves three
applied — and the CLI prints that alongside the targets already finished, in
whichever format was asked for. Connection failures take the same path, because
an unreachable third shard says nothing about the two behind it.

Also fixed: the plugin's non-`dev` warning on a dedicated Harvest database said
"Run `autumn migrate`", the one command that cannot apply those migrations
(issue #1240's root cause). It now names `harvest migrate run`. The connector
chapter, operations guide, sharding runbook and 0.6.0 upgrade guide all point at
the new command.

Tests: 26 unit tests in `src/migrate.rs` (version extraction — including a
multibyte prefix that fits `VARCHAR(50)` in characters though not in bytes —
plan classification, directory reading, embedded-set/bundle agreement, and the
metadata parser's parity with Diesel: what it accepts, and eleven files it
refuses), 38 CLI tests (argument mapping, `--include-dir` loading and collision
refusal, text/JSON rendering including partial and setup failures, the `--check`
gate's counts, exit code and remedy, DSN redaction and target labelling, and
`sslmode` normalization over both DSN forms with quoting and escapes), and a new
Docker-backed suite `migrate_tests` of 7 tests (fresh-database apply, idempotent
re-run, `plan` writing nothing, an extra set applying alongside the embedded one,
a failing migration rolling back with its ledger row and the run resuming after a
fix, a self-conflicting body failing rather than skipping, a `CREATE INDEX
CONCURRENTLY` migration applying under `run_in_transaction = false` and a failing
one staying unrecorded, and an unrecognized ledger row surviving). No new
`WorkflowEvent` variant, no migration.
