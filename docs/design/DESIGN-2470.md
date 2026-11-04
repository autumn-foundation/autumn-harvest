# Design — Issue #2470: transactional outbox on a SQLite app

Part of epic [#2466](https://github.com/autumn-foundation/autumn-harvest/issues/2466).
After slice 1 ([#2467](https://github.com/autumn-foundation/autumn-harvest/issues/2467)).

On Postgres, app code writes a workflow start to `harvest_workflow_outbox`
in its own business transaction. A relay then starts the run. A SQLite app
had no outbox table and no relay. This issue adds both, and a writer for
app code.

## Acceptance

- **AC1.** A start written in a rolled-back business transaction never runs.
- **AC2.** A committed start runs exactly once, across a crash between the
  commit and the relay.

---

## 0. Planning record

### 0.1 Facts found before the plan

- The Postgres outbox is `outbox.rs`. Its relay claims rows with
  `FOR UPDATE SKIP LOCKED`, starts each one with `start_or_load`, and marks
  it delivered. A failed start gets a capped exponential backoff.
- Under `sqlite`, `autumn_web::db::RuntimeConnection` is
  `SyncConnectionWrapper<SqliteConnection>`. App code holds one in its
  business transaction.
- `autumn-web`'s own outbox (`autumn_web::outbox`) creates its SQLite table
  at boot with `CREATE TABLE IF NOT EXISTS`. Its times are epoch
  milliseconds. It registers no migration set.
- `autumn-web` exits at boot when an in-memory app database has a
  registered migration set. Slice 1 supports an in-memory app database with
  `sqlite_path`.
- Before autumn#3536, a registered migration set reaches every fleet
  database too, not only the control database.
- `TestApp` applies no plugin migration set.
- One driver task owns the `SqliteRuntime`. It holds an exclusive writer
  lock on the Harvest file, so one process runs Harvest for one file.
- `start_workflow_with_options` with the default reuse policy returns the
  existing run of a key. A sealed prior (`TERMINATED`, `CONTINUED_AS_NEW`)
  is invisible to it, so a second start over a sealed prior runs again.
- `list_executions` with a name and a workflow id, and no state, returns
  every run of the key, newest first.
- `HarvestOutboxConfig` (`harvest.outbox.*`) is loaded on both backends.

### 0.2 Brainstorm — where does the table come from?

| # | Idea | Verdict |
|---|------|---------|
| S1 | A SQLite migration set, registered with `plugin_migrations`. | Rejected. It reaches every fleet database before autumn#3536. It makes an in-memory app database exit at boot. `TestApp` does not apply it. |
| S2 | `CREATE TABLE IF NOT EXISTS` at boot, on the primary pool. | **Adopted.** It follows `autumn_web::outbox::ensure_schema`. It reaches the control database only. |
| S3 | Reuse `autumn_web::outbox` with a Harvest topic. | Rejected. It needs `outbox.enabled` in the app config, and a second relay. The Postgres table, writer and config names would differ. |
| S4 | Put the outbox table in the Harvest file. | Rejected. The Harvest file is not in the business transaction. |

### 0.3 Brainstorm — how does the relay start a run once?

| # | Idea | Verdict |
|---|------|---------|
| R1 | Start with the default reuse policy, as on Postgres. | Rejected. A redelivery over a sealed prior runs again. |
| R2 | A driver command `StartOnce`: return any run of the key, else start. | **Adopted.** The driver runs one command at a time, so the check and the start are atomic. |
| R3 | Claim rows with an owner and a TTL, as on Postgres. | Rejected. One process holds the Harvest lock. A claim TTL would delay the redelivery after a crash. |
| R4 | One drain at a time in the process, behind a mutex. | **Adopted.** The relay and an explicit flush cannot dispatch one row twice. |
| R5 | Mark a row only while `delivered_at IS NULL`. | **Adopted.** A delivery counts once. |

### 0.4 Reverse brainstorm — how could this lose or double a start?

| # | Failure | Guard |
|---|---------|-------|
| F1 | The writer commits outside the business transaction. | It takes the caller's connection and opens no transaction. |
| F2 | A crash after the commit and before the relay. | The row stays undelivered. The next boot drains it. |
| F3 | A crash after the start and before the mark. | `StartOnce` returns the existing run. The relay marks it. |
| F4 | A redelivery over a terminated run starts it again. | `StartOnce` matches a run in any state. |
| F5 | Two drains in one process dispatch one row. | A mutex serializes drains. The mark guard counts it once. |
| F6 | App code writes the row in a fleet tenant database. | That database has no table, so the write fails. |
| F7 | A row that can never start blocks the queue. | It retries with a capped backoff and `last_error`, as on Postgres. Rows behind it still drain. |
| F8 | A queue name or search attribute that SQLite cannot honour. | The writer refuses it, so the row never waits for a retry that cannot pass. |
| F9 | Shutdown drops the writer lock while the relay dispatches. | The shutdown hook stops the relay before the driver. A `Stopped` reply ends the drain. |
| F10 | The relay spins on a broken table. | It logs once per change, and waits for the next tick. |

### 0.5 Six thinking hats

- **White (facts).** See 0.1. The Postgres contract is the reference: the
  same table name, request type, config and flush functions.
- **Red (feel).** Exactly-once claims make people nervous. The proof must
  be a test that crashes in each window, not an argument.
- **Black (risk).** S2 bypasses the migration policy of the app. The DDL is
  idempotent, and `autumn_web::outbox` does the same. A future column needs
  an `ALTER TABLE` guarded by a column check.
- **Yellow (value).** App code on SQLite gets the Postgres outbox pattern.
  `flush_workflow_start_outbox` works on both backends.
- **Green (ideas).** A per-tenant outbox in each fleet database, relayed to
  that tenant's Harvest. Deferred to autumn#3536 and a later issue.
- **Blue (process).** RED: tests for AC1, AC2 and each refusal. GREEN: the
  schema, the writer, `StartOnce` and the relay. REFACTOR: share the
  backoff with Postgres.

---

## 1. Design

### 1.1 Table

`sqlite::outbox::SCHEMA_SQL` creates `harvest_workflow_outbox` in the app's
primary database at boot. The columns follow Postgres. Times are epoch
milliseconds. There is no claim column (R3).

### 1.2 Writer

`sqlite::enqueue_workflow_start_outbox(conn, &request)` inserts one row on
the caller's connection. A duplicate `(workflow_name, workflow_id)` is a
no-op, as on Postgres. It refuses:

- an empty `workflow_id`, as on Postgres;
- a `queue_name` other than `default`: SQLite has one queue;
- a `search_attrs` that is not an object of JSON primitives;
- a database with no outbox table, with a message that names this issue.

### 1.3 Relay

The startup hook creates the table, then spawns the relay. Each tick of
`harvest.outbox.poll_interval_ms` drains the due rows in batches of
`batch_size`. Each row goes to the driver as `StartOnce`. A start marks the
row delivered with its execution id. A failure sets `last_error` and the
next attempt time. `claim_ttl_ms` has no effect on SQLite.

`flush_workflow_start_outbox` and `drain_workflow_start_outbox_once` drain
the SQLite outbox when the app runs Harvest on SQLite.

### 1.4 Shutdown

The shutdown hook cancels the relay and waits for it, then stops the
driver.

## 2. Out of scope

- An outbox in each fleet tenant database (autumn#3536).
- The `harvest.admission.bypassed` metric. The SQLite backend has no
  admission gate.
