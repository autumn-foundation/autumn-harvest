## Plugin — SQLite app: transactional outbox (issue #2470, epic #2466)

A SQLite app in embedded mode gets the transactional outbox of the Postgres
path. The Postgres default build does not change. The design record is
`docs/design/DESIGN-2470.md`.

- **Writer.** `sqlite::enqueue_workflow_start_outbox(conn, &request)` takes
  the `WorkflowStartRequest` of Postgres. It writes on the caller's
  connection, so the start commits or rolls back with the business rows. A
  duplicate key is a no-op. It refuses an empty or blank `workflow_id`, a
  `queue_name` other than `"default"`, `search_attrs` that is not an object
  of JSON primitives, and a database with no outbox table.
- **Table.** The startup hook creates `harvest_workflow_outbox` in the app's
  primary database with `CREATE TABLE IF NOT EXISTS`, as
  `autumn_web::outbox` does. In a fleet app, only the control database gets
  it, so autumn#3536 is not needed. Times are epoch milliseconds. There is no
  claim column. `sqlite::OUTBOX_SCHEMA_SQL` holds the DDL. A partial index
  holds the undelivered rows. The boot compiles each statement of the writer
  and the relay with `EXPLAIN`, so a table of another shape fails the boot.
- **Relay.** It runs on the `harvest.outbox.*` settings. Each row goes to the
  driver as the new `StartOnce` command, which returns any run of the key,
  in any state, before it starts one. The new
  `SqliteRuntime::newest_run_of_key` answers that from the key index. This
  is stricter than the Postgres reuse policy over a sealed run. A mutex keeps two drains in one
  process apart. The shutdown hook stops the relay before the driver.
  `flush_workflow_start_outbox` and `drain_workflow_start_outbox_once`
  drain the SQLite outbox. A call from an activity or a workflow is refused,
  because it would wait for its own driver task. A driver that stops outside
  shutdown ends the relay with a warning. `claim_ttl_ms` has no effect on
  SQLite.
- **Split and external mode.** The boot warns when the app database holds
  undelivered starts that no relay drains.
- **Shared backoff.** `outbox::retry_delay_for_attempts` serves both
  backends.
- **No new `WorkflowEvent` variant. No migration.**

Tests: `tests/sqlite_plugin/outbox.rs` (in the `sqlite_plugin` suite) proves
AC1 with a rolled-back business transaction, and AC2 with a crash before
the relay and a crash between the start and the mark. It also covers a
redelivery over a terminated run, a failed start with backoff, a duplicate
enqueue and an app with no database. `tests/sqlite_plugin_fleet.rs` runs a
start on the control Harvest and refuses a write on a tenant database. Unit
tests in `sqlite::outbox` drive the relay with no background task: a real
start whose mark fails, then a redelivery over the terminated run, the
`drain_once` limit, four concurrent flushes, a disabled relay, a stopped
driver and a flush on the driver task. They also cover the schema and its
shape check, the refusals, the rollback of the writer, a database with no
table and the stored start options. `StartOnce` and `newest_run_of_key`
have their own tests over a live, a terminated and a blank key.
