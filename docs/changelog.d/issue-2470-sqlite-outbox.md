## Plugin — SQLite app: transactional outbox (issue #2470, epic #2466)

A SQLite app in embedded mode gets the transactional outbox of the Postgres
path. The Postgres default build does not change. The design record is
`docs/design/DESIGN-2470.md`.

- **Writer.** `sqlite::enqueue_workflow_start_outbox(conn, &request)` takes
  the `WorkflowStartRequest` of Postgres. It writes on the caller's
  connection, so the start commits or rolls back with the business rows. A
  duplicate key is a no-op. It refuses an empty `workflow_id`, a
  `queue_name` other than `"default"`, `search_attrs` that is not an object
  of JSON primitives, and a database with no outbox table.
- **Table.** The startup hook creates `harvest_workflow_outbox` in the app's
  primary database with `CREATE TABLE IF NOT EXISTS`, as
  `autumn_web::outbox` does. In a fleet app, only the control database gets
  it, so autumn#3536 is not needed. Times are epoch milliseconds. There is no
  claim column. `sqlite::OUTBOX_SCHEMA_SQL` holds the DDL.
- **Relay.** It runs on the `harvest.outbox.*` settings. Each row goes to the
  driver as the new `StartOnce` command, which returns any run of the key,
  in any state, before it starts one. A mutex keeps two drains in one
  process apart. The shutdown hook stops the relay before the driver.
  `flush_workflow_start_outbox` and `drain_workflow_start_outbox_once`
  drain the SQLite outbox. `claim_ttl_ms` has no effect on SQLite.
- **Shared backoff.** `outbox::retry_delay_for_attempts` serves both
  backends.
- **No new `WorkflowEvent` variant. No migration.**

Tests: `tests/sqlite_plugin/outbox.rs` (in the `sqlite_plugin` suite) proves
AC1 with a rolled-back business transaction, and AC2 with a crash before
the relay and a crash between the start and the mark. It also covers a
redelivery over a terminated run, a failed start with backoff, and a
duplicate enqueue. Unit tests cover the schema, the refusals, the rollback
of the writer, a database with no table, the stored start options, and
`StartOnce` over a live, a terminated and a blank key.
