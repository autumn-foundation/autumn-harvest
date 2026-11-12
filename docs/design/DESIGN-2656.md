# DESIGN-2656: open the SQLite overlay store with a connection factory

Issue #2656.

## Problem

`SqliteOverlayStore::open` keeps one connection for the life of the store.
That connection uses the default `wal_autocheckpoint`. An autumn-web fleet
database that replicates its WAL forbids both facts (autumn ADR 0019,
section 5):

- Only the replicator checkpoints. The database runs with
  `wal_autocheckpoint = 0`.
- The replicator connection closes last. So the checkpoint at the last
  close never folds a frame that the replicator did not ship.

A long-lived store connection checkpoints after 1000 pages, and it can
close last. So the overlays stay in a separate, unreplicated file.

## Acceptance

A test opens the store through a factory on a WAL file with
`wal_autocheckpoint = 0`. It puts, activates and rolls back a version.
`PRAGMA wal_checkpoint` then reports the frames still in the WAL.

## Facts

- `SqliteOverlayLog` in `autumn-harvest-sqlite` holds the connection in a
  mutex. Each method runs under that lock. The typed store in
  `autumn-harvest-agent` wraps the log.
- Each write uses one `IMMEDIATE` transaction. No method keeps state
  between calls, so a new connection for each call is safe.
- The schema is `CREATE ... IF NOT EXISTS`. The guards are triggers in the
  file, so they hold for every connection.
- `foreign_keys` is a pragma of a connection. It does not touch the WAL.
- SQLite checkpoints when the last connection to a WAL file closes. It
  then deletes the WAL file.

## Brainstorm

- A factory closure, as the issue asks.
- A borrowed connection for each call: `store.put_on(&conn, ...)`.
- A pool trait, such as `r2d2::ManageConnection`.
- A flag on `open` that sets `wal_autocheckpoint = 0`.

## Reverse brainstorm: how can the change fail?

| Failure | Guard |
|---|---|
| The store keeps a connection after a call. | A test closes the replicator connection while the store lives. The WAL must go. |
| The store sets `journal_mode` or `wal_autocheckpoint` and undoes the app. | The store sets `foreign_keys` only. A test writes over 1000 pages, and the main file must not change. |
| The test cannot see a checkpoint. | A control test sets `wal_autocheckpoint = 1` and sees one. |
| A store call runs after the replicator closes, and its connection closes last. | Each store connection turns off the checkpoint at its close. A test closes the replicator first. |
| The open waits for a fleet writer. | The schema runs in a `DEFERRED` transaction. A test opens while another connection holds the write lock. |
| The schema runs on a connection outside the factory. | A test counts the factory calls of the open. |
| A failed connect panics or hangs. | A test fails the factory, and the call returns a backend error. |
| The guards stop on a factory connection. | The shared store suite runs on a factory store. |
| Two calls of one store fight for the write lock. | The store runs one call at a time. So at most one store connection is open. |

## Six hats

- **White (facts):** see Facts above.
- **Red (feel):** the change is small and local. The per-call open costs
  little, because overlay calls are rare: one read per agent op and some
  operator calls.
- **Black (risks):** an app with no `busy_timeout` gets `SQLITE_BUSY`
  under a fleet writer. A factory that opens a new in-memory database on
  each call loses the tables. Both are app duties, and the docs say so.
- **Yellow (value):** the overlays join the tenant database. So one
  replica holds the full state of an owner.
- **Green (options):** the borrowed connection changes every trait method.
  A pool trait adds a dependency. A flag on `open` keeps the connection,
  so it can still close last. The factory is the smallest change that
  meets both rules.
- **Blue (process):** red tests first, then the code, then a refactor,
  the docs and the gates.

## Decision

- `autumn_harvest_sqlite::SqliteConnector` is
  `Arc<dyn Fn() -> rusqlite::Result<Connection> + Send + Sync>`.
  `autumn-harvest-agent` re-exports it with the `sqlite` feature.
- `SqliteOverlayLog::with_connector(connect)` applies the schema through
  `connect`, in one `DEFERRED` transaction, and drops that connection. A
  file that has the schema needs no write, so the open takes no write
  lock.
- Each later call opens one connection through `connect`, sets
  `foreign_keys = ON`, runs its statements and drops the connection.
- The store runs one call at a time, as `open` does.
- Each store connection sets `SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE`. So a
  store connection that closes last in a shutdown race checkpoints
  nothing. The app rule stays: the replicator connection closes last.
- The store sets no other pragma. The app sets `journal_mode`,
  `busy_timeout`, `synchronous` and `wal_autocheckpoint` in `connect`.
- `SqliteOverlayStore::with_connector(connect, validator)` wraps the log.
- `open` and `open_in_memory` keep one connection, as before.
- The guards do not change.

## Tests

- `autumn-harvest-agent/tests/overlay_store.rs`: the shared suite on a
  factory store, and the acceptance test.
- `autumn-harvest-sqlite/tests/integration/overlay_log_connector.rs`: the factory
  calls, the last close, the close of a store connection after the
  replicator, the open under a writer, the 1000-page write, the control
  and the failed
  connect.
