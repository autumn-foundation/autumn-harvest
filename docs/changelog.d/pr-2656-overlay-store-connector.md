## The SQLite overlay store on a connection factory (issue #2656)

`SqliteOverlayStore::with_connector(connect, validator)` opens the store
on the connections of an app factory. The overlays can then live in the
tenant database of a replicated autumn-web fleet. The design is in
`docs/design/DESIGN-2656.md`.

- **Factory.** `SqliteConnector` is
  `Arc<dyn Fn() -> rusqlite::Result<Connection> + Send + Sync>`.
  `autumn-harvest-sqlite` defines it, and `autumn-harvest-agent` re-exports
  it with the `sqlite` feature. `SqliteOverlayLog::with_connector` is the
  raw log on the same factory.
- **One connection per call.** The schema runs through the factory, in one
  `DEFERRED` transaction. A file that has the schema needs no write lock. Each later call opens one connection, runs its
  statements and drops it. The store runs one call at a time, so at most
  one store connection is open.
- **Pragmas.** The store sets `foreign_keys = ON` and no other pragma. The
  app sets `wal_autocheckpoint = 0`, `busy_timeout` and the rest.
- **No checkpoint at close.** Each store connection sets
  `SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE`. A store connection that closes last
  then leaves every frame in the WAL.
- **Guards.** The immutability triggers and the head and activation guards
  do not change. `open` and `open_in_memory` keep one connection, as before.
- **Tests.** `a_factory_store_leaves_every_frame_in_the_wal` puts,
  activates and rolls back a version on a WAL file with
  `wal_autocheckpoint = 0`. The main file does not change, and
  `PRAGMA wal_checkpoint` reports the frames still in the WAL. The close
  of the replicator then removes the WAL, so the store kept no connection.
  The old `open` fails this test. The shared
  overlay store suite runs on a factory store. In
  `autumn-harvest-sqlite`, `overlay_log_connector.rs` shows that the log
  keeps no connection between calls, that over 1000 pages stay in the WAL,
  and that a failed connect is an error. A control test with
  `wal_autocheckpoint = 1` sees a checkpoint, so the WAL check is not empty.
