//! The overlay store suite on the in-memory and `SQLite` stores
//! (issue #2371).
//!
//! `overlay_store_postgres.rs` runs the same suite on Postgres.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod overlay_store_suite;

use autumn_harvest_agent::InMemoryOverlayStore;

#[tokio::test]
async fn the_in_memory_store_passes_the_suite() {
    overlay_store_suite::run_all(
        &|validator| Box::new(InMemoryOverlayStore::new(validator)),
        &|validator, clock| Box::new(InMemoryOverlayStore::new(validator).with_clock(clock)),
    )
    .await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn the_sqlite_store_passes_the_suite() {
    use autumn_harvest_agent::SqliteOverlayStore;

    overlay_store_suite::run_all(
        &|validator| Box::new(SqliteOverlayStore::open_in_memory(validator).expect("open")),
        &|validator, clock| {
            Box::new(
                SqliteOverlayStore::open_in_memory(validator)
                    .expect("open")
                    .with_clock(clock),
            )
        },
    )
    .await;
}

/// A store on a file keeps its versions after a reopen, as after a deploy.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn the_sqlite_store_keeps_versions_across_a_reopen() {
    use std::sync::Arc;

    use autumn_harvest_agent::{
        OverlayVersion, ProfileOverlay, ProfileOverlayStore, Provenance, SqliteOverlayStore,
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("harvest.db");
    let gate = Arc::new(overlay_store_suite::Gate::default());
    let overlay = ProfileOverlay {
        max_steps: Some(4),
        ..ProfileOverlay::default()
    };
    let v1 = {
        let store = SqliteOverlayStore::open(&path, gate.clone()).expect("open");
        let v1 = store
            .put("support", &overlay, Provenance::operator("ops"))
            .await
            .expect("put");
        store
            .activate("support", v1, None, Provenance::operator("ops"))
            .await
            .expect("activate");
        v1
    };
    let store = SqliteOverlayStore::open(&path, gate).expect("reopen");
    let active = store.active("support").await.expect("active").expect("set");
    assert_eq!(active.version(), v1);
    assert_eq!(v1, OverlayVersion::new(1).expect("v1"));
    assert_eq!(active.overlay(), &overlay);
}

/// A fleet database: a WAL file whose replicator connection stays open
/// and never checkpoints by itself.
#[cfg(feature = "sqlite")]
struct Fleet {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
    replicator: rusqlite::Connection,
}

#[cfg(feature = "sqlite")]
impl Fleet {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tenant.db");
        let replicator = rusqlite::Connection::open(&path).expect("replicator");
        replicator
            .execute_batch(
                "PRAGMA journal_mode = WAL;\n\
                 PRAGMA wal_autocheckpoint = 0;\n\
                 CREATE TABLE IF NOT EXISTS app_rows (id INTEGER PRIMARY KEY);",
            )
            .expect("replicator pragmas");
        Self {
            _dir: dir,
            path,
            replicator,
        }
    }

    /// The factory of the app. It sets the pragmas of the fleet.
    fn connector(&self) -> autumn_harvest_agent::SqliteConnector {
        let path = self.path.clone();
        std::sync::Arc::new(move || {
            let conn = rusqlite::Connection::open(&path)?;
            conn.execute_batch(
                "PRAGMA busy_timeout = 5000;\n\
                 PRAGMA wal_autocheckpoint = 0;",
            )?;
            Ok(conn)
        })
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn the_sqlite_factory_store_passes_the_suite() {
    use autumn_harvest_agent::SqliteOverlayStore;

    let fleet = Fleet::new();
    // The replicator connection is not `Sync`. The suite closures share the
    // factory only.
    let connect = fleet.connector();
    overlay_store_suite::run_all(
        &|validator| {
            Box::new(SqliteOverlayStore::with_connector(connect.clone(), validator).expect("open"))
        },
        &|validator, clock| {
            Box::new(
                SqliteOverlayStore::with_connector(connect.clone(), validator)
                    .expect("open")
                    .with_clock(clock),
            )
        },
    )
    .await;
}

/// A factory store on a fleet WAL file never checkpoints it. The frames of
/// a put, an activation and a rollback stay in the WAL for the replicator.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn a_factory_store_leaves_every_frame_in_the_wal() {
    use std::sync::Arc;

    use autumn_harvest_agent::{
        ProfileOverlay, ProfileOverlayStore, Provenance, SqliteOverlayStore,
    };

    let fleet = Fleet::new();
    let main_before = std::fs::read(&fleet.path).expect("main file");
    let store = SqliteOverlayStore::with_connector(
        fleet.connector(),
        Arc::new(overlay_store_suite::Gate::default()),
    )
    .expect("open");
    let ops = || Provenance::operator("ops");
    let narrow = |steps| ProfileOverlay {
        max_steps: Some(steps),
        ..ProfileOverlay::default()
    };
    let v1 = store
        .put("support", &narrow(4), ops())
        .await
        .expect("put v1");
    store
        .activate("support", v1, None, ops())
        .await
        .expect("activate v1");
    let v2 = store
        .put("support", &narrow(3), ops())
        .await
        .expect("put v2");
    store
        .activate("support", v2, Some(v1), ops())
        .await
        .expect("activate v2");
    let rollback = store.rollback("support", ops()).await.expect("rollback");
    assert_eq!(rollback.to, v1);
    let active = store.active("support").await.expect("active").expect("set");
    assert_eq!(active.version(), v1);

    assert_eq!(
        std::fs::read(&fleet.path).expect("main file"),
        main_before,
        "the store folded no frame into the main file"
    );
    let (busy, log, done): (i64, i64, i64) = fleet
        .replicator
        .query_row("PRAGMA wal_checkpoint", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("checkpoint");
    assert_eq!(busy, 0);
    assert!(log > 0, "the WAL still holds the frames of the store");
    assert_eq!(done, log, "the replicator checkpoints them all");
    drop(store);
}
