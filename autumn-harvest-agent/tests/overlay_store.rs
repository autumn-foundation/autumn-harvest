//! The overlay store suite on the in-memory and `SQLite` stores
//! (issue #2371).
//!
//! `overlay_store_postgres.rs` runs the same suite on Postgres.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod overlay_store_suite;

use autumn_harvest_agent::InMemoryOverlayStore;

#[tokio::test]
async fn the_in_memory_store_passes_the_suite() {
    overlay_store_suite::run_all(&|validator| Box::new(InMemoryOverlayStore::new(validator))).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn the_sqlite_store_passes_the_suite() {
    use autumn_harvest_agent::SqliteOverlayStore;

    overlay_store_suite::run_all(&|validator| {
        Box::new(SqliteOverlayStore::open_in_memory(validator).expect("open"))
    })
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
        store.activate("support", v1, None).await.expect("activate");
        v1
    };
    let store = SqliteOverlayStore::open(&path, gate).expect("reopen");
    let active = store.active("support").await.expect("active").expect("set");
    assert_eq!(active.version(), v1);
    assert_eq!(v1, OverlayVersion::new(1).expect("v1"));
    assert_eq!(active.overlay(), &overlay);
}
