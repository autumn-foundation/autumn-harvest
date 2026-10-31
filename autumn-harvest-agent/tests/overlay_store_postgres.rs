//! The overlay store suite on Postgres (issue #2371).
//!
//! The tests use `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database. Without it, each test starts a Postgres container.
//! CI runs this target from a `linux` row of
//! `.github/ci/integration-suites.txt`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod overlay_store_suite;

use std::sync::Arc;

use autumn_harvest::migrate;
use autumn_harvest::worker::DbPool;
use autumn_harvest_agent::{PostgresOverlayStore, ProfileOverlay, ProfileOverlayStore, Provenance};
use diesel::sql_types::Text;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// A migrated database, and the container that serves it, if any.
async fn database() -> (String, Option<ContainerAsync<Postgres>>) {
    let (url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.expect("container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("container port");
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    migrate::apply_to_connection(&mut conn, &migrate::embedded())
        .await
        .expect("migrate");
    (url, container)
}

/// A kind that no earlier test or run used, so a reused database works.
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |at| at.as_nanos());
    format!("{prefix}-{nanos}")
}

fn pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_postgres_store_passes_the_suite() {
    let (url, _container) = database().await;
    let pool = pool(&url);
    overlay_store_suite::run_all(&|validator| {
        Box::new(PostgresOverlayStore::new(pool.clone(), validator))
    })
    .await;
}

/// Postgres refuses an `UPDATE` of a version row, as the `harvest_events`
/// guard refuses a history rewrite.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_refuses_an_update_of_a_version_row() {
    let (url, _container) = database().await;
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    let kind = unique("guard");
    let overlay = ProfileOverlay {
        max_steps: Some(3),
        ..ProfileOverlay::default()
    };
    let v1 = store
        .put(&kind, &overlay, Provenance::operator("ops"))
        .await
        .expect("put");
    let written = store.get(&kind, v1).await.expect("get").expect("v1");

    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    for statement in [
        "UPDATE harvest_agent_overlays SET body = '\\x7b7d'::bytea WHERE kind = $1",
        "UPDATE harvest_agent_overlays SET author = 'mallory' WHERE kind = $1",
        "UPDATE harvest_agent_overlays SET version = version WHERE kind = $1",
    ] {
        let err = diesel::sql_query(statement)
            .bind::<Text, _>(&kind)
            .execute(&mut conn)
            .await
            .expect_err("the guard refuses the update");
        assert!(
            err.to_string()
                .contains("harvest_agent_overlays is append-only"),
            "{statement}: {err}"
        );
    }
    let after = store.get(&kind, v1).await.expect("get").expect("v1");
    assert_eq!(after, written);
}

/// The foreign key on the pointer refuses the delete of the active version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_refuses_a_delete_of_the_active_version() {
    let (url, _container) = database().await;
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    let kind = unique("pointer");
    let v1 = store
        .put(
            &kind,
            &ProfileOverlay::default(),
            Provenance::operator("ops"),
        )
        .await
        .expect("put");
    store.activate(&kind, v1, None).await.expect("activate");

    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let err = diesel::sql_query("DELETE FROM harvest_agent_overlays WHERE kind = $1")
        .bind::<Text, _>(&kind)
        .execute(&mut conn)
        .await
        .expect_err("the foreign key refuses the delete");
    assert!(err.to_string().contains("foreign key"), "{err}");
    let active = store.active(&kind).await.expect("active").expect("set");
    assert_eq!(active.version(), v1);
}

/// The log refuses the delete of a head row, so a deleted version never
/// comes back with other bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_never_gives_a_version_number_twice() {
    let (url, _container) = database().await;
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    let kind = unique("reuse");
    let first = ProfileOverlay {
        max_steps: Some(3),
        ..ProfileOverlay::default()
    };
    let ops = || Provenance::operator("ops");
    let v1 = store.put(&kind, &first, ops()).await.expect("v1");
    let v2 = store.put(&kind, &first, ops()).await.expect("v2");
    store.activate(&kind, v1, None).await.expect("activate");

    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    diesel::sql_query("DELETE FROM harvest_agent_overlays WHERE kind = $1 AND version = 2")
        .bind::<Text, _>(&kind)
        .execute(&mut conn)
        .await
        .expect("a version that is not active can go");
    let err = diesel::sql_query("DELETE FROM harvest_agent_overlay_heads WHERE kind = $1")
        .bind::<Text, _>(&kind)
        .execute(&mut conn)
        .await
        .expect_err("the guard keeps the head row");
    assert!(err.to_string().contains("keeps its rows"), "{err}");

    let v3 = store
        .put(&kind, &ProfileOverlay::default(), ops())
        .await
        .expect("v3");
    assert!(v3 > v2, "{v3} > {v2}");
    assert_eq!(store.get(&kind, v2).await.expect("get"), None);
}

/// The log reads the facts of an activation under the lock, so its own
/// refusals hold without the store in front of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_postgres_log_decides_under_the_lock() {
    use autumn_harvest::overlay_log::{ActivateOutcome, activate_overlay, append_overlay};

    let (url, _container) = database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let kind = unique("log");
    assert_eq!(
        activate_overlay(&mut conn, &kind, 1, None)
            .await
            .expect("no head"),
        ActivateOutcome::UnknownVersion
    );
    let row = append_overlay(&mut conn, &kind, b"{}", "operator", "ops")
        .await
        .expect("append")
        .expect("a version");
    assert_eq!(row.version, 1);
    assert_eq!(
        activate_overlay(&mut conn, &kind, 2, None)
            .await
            .expect("unknown"),
        ActivateOutcome::UnknownVersion
    );
    assert_eq!(
        activate_overlay(&mut conn, &kind, u64::MAX, None)
            .await
            .expect("too large"),
        ActivateOutcome::UnknownVersion
    );
    assert_eq!(
        activate_overlay(&mut conn, &kind, 1, Some(u64::MAX))
            .await
            .expect("far expected"),
        ActivateOutcome::Conflict { actual: None }
    );
    assert_eq!(
        activate_overlay(&mut conn, &kind, 1, None)
            .await
            .expect("activate"),
        ActivateOutcome::Activated
    );
}

/// A full counter inserts nothing and reports it, so the store returns
/// `Full`, not a database error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_postgres_counter_reports_full() {
    use autumn_harvest::overlay_log::append_overlay;

    let (url, _container) = database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let kind = unique("full");
    diesel::sql_query(
        "INSERT INTO harvest_agent_overlay_heads (kind, latest) VALUES ($1, 9223372036854775807)",
    )
    .bind::<Text, _>(&kind)
    .execute(&mut conn)
    .await
    .expect("seed a full counter");
    let row = append_overlay(&mut conn, &kind, b"{}", "operator", "ops")
        .await
        .expect("append");
    assert_eq!(row, None);
    let store =
        PostgresOverlayStore::new(pool(&url), Arc::new(overlay_store_suite::Gate::default()));
    assert_eq!(
        store
            .put(
                &kind,
                &ProfileOverlay::default(),
                Provenance::operator("ops")
            )
            .await,
        Err(autumn_harvest_agent::OverlayStoreError::Full)
    );
}
