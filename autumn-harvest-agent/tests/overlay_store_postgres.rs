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
    let kind = format!("guard-{}", std::process::id());
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
    let kind = format!("pointer-{}", std::process::id());
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
