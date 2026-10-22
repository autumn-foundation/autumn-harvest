//! The Postgres side against a real database (issue #2257).
//!
//! The test uses `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database. Without it, the test starts a Postgres container.
//! CI runs this target from a `linux` row of `.github/ci/integration-suites.txt`.

use autumn_harvest::WorkflowEvent;
use autumn_harvest::migrate;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use history_load_gate::{pg_side, seed};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// A migrated connection, and the container that serves it, if any.
async fn connect() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
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
    (conn, container)
}

fn values(events: &[WorkflowEvent]) -> serde_json::Value {
    serde_json::to_value(events).expect("encode")
}

/// One test, so the migration runs once. Each step checks one path.
#[tokio::test]
async fn the_postgres_side_seeds_clones_loads_and_deletes() {
    let (mut conn, _container) = connect().await;

    // A seeded history loads back event for event, after the start event
    // that the start path writes.
    let history = seed::history(2_501, 64);
    let exec_id = pg_side::seed(&mut conn, &history, "hlg-test")
        .await
        .expect("seed");
    let loaded = pg_side::load(&mut conn, exec_id).await.expect("load");
    assert_eq!(loaded.events.len(), history.len());
    assert_eq!(loaded.next_event_id, 2_501);
    assert_eq!(values(&loaded.events[1..]), values(&history[1..]));
    pg_side::delete_runs(&mut conn, &[exec_id])
        .await
        .expect("delete");

    // Each clone holds a copy of the template events under its own id.
    let history = seed::history(13, 16);
    let template = pg_side::seed(&mut conn, &history, "hlg-test")
        .await
        .expect("seed");
    let expected = pg_side::load(&mut conn, template).await.expect("load");
    let clones = pg_side::clone_runs(&mut conn, template, 50)
        .await
        .expect("clone");
    assert_eq!(clones.len(), 50);
    for exec_id in &clones {
        let loaded = pg_side::load(&mut conn, *exec_id).await.expect("load");
        assert_eq!(values(&loaded.events), values(&expected.events));
    }

    // A delete removes each run and its events.
    let mut all = clones;
    all.push(template);
    pg_side::delete_runs(&mut conn, &all).await.expect("delete");
    for exec_id in [template, all[0]] {
        let gone = pg_side::load(&mut conn, exec_id).await.expect("load");
        assert!(gone.events.is_empty(), "a deleted run has no events");
    }
}
