//! The Postgres side against a real database (issue #2257).
//!
//! Set `HARVEST_TEST_DATABASE_URL` to run these tests. Without it each test
//! prints a notice and passes, because CI runs this crate without Postgres.

use autumn_harvest::WorkflowEvent;
use autumn_harvest::migrate;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use history_load_gate::{pg_side, seed};

async fn connect() -> Option<AsyncPgConnection> {
    let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
        eprintln!("HARVEST_TEST_DATABASE_URL is not set; skipping");
        return None;
    };
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    migrate::apply_to_connection(&mut conn, &migrate::embedded())
        .await
        .expect("migrate");
    Some(conn)
}

fn values(events: &[WorkflowEvent]) -> serde_json::Value {
    serde_json::to_value(events).expect("encode")
}

/// The seeded history loads back event for event, with the start event
/// that the start path writes.
#[tokio::test]
async fn a_seeded_run_loads_back_with_every_event() {
    let Some(mut conn) = connect().await else {
        return;
    };
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
}

/// Each clone holds a copy of the template events under its own id.
#[tokio::test]
async fn each_clone_loads_the_template_history() {
    let Some(mut conn) = connect().await else {
        return;
    };
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
    let mut all = clones;
    all.push(template);
    pg_side::delete_runs(&mut conn, &all).await.expect("delete");
    let gone = pg_side::load(&mut conn, template).await.expect("load");
    assert!(gone.events.is_empty(), "a deleted run has no events");
}
