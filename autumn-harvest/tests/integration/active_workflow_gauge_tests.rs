#![cfg(feature = "db")]
//! Active-workflow population gauge behavioral tests — issue #770.
//!
//! Drives `worker::sample_active_workflow_counts` against a Postgres instance
//! and asserts the shard-local grouped read that feeds the
//! `harvest.workflow.active` gauge:
//!
//! - RUNNING and PAUSED executions are counted, grouped by
//!   `(workflow_name, state)` with the state normalized to the bounded
//!   lowercase label value (`running` / `paused`).
//! - Terminal states (COMPLETED, FAILED, …) are excluded — a non-active state
//!   never appears in the result.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly (each test scrubs the executions table first);
//! otherwise a fresh testcontainers Postgres is booted with the full migration
//! bundle.

use std::collections::BTreeMap;

use autumn_harvest::worker::{ActiveWorkflowState, sample_active_workflow_counts};
use diesel::sql_types::Text;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

/// Returns a live URL to a migrated Postgres, keeping the container (if any)
/// alive for the test's duration.
async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url)
        .await
        .expect("connect to test DB")
}

/// Scrubs all workflow executions so a shared migrated DB stays isolated.
async fn scrub(conn: &mut AsyncPgConnection) {
    diesel::sql_query("DELETE FROM harvest_workflow_executions")
        .execute(conn)
        .await
        .expect("scrub executions");
}

/// Inserts one execution of `workflow_name` in `state`.
async fn insert_execution(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
) {
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions
            (workflow_name, workflow_id, shard_id, state, input, started_at)
         VALUES ($1, $2, 0, $3, '{}'::jsonb, NOW())",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Text, _>(state)
    .execute(conn)
    .await
    .expect("insert execution");
}

/// Collects the sampler output into a sorted `(workflow, state) -> count` map.
fn as_map(rows: Vec<(String, ActiveWorkflowState, u64)>) -> BTreeMap<(String, String), u64> {
    rows.into_iter()
        .map(|(wf, state, count)| ((wf, state.as_str().to_owned()), count))
        .collect()
}

#[tokio::test]
async fn sample_groups_active_states_and_excludes_terminal() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    // 2 RUNNING + 1 PAUSED of "alpha", 1 RUNNING of "beta", 1 COMPLETED of
    // "alpha" (must be excluded).
    insert_execution(&mut conn, "alpha", "a-1", "RUNNING").await;
    insert_execution(&mut conn, "alpha", "a-2", "RUNNING").await;
    insert_execution(&mut conn, "alpha", "a-3", "PAUSED").await;
    insert_execution(&mut conn, "beta", "b-1", "RUNNING").await;
    insert_execution(&mut conn, "alpha", "a-done", "COMPLETED").await;

    let rows = sample_active_workflow_counts(&mut conn)
        .await
        .expect("sample must succeed");
    let map = as_map(rows);

    let mut expected: BTreeMap<(String, String), u64> = BTreeMap::new();
    expected.insert(("alpha".to_owned(), "running".to_owned()), 2);
    expected.insert(("alpha".to_owned(), "paused".to_owned()), 1);
    expected.insert(("beta".to_owned(), "running".to_owned()), 1);

    assert_eq!(map, expected);

    // No non-active (terminal) state may appear.
    for (_, state) in map.keys() {
        assert!(
            state == "running" || state == "paused",
            "unexpected non-active state {state:?} in gauge sample"
        );
    }
}

#[tokio::test]
async fn sample_is_empty_when_no_active_executions() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    scrub(&mut conn).await;

    insert_execution(&mut conn, "gamma", "g-1", "COMPLETED").await;
    insert_execution(&mut conn, "gamma", "g-2", "FAILED").await;

    let rows = sample_active_workflow_counts(&mut conn)
        .await
        .expect("sample must succeed");
    assert!(
        rows.is_empty(),
        "only terminal executions exist — the gauge sample must be empty, got {rows:?}"
    );
}
