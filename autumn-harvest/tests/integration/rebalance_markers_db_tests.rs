#![cfg(feature = "db")]
//! The rebalance markers that sit outside the executions `table!` (issue #2492).
//!
//! `schema::harvest_workflow_executions_unmapped` reads four columns through
//! SQL fragments, so the `table!` keeps 64 columns or fewer. A fragment is not
//! checked against the database at compile time. These tests write each
//! column with raw SQL and read it back through the typed paths.

use autumn_harvest::models::{RebalanceMarkers, WorkflowExecution};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::schema::harvest_workflow_executions_unmapped as unmapped;

use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

async fn connect() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    let (url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
            .with_tag("16")
            .start()
            .await
            .expect("start the Postgres container");
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
    let conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to the test database");
    (conn, container)
}

/// Insert one `MIGRATED` seal with every marker set, and return its id.
async fn seed_seal(conn: &mut AsyncPgConnection, vacated_by: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, run_id, shard_id, state, input, \
            started_at, created_at, migrated_from_shards, \
            migrated_run_terminal_state, staging_vacated_state, staging_vacated_by) \
         VALUES \
           ('{id}', 'markers_flow', 'markers-{id}', gen_random_uuid(), 0, \
            'MIGRATED', '{{}}', now(), now(), '[3, 5]'::jsonb, \
            'FAILED', 'COMPLETED', '{vacated_by}')"
    ))
    .await
    .expect("seed a seal with every marker set");
    id
}

#[tokio::test]
async fn the_execution_model_reads_every_marker_through_its_fragment() {
    let (mut conn, _container) = connect().await;
    let vacated_by = Uuid::new_v4();
    let id = seed_seal(&mut conn, vacated_by).await;

    let row: WorkflowExecution = harvest_workflow_executions::table
        .find(id)
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("load the seal");

    assert_eq!(
        row.rebalance,
        RebalanceMarkers {
            migrated_from_shards: Some(serde_json::json!([3, 5])),
            migrated_run_terminal_state: Some("FAILED".to_string()),
            staging_vacated_state: Some("COMPLETED".to_string()),
            staging_vacated_by: Some(vacated_by),
        }
    );
    assert_eq!(row.effective_terminal_state(), "FAILED");
}

#[tokio::test]
async fn a_fragment_mixes_with_table_columns_in_one_select() {
    let (mut conn, _container) = connect().await;
    let vacated_by = Uuid::new_v4();
    let id = seed_seal(&mut conn, vacated_by).await;

    let (state, shards, vacated): (String, Option<serde_json::Value>, Option<Uuid>) =
        harvest_workflow_executions::table
            .find(id)
            .select((
                harvest_workflow_executions::state,
                unmapped::migrated_from_shards(),
                unmapped::staging_vacated_by(),
            ))
            .for_update()
            .first(&mut conn)
            .await
            .expect("select table columns and fragments together");

    assert_eq!(state, "MIGRATED");
    assert_eq!(shards, Some(serde_json::json!([3, 5])));
    assert_eq!(vacated, Some(vacated_by));
}

#[tokio::test]
async fn an_unmoved_run_reads_empty_markers() {
    let (mut conn, _container) = connect().await;
    let id = Uuid::new_v4();
    conn.batch_execute(&format!(
        "INSERT INTO harvest_workflow_executions \
           (id, workflow_name, workflow_id, run_id, shard_id, state, input, \
            started_at, created_at) \
         VALUES \
           ('{id}', 'markers_flow', 'plain-{id}', gen_random_uuid(), 0, \
            'RUNNING', '{{}}', now(), now())"
    ))
    .await
    .expect("seed a run that never moved");

    let row: WorkflowExecution = harvest_workflow_executions::table
        .find(id)
        .select(WorkflowExecution::as_select())
        .first(&mut conn)
        .await
        .expect("load the run");

    assert_eq!(row.rebalance, RebalanceMarkers::default());
    assert_eq!(row.effective_terminal_state(), "RUNNING");
}

/// Every column that a fragment names exists on the table.
///
/// A typo in a fragment fails here by name, not as an opaque SQL error in
/// some unrelated query.
#[tokio::test]
async fn every_fragment_column_exists() {
    #[derive(QueryableByName)]
    struct Name {
        #[diesel(sql_type = diesel::sql_types::Text)]
        column_name: String,
    }

    let (mut conn, _container) = connect().await;
    let names: Vec<Name> = diesel::sql_query(
        "SELECT column_name::text AS column_name FROM information_schema.columns \
          WHERE table_schema = current_schema() \
            AND table_name = 'harvest_workflow_executions'",
    )
    .load(&mut conn)
    .await
    .expect("list the table's columns");
    let names: Vec<String> = names.into_iter().map(|row| row.column_name).collect();

    for column in unmapped::COLUMNS {
        assert!(
            names.iter().any(|name| name == column),
            "fragment column `{column}` is missing from harvest_workflow_executions"
        );
    }
}
