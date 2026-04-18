#![cfg(feature = "db")]

use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::signal::{load_pending_signals, mark_signals_consumed, send_signal};
use autumn_harvest::types::ExecutionId;
use diesel_async::RunQueryDsl;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const INIT_SQL: &str = include_str!("../migrations/20260409000000_harvest_initial/up.sql");

async fn setup_test_db() -> (
    diesel_async::AsyncPgConnection,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let conn = <diesel_async::AsyncPgConnection as diesel_async::AsyncConnection>::establish(
        &database_url,
    )
    .await
    .expect("failed to connect to Postgres container");

    (conn, container)
}

#[tokio::test]
async fn test_send_and_load_signals() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();

    // Create workflow execution first (FK constraint)
    let new_exec = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "test_workflow",
        workflow_id: "test_id",
        run_id: exec_id.as_uuid(),
        shard_id: 0,
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&new_exec)
        .execute(&mut conn)
        .await
        .unwrap();

    // Load initial - should be empty
    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert!(signals.is_empty());

    // Send a signal
    let payload = serde_json::json!({"key": "value"});
    send_signal(&mut conn, exec_id, "test_signal", payload.clone())
        .await
        .unwrap();

    // Load again
    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0].signal_name, "test_signal");
    assert_eq!(signals[0].payload, payload);
    assert!(!signals[0].consumed);
}

#[tokio::test]
async fn test_mark_signals_consumed() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = ExecutionId::new();

    // Create workflow execution
    let new_exec = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "test_workflow",
        workflow_id: "test_id",
        run_id: exec_id.as_uuid(),
        shard_id: 0,
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&new_exec)
        .execute(&mut conn)
        .await
        .unwrap();

    // Send signals
    send_signal(&mut conn, exec_id, "sig1", serde_json::json!(1))
        .await
        .unwrap();
    send_signal(&mut conn, exec_id, "sig2", serde_json::json!(2))
        .await
        .unwrap();

    let signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(signals.len(), 2);

    // Mark first signal consumed
    let ids_to_mark = vec![signals[0].id];
    mark_signals_consumed(&mut conn, &ids_to_mark)
        .await
        .unwrap();

    // Load again
    let pending_signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(pending_signals.len(), 1);
    assert_eq!(pending_signals[0].id, signals[1].id);
    assert_eq!(pending_signals[0].signal_name, "sig2");

    let ids_to_mark = vec![];
    mark_signals_consumed(&mut conn, &ids_to_mark)
        .await
        .unwrap(); // Empty list test
    let pending_signals = load_pending_signals(&mut conn, exec_id).await.unwrap();
    assert_eq!(pending_signals.len(), 1);
}
