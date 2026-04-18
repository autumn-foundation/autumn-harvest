use autumn_harvest_plugin::{
    HarvestDbPool, HarvestOutboxConfig, WorkflowStartRequest, drain_workflow_start_outbox_once,
    enqueue_workflow_start_outbox, flush_workflow_start_outbox,
};
use autumn_web::AppState;
use autumn_web::config::DatabaseConfig;
use diesel::QueryableByName;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

const OUTBOX_INIT_SQL: &str =
    include_str!("../migrations/20260409010000_harvest_workflow_outbox/up.sql");
const HARVEST_INIT_SQL: &str =
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql");

#[derive(Debug, QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(Debug, QueryableByName)]
struct DelayRow {
    #[diesel(sql_type = diesel::sql_types::Double)]
    seconds: f64,
}

#[tokio::test]
async fn framework_outbox_delivers_to_split_harvest_store() {
    let (state, mut app_conn, _app_url, _harvest_url, _container) =
        setup_split_test_databases(true).await;

    let request = WorkflowStartRequest {
        workflow_name: "user_onboarding".to_string(),
        workflow_id: "user-onboarding:42".to_string(),
        queue_name: "default".to_string(),
        input: serde_json::json!({
            "user_id": 42,
            "username": "ferris",
        }),
        memo: Some(serde_json::json!({
            "kind": "user_onboarding",
            "user_id": 42,
        })),
        search_attrs: Some(serde_json::json!({
            "user_id": 42,
            "username": "ferris",
        })),
    };

    enqueue_workflow_start_outbox(&mut app_conn, &request)
        .await
        .expect("outbox row should persist in the app database");

    let delivered = drain_workflow_start_outbox_once(&state, 32)
        .await
        .expect("outbox drain should succeed");
    assert_eq!(delivered, 1, "one row should be delivered");
}

#[tokio::test]
async fn framework_outbox_retries_failed_delivery() {
    let (state, mut app_conn, _app_url, _harvest_url, _container) =
        setup_split_test_databases(false).await;

    let request = WorkflowStartRequest {
        workflow_name: "post_publication".to_string(),
        workflow_id: "post-publication:99".to_string(),
        queue_name: "default".to_string(),
        input: serde_json::json!({
            "post_id": 99,
            "title": "Ferris arrives",
        }),
        memo: Some(serde_json::json!({
            "kind": "post_publication",
            "post_id": 99,
        })),
        search_attrs: Some(serde_json::json!({
            "post_id": 99,
            "post_slug": "ferris-arrives",
        })),
    };

    enqueue_workflow_start_outbox(&mut app_conn, &request)
        .await
        .expect("outbox row should persist even when delivery later fails");

    let delivered = drain_workflow_start_outbox_once(&state, 32)
        .await
        .expect("outbox drain should record failure without crashing");
    assert_eq!(delivered, 0, "failed delivery should not report success");
}

#[tokio::test]
async fn framework_outbox_flush_keeps_draining_full_failed_batches() {
    let (state, mut app_conn, _app_url, _harvest_url, _container) =
        setup_split_test_databases(false).await;
    state.insert_extension(HarvestOutboxConfig {
        batch_size: 2,
        ..HarvestOutboxConfig::default()
    });

    for workflow_id in [
        "post-publication:1",
        "post-publication:2",
        "post-publication:3",
    ] {
        enqueue_workflow_start_outbox(
            &mut app_conn,
            &WorkflowStartRequest {
                workflow_name: "post_publication".to_string(),
                workflow_id: workflow_id.to_string(),
                queue_name: "default".to_string(),
                input: serde_json::json!({ "workflow_id": workflow_id }),
                memo: None,
                search_attrs: None,
            },
        )
        .await
        .expect("outbox row should persist before drain");
    }

    let delivered = flush_workflow_start_outbox(&state)
        .await
        .expect("flush should keep draining full failed batches");
    assert_eq!(delivered, 0, "failed rows should not count as delivered");

    let attempts: CountRow = diesel::sql_query(
        "SELECT COUNT(*) AS count FROM harvest_workflow_outbox WHERE delivery_attempts = 1",
    )
    .get_result(&mut app_conn)
    .await
    .expect("should count processed outbox rows");
    assert_eq!(
        attempts.count, 3,
        "flush should process every due row even when the first full batch only fails",
    );
}

#[tokio::test]
async fn framework_outbox_retry_delay_tracks_database_clock() {
    let (_state, mut app_conn, app_url, harvest_url, _container) =
        setup_split_test_databases(false).await;
    app_conn
        .batch_execute("SET TIME ZONE 'America/Chicago'")
        .await
        .expect("app session should accept a non-UTC timezone");

    let state = build_test_state(&app_url, &harvest_url, 1);
    state.insert_extension(HarvestOutboxConfig {
        base_retry_delay_ms: 1_000,
        max_retry_delay_ms: 1_000,
        ..HarvestOutboxConfig::default()
    });
    let mut pooled = state
        .pool()
        .expect("state should expose the app pool")
        .get()
        .await
        .expect("failed to borrow pooled app connection");
    pooled
        .batch_execute("SET TIME ZONE 'America/Chicago'")
        .await
        .expect("pooled app session should accept a non-UTC timezone");
    drop(pooled);

    enqueue_workflow_start_outbox(
        &mut app_conn,
        &WorkflowStartRequest {
            workflow_name: "post_publication".to_string(),
            workflow_id: "post-publication:timezone".to_string(),
            queue_name: "default".to_string(),
            input: serde_json::json!({ "post_id": 42 }),
            memo: None,
            search_attrs: None,
        },
    )
    .await
    .expect("outbox row should persist before failure");

    let delivered = drain_workflow_start_outbox_once(&state, 1)
        .await
        .expect("drain should record the failed delivery");
    assert_eq!(delivered, 0, "failed delivery should not report success");

    let delay: DelayRow = diesel::sql_query(
        "SELECT EXTRACT(EPOCH FROM (next_attempt_at - created_at)) AS seconds \
         FROM harvest_workflow_outbox \
         WHERE workflow_id = 'post-publication:timezone'",
    )
    .get_result(&mut app_conn)
    .await
    .expect("should compute the retry delay from persisted timestamps");
    assert!(
        (0.5..5.0).contains(&delay.seconds),
        "retry delay should stay close to one second regardless of session timezone, got {}s",
        delay.seconds,
    );
}

async fn setup_split_test_databases(
    apply_harvest_migrations: bool,
) -> (
    AppState,
    AsyncPgConnection,
    String,
    String,
    ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
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
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let app_db_name = format!("framework_outbox_app_{}", Uuid::new_v4().simple());
    let harvest_db_name = format!("framework_outbox_harvest_{}", Uuid::new_v4().simple());

    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("failed to connect to admin database");
    diesel::sql_query(format!("CREATE DATABASE {app_db_name}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create app database");
    diesel::sql_query(format!("CREATE DATABASE {harvest_db_name}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create harvest database");

    let app_url = format!("postgres://postgres:postgres@{host}:{port}/{app_db_name}");
    let harvest_url = format!("postgres://postgres:postgres@{host}:{port}/{harvest_db_name}");

    let mut app_conn = <AsyncPgConnection as AsyncConnection>::establish(&app_url)
        .await
        .expect("failed to connect to app database");
    app_conn
        .batch_execute(OUTBOX_INIT_SQL)
        .await
        .expect("failed to apply app outbox migration");

    if apply_harvest_migrations {
        let mut harvest_conn = <AsyncPgConnection as AsyncConnection>::establish(&harvest_url)
            .await
            .expect("failed to connect to harvest database");
        harvest_conn
            .batch_execute(HARVEST_INIT_SQL)
            .await
            .expect("failed to apply harvest migrations");
    }

    let state = build_test_state(&app_url, &harvest_url, 4);

    (state, app_conn, app_url, harvest_url, container)
}

fn build_test_state(app_url: &str, harvest_url: &str, pool_size: usize) -> AppState {
    let state = AppState::for_test().with_pool(build_pool(app_url, pool_size));
    state.insert_extension(HarvestDbPool::from(build_pool(harvest_url, pool_size)));
    state
}

fn build_pool(
    database_url: &str,
    pool_size: usize,
) -> diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection> {
    autumn_web::db::create_pool(&DatabaseConfig {
        url: Some(database_url.to_owned()),
        pool_size,
        ..DatabaseConfig::default()
    })
    .expect("failed to build pool config")
    .expect("database url should create a pool")
}
