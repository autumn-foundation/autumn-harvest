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
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

const OUTBOX_INIT_SQL: &str =
    include_str!("../migrations/app/20260409010000_harvest_workflow_outbox/up.sql");
#[derive(Debug, QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

/// The deadline spans of a started run, in seconds from its start.
#[derive(Debug, QueryableByName)]
struct Spans {
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
    timeout: Option<f64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
    sla: Option<f64>,
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

fn noop_handler<'a>(
    _ctx: &'a autumn_harvest::WorkflowContext,
    _input: serde_json::Value,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>,
> {
    Box::pin(async move { Ok(serde_json::json!({})) })
}

/// Issue #2387: an outbox start takes the operator's timeout and SLA override,
/// as an API start does.
#[tokio::test]
async fn framework_outbox_start_takes_the_run_default_override() {
    const HOUR: std::time::Duration = std::time::Duration::from_secs(3600);
    let (state, mut app_conn, _app_url, harvest_url, _container) =
        setup_split_test_databases(true).await;
    let info = autumn_harvest::info::WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "user_onboarding",
        module: "outbox_integration",
        handler: noop_handler,
        execution_timeout: Some(3 * HOUR),
        chain_execution_timeout: None,
        sla: Some(2 * HOUR),
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    };
    let registry = std::sync::Arc::new(autumn_harvest::worker::HandlerRegistry::new(
        vec![info],
        vec![],
    ));
    registry.run_defaults().write_through(
        "user_onboarding",
        autumn_harvest::run_defaults::RunDefaultFields {
            execution_timeout: Some(std::time::Duration::from_secs(600)),
            sla: None,
        },
    );
    state.insert_extension(registry);

    let request = WorkflowStartRequest {
        workflow_name: "user_onboarding".to_string(),
        workflow_id: "user-onboarding:2387".to_string(),
        queue_name: "default".to_string(),
        input: serde_json::json!({}),
        memo: None,
        search_attrs: None,
    };
    enqueue_workflow_start_outbox(&mut app_conn, &request)
        .await
        .expect("outbox row should persist");
    let delivered = drain_workflow_start_outbox_once(&state, 32)
        .await
        .expect("outbox drain should succeed");
    assert_eq!(delivered, 1);

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&harvest_url)
        .await
        .expect("connect to the harvest database");
    let spans: Spans = diesel::sql_query(
        "SELECT EXTRACT(EPOCH FROM deadline_at - started_at)::float8 AS timeout, \
                EXTRACT(EPOCH FROM sla_deadline_at - started_at)::float8 AS sla \
         FROM harvest_workflow_executions WHERE workflow_id = 'user-onboarding:2387'",
    )
    .get_result(&mut conn)
    .await
    .expect("read the started run");
    let timeout = spans.timeout.expect("the run has a hard deadline");
    assert!(
        (timeout - 600.0).abs() < 2.0,
        "the override timeout applies: {timeout}"
    );
    // The code SLA of 2 hours is clamped to the 10-minute override timeout.
    let sla = spans.sla.expect("the run has an SLA deadline");
    assert!(
        (sla - 600.0).abs() < 2.0,
        "the SLA is clamped to the timeout: {sla}"
    );
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
        "SELECT EXTRACT(EPOCH FROM (next_attempt_at - created_at))::double precision AS seconds \
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
        .with_tag("16")
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
            .batch_execute(&autumn_harvest::test_init_sql())
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
