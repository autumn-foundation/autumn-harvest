use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::WorkflowEvent;
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::store;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::{ActivityExecId, ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{Duration, SecondsFormat, Utc};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

type HarvestApiApp = axum::Router;

fn billing_checkout_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("charge_card", json!({ "amount": 42 }), "payments")
            .await
            .map_err(|error| error.to_string())?;
        Ok(json!({ "ok": true }))
    })
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

async fn setup_database_url_with_migrations() -> Option<(String, ContainerAsync<Postgres>)> {
    let container = match Postgres::default().with_tag("16").start().await {
        Ok(container) => container,
        Err(error) => {
            eprintln!(
                "skipping history export integration test; Postgres container unavailable: {error}"
            );
            return None;
        }
    };
    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    autumn_web::migrate::run_pending(&url, autumn_harvest::MIGRATIONS)
        .expect("failed to run Harvest migrations");
    Some((url, container))
}

async fn setup_two_shards_with_migrations() -> Option<((String, String), ContainerAsync<Postgres>)>
{
    let container = match Postgres::default().with_tag("16").start().await {
        Ok(container) => container,
        Err(error) => {
            eprintln!(
                "skipping history export integration test; Postgres container unavailable: {error}"
            );
            return None;
        }
    };
    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let shard0_db = format!("harvest_history_export_0_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_history_export_1_{}", uuid::Uuid::new_v4().simple());

    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("failed to connect to admin database");
    diesel::sql_query(format!("CREATE DATABASE {shard0_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create shard 0 database");
    diesel::sql_query(format!("CREATE DATABASE {shard1_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create shard 1 database");

    let shard0_url = format!("postgres://postgres:postgres@{host}:{port}/{shard0_db}");
    let shard1_url = format!("postgres://postgres:postgres@{host}:{port}/{shard1_db}");
    autumn_web::migrate::run_pending(&shard0_url, autumn_harvest::MIGRATIONS)
        .expect("failed to migrate shard 0");
    autumn_web::migrate::run_pending(&shard1_url, autumn_harvest::MIGRATIONS)
        .expect("failed to migrate shard 1");
    Some(((shard0_url, shard1_url), container))
}

fn build_api_app(pool: HarvestDbPool, router: ShardRouter) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(pool);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::<WorkflowSchedule>::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    harvest_api_router(api_state).with_state(autumn_web::AppState::for_test())
}

fn build_two_shard_pool(shard0_url: &str, shard1_url: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_test_pool(shard0_url));
    pools.insert(ShardId::new(1), build_test_pool(shard1_url));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

fn two_shard_router() -> ShardRouter {
    ShardRouter::new(
        vec![ShardId::new(0), ShardId::new(1)],
        vec![ShardId::new(0), ShardId::new(1)],
        ShardId::new(0),
    )
}

async fn read_json_response(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    serde_json::from_slice(&body).expect("response must be JSON")
}

async fn get_json(app: &HarvestApiApp, uri: impl Into<String>) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request failed");
    let status = response.status();
    let json = read_json_response(response).await;
    (status, json)
}

async fn insert_execution(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    state: &str,
    events: Vec<WorkflowEvent>,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to test database");
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: shard.as_i32(),
        input: json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,

        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,

        sla_deadline_at: None,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("failed to insert workflow execution");

    let completed_at = if state == "RUNNING" {
        None
    } else {
        Some(Utc::now())
    };
    diesel::update(
        autumn_harvest::schema::harvest_workflow_executions::table.find(exec_id.as_uuid()),
    )
    .set((
        autumn_harvest::schema::harvest_workflow_executions::state.eq(state),
        autumn_harvest::schema::harvest_workflow_executions::completed_at.eq(completed_at),
    ))
    .execute(&mut conn)
    .await
    .expect("failed to update workflow state");

    store::append_events(&mut conn, exec_id, &events, 0)
        .await
        .expect("failed to append history");
    exec_id
}

async fn set_execution_and_event_times(
    database_url: &str,
    exec_id: ExecutionId,
    row_time: chrono::DateTime<Utc>,
    event_time: chrono::DateTime<Utc>,
) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to test database");
    diesel::update(
        autumn_harvest::schema::harvest_workflow_executions::table.find(exec_id.as_uuid()),
    )
    .set((
        autumn_harvest::schema::harvest_workflow_executions::started_at.eq(row_time),
        autumn_harvest::schema::harvest_workflow_executions::created_at.eq(row_time),
        autumn_harvest::schema::harvest_workflow_executions::completed_at.eq(Some(row_time)),
    ))
    .execute(&mut conn)
    .await
    .expect("failed to backdate workflow execution row");

    diesel::update(
        autumn_harvest::schema::harvest_events::table
            .filter(autumn_harvest::schema::harvest_events::workflow_exec_id.eq(exec_id.as_uuid())),
    )
    .set(autumn_harvest::schema::harvest_events::timestamp.eq(event_time))
    .execute(&mut conn)
    .await
    .expect("failed to set history event timestamp");
}

async fn set_running_execution_and_event_times(
    database_url: &str,
    exec_id: ExecutionId,
    row_time: chrono::DateTime<Utc>,
    event_time: chrono::DateTime<Utc>,
) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to test database");
    diesel::update(
        autumn_harvest::schema::harvest_workflow_executions::table.find(exec_id.as_uuid()),
    )
    .set((
        autumn_harvest::schema::harvest_workflow_executions::state.eq("RUNNING"),
        autumn_harvest::schema::harvest_workflow_executions::started_at.eq(row_time),
        autumn_harvest::schema::harvest_workflow_executions::created_at.eq(row_time),
        autumn_harvest::schema::harvest_workflow_executions::completed_at
            .eq(None::<chrono::DateTime<Utc>>),
    ))
    .execute(&mut conn)
    .await
    .expect("failed to backdate running workflow execution row");

    diesel::update(
        autumn_harvest::schema::harvest_events::table
            .filter(autumn_harvest::schema::harvest_events::workflow_exec_id.eq(exec_id.as_uuid())),
    )
    .set(autumn_harvest::schema::harvest_events::timestamp.eq(event_time))
    .execute(&mut conn)
    .await
    .expect("failed to set history event timestamp");
}

#[tokio::test]
async fn single_full_history_export_returns_replay_fixture_shape() {
    let Some((database_url, _container)) = setup_database_url_with_migrations().await else {
        return;
    };
    let activity_id = ActivityExecId::new();
    let exec_id = insert_execution(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "fixture-single",
        "COMPLETED",
        vec![
            WorkflowEvent::WorkflowStarted {
                input: json!({ "customer": "acme" }),
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_card".to_string(),
                input: json!({ "amount": 42 }),
                queue: "payments".to_string(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: json!({ "approved": true }),
            },
            WorkflowEvent::WorkflowCompleted {
                output: json!({ "ok": true }),
            },
        ],
    )
    .await;

    let app = build_api_app(
        HarvestDbPool::from(build_test_pool(&database_url)),
        ShardRouter::single(),
    );
    let (status, json) = get_json(
        &app,
        format!("/workflows/{exec_id}/history/export?payload_policy=full"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["schema"], "autumn-harvest.history-export");
    assert_eq!(json["version"], 1);
    assert_eq!(json["workflow_name"], "billing_checkout");
    assert_eq!(json["execution_id"], exec_id.to_string());
    assert_eq!(json["shard_id"], 0);
    assert_eq!(json["event_count"], 4);
    assert_eq!(json["status"]["terminal"], true);
    assert_eq!(json["payload_policy"], "full");
    assert_eq!(json["events"][1]["data"]["input"]["amount"], 42);

    let fixture_path = std::env::temp_dir().join(format!("autumn-history-export-{exec_id}.json"));
    std::fs::write(
        &fixture_path,
        serde_json::to_string_pretty(&json).expect("fixture JSON should render"),
    )
    .expect("fixture should be written");
    let fixture_json = std::fs::read_to_string(&fixture_path).expect("fixture should be readable");
    let report = WorkflowReplayer::new()
        .register_fn("billing_checkout", billing_checkout_workflow)
        .replay_from_json(&fixture_json)
        .await
        .expect("full API export should parse as a replay fixture");
    let _ = std::fs::remove_file(&fixture_path);

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "API-exported fixture must replay successfully: {report}"
    );
}

#[tokio::test]
async fn batch_redacted_history_export_filters_and_reports_shard_coverage() {
    let Some(((shard0_url, shard1_url), _container)) = setup_two_shards_with_migrations().await
    else {
        return;
    };
    insert_execution(
        &shard0_url,
        ShardId::new(0),
        "billing_checkout",
        "fixture-batch",
        "COMPLETED",
        vec![WorkflowEvent::WorkflowStarted {
            input: json!({ "authorization": "Bearer do-not-export" }),
            timestamp: Utc::now(),
        }],
    )
    .await;
    insert_execution(
        &shard1_url,
        ShardId::new(1),
        "other_workflow",
        "fixture-other",
        "COMPLETED",
        vec![WorkflowEvent::WorkflowStarted {
            input: json!({ "not": "selected" }),
            timestamp: Utc::now(),
        }],
    )
    .await;

    let app = build_api_app(
        build_two_shard_pool(&shard0_url, &shard1_url),
        two_shard_router(),
    );
    let (status, json) = get_json(
        &app,
        "/admin/history/exports?workflow_name=billing_checkout&state_group=terminal&limit=1000&payload_policy=redacted",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "complete");
    assert_eq!(json["payload_policy"], "redacted");
    assert_eq!(json["exports"].as_array().expect("exports array").len(), 1);
    assert_eq!(json["exports"][0]["workflow_name"], "billing_checkout");
    assert_eq!(
        json["exports"][0]["events"][0]["data"]["input"]["redacted"],
        true
    );
    assert!(
        !json.to_string().contains("Bearer do-not-export"),
        "redacted batch export must not expose raw payload"
    );
    assert_eq!(json["shard_coverage"]["inspected_shards"], json!([0, 1]));
    assert_eq!(json["shard_coverage"]["matched_shards"], json!([0]));
    assert_eq!(json["shard_coverage"]["unavailable_shards"], json!([]));
}

#[tokio::test]
async fn batch_limit_is_applied_after_global_history_timestamp_ordering() {
    let Some(((shard0_url, shard1_url), _container)) = setup_two_shards_with_migrations().await
    else {
        return;
    };
    let base = Utc::now() - Duration::days(7);
    let older_shard0 = insert_execution(
        &shard0_url,
        ShardId::new(0),
        "billing_checkout",
        "older-shard0",
        "COMPLETED",
        vec![WorkflowEvent::WorkflowStarted {
            input: json!({ "case": "older-shard0" }),
            timestamp: Utc::now(),
        }],
    )
    .await;
    let newer_shard0 = insert_execution(
        &shard0_url,
        ShardId::new(0),
        "billing_checkout",
        "newer-shard0",
        "COMPLETED",
        vec![WorkflowEvent::WorkflowStarted {
            input: json!({ "case": "newer-shard0" }),
            timestamp: Utc::now(),
        }],
    )
    .await;
    let newest_shard1 = insert_execution(
        &shard1_url,
        ShardId::new(1),
        "billing_checkout",
        "newest-shard1",
        "COMPLETED",
        vec![WorkflowEvent::WorkflowStarted {
            input: json!({ "case": "newest-shard1" }),
            timestamp: Utc::now(),
        }],
    )
    .await;
    set_execution_and_event_times(&shard0_url, older_shard0, base, base).await;
    set_execution_and_event_times(&shard0_url, newer_shard0, base, base + Duration::minutes(1))
        .await;
    set_execution_and_event_times(
        &shard1_url,
        newest_shard1,
        base,
        base + Duration::minutes(2),
    )
    .await;

    let app = build_api_app(
        build_two_shard_pool(&shard0_url, &shard1_url),
        two_shard_router(),
    );
    let (status, json) = get_json(
        &app,
        "/admin/history/exports?workflow_name=billing_checkout&state_group=terminal&limit=2&payload_policy=redacted",
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let exported_ids = json["exports"]
        .as_array()
        .expect("exports array")
        .iter()
        .map(|export| {
            export["execution_id"]
                .as_str()
                .expect("execution id")
                .to_string()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        exported_ids,
        vec![newest_shard1.to_string(), newer_shard0.to_string()]
    );
}

#[tokio::test]
async fn batch_updated_window_uses_latest_history_event_timestamp() {
    let Some((database_url, _container)) = setup_database_url_with_migrations().await else {
        return;
    };
    let old_row_time = Utc::now() - Duration::days(30);
    let recent_event_time = Utc::now();
    let exec_id = insert_execution(
        &database_url,
        ShardId::new(0),
        "billing_checkout",
        "recent-event-old-row",
        "RUNNING",
        vec![WorkflowEvent::WorkflowStarted {
            input: json!({ "case": "recent-event-old-row" }),
            timestamp: Utc::now(),
        }],
    )
    .await;
    set_running_execution_and_event_times(&database_url, exec_id, old_row_time, recent_event_time)
        .await;

    let app = build_api_app(
        HarvestDbPool::from(build_test_pool(&database_url)),
        ShardRouter::single(),
    );
    let updated_after =
        (recent_event_time - Duration::hours(1)).to_rfc3339_opts(SecondsFormat::Millis, true);
    let (status, json) = get_json(
        &app,
        format!(
            "/admin/history/exports?workflow_name=billing_checkout&state_group=active&updated_after={updated_after}&payload_policy=redacted"
        ),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let exported_ids = json["exports"]
        .as_array()
        .expect("exports array")
        .iter()
        .map(|export| {
            export["execution_id"]
                .as_str()
                .expect("execution id")
                .to_string()
        })
        .collect::<Vec<_>>();

    assert_eq!(exported_ids, vec![exec_id.to_string()]);
}
