//! Entity state queries read the projection, not a replay (issue #2259).
//!
//! `GET` and `POST /workflows/{id}/query/{name}` answer
//! `harvest.entity.state` and `harvest.entity.stats` from
//! `harvest_entity_state` when the queried run wrote the row. The body of
//! the workflow never runs. A run with no row still replays.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run these tests
//! without Docker.

#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::entity::{
    ENTITY_CHECKPOINT_SIDE_EFFECT, ENTITY_OP_SIGNAL, ENTITY_STATE_QUERY, ENTITY_STATE_SIDE_EFFECT,
    ENTITY_STATS_QUERY, Entity, EntityMessage,
};
use autumn_harvest::event::{SideEffectKind, WorkflowEvent};
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

/// How many times the body of [`counter`] started.
static BODY_RUNS: AtomicUsize = AtomicUsize::new(0);

/// A counter entity. It counts each start of its body.
fn counter<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        AtomicUsize::fetch_add(&BODY_RUNS, 1, Ordering::SeqCst);
        let state = Entity::<i64>::from_input(ctx, input)
            .run(|count, add: i64| async move { Ok(count + add) })
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(state))
    })
}

fn counter_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "entity_projection_counter",
        module: "tests",
        handler: counter,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
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
    }
}

async fn setup_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    (
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        Some(container),
    )
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![counter_info()], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("entity-projection-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state)
}

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-harvest-admin", "true");
    if body.is_some() {
        request = request.header("content-type", "application/json");
    }
    let body = body.map_or_else(Body::empty, |b| Body::from(b.to_string()));
    let response = app
        .clone()
        .oneshot(request.body(body).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

fn started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn signal(add: i64) -> WorkflowEvent {
    WorkflowEvent::SignalReceived {
        signal_name: ENTITY_OP_SIGNAL.to_string(),
        payload: json!(EntityMessage::op(add)),
    }
}

fn snapshot(state: i64, applied: u64) -> WorkflowEvent {
    WorkflowEvent::SideEffectRecorded {
        kind: SideEffectKind::EntityState,
        name: Some(ENTITY_STATE_SIDE_EFFECT.to_string()),
        value: json!({"state": state, "stats": {"applied": applied}}),
    }
}

fn checkpoint_record() -> WorkflowEvent {
    WorkflowEvent::SideEffectRecorded {
        kind: SideEffectKind::Custom,
        name: Some(ENTITY_CHECKPOINT_SIDE_EFFECT.to_string()),
        value: json!(false),
    }
}

/// Seed a run of [`counter`] in `state` with `input` and `events`.
async fn seed_run(
    pool: &DbPool,
    state: &str,
    input: Value,
    events: &[WorkflowEvent],
) -> ExecutionId {
    let mut conn = pool.get().await.expect("conn");
    let exec_id = ExecutionId::new();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, shard_id, input, queue_name, state) \
         VALUES ($1, 'entity_projection_counter', $2, 0, $3, 'default', $4)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .bind::<diesel::sql_types::Jsonb, _>(input)
    .bind::<diesel::sql_types::Text, _>(state)
    .execute(&mut conn)
    .await
    .expect("seed run");
    store::append_events(&mut conn, exec_id, events, 0)
        .await
        .expect("seed history");
    exec_id
}

/// Seed a `RUNNING` run of [`counter`] with `events`.
async fn seed(pool: &DbPool, events: &[WorkflowEvent]) -> ExecutionId {
    seed_run(pool, "RUNNING", json!({}), events).await
}

/// AC: queries on entity state read the projection without replaying the
/// body. This file holds one test, because [`BODY_RUNS`] is shared state.
#[tokio::test]
async fn entity_queries_read_the_projection() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // A run that recorded snapshots: the projection answers.
    let projected = seed(
        &pool,
        &[
            started(),
            snapshot(0, 0),
            signal(5),
            snapshot(5, 1),
            checkpoint_record(),
        ],
    )
    .await;
    AtomicUsize::store(&BODY_RUNS, 0, Ordering::SeqCst);
    let (status, state) = send(
        &app,
        "GET",
        &format!("/workflows/{projected}/query/{ENTITY_STATE_QUERY}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{state}");
    assert_eq!(state, json!(5));
    let (status, counters) = send(
        &app,
        "POST",
        &format!("/workflows/{projected}/query/{ENTITY_STATS_QUERY}"),
        Some(json!({"args": null})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{counters}");
    assert_eq!(counters["result"]["applied"], json!(1));
    assert_eq!(
        AtomicUsize::load(&BODY_RUNS, Ordering::SeqCst),
        0,
        "no query replayed the body"
    );

    // A run from before issue #2259 has no row: the query replays.
    let replayed = seed(&pool, &[started(), signal(7), checkpoint_record()]).await;
    let (status, state) = send(
        &app,
        "GET",
        &format!("/workflows/{replayed}/query/{ENTITY_STATE_QUERY}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{state}");
    assert_eq!(state, json!(7));
    assert!(
        AtomicUsize::load(&BODY_RUNS, Ordering::SeqCst) > 0,
        "a run with no row replays"
    );

    // Another query name is never served from the projection.
    AtomicUsize::store(&BODY_RUNS, 0, Ordering::SeqCst);
    let (status, _) = send(
        &app,
        "GET",
        &format!("/workflows/{projected}/query/progress"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        AtomicUsize::load(&BODY_RUNS, Ordering::SeqCst) > 0,
        "it replays"
    );

    // A bad body is still a 400 on the projected path.
    let (status, _) = send(
        &app,
        "POST",
        &format!("/workflows/{projected}/query/{ENTITY_STATE_QUERY}"),
        Some(json!("not an object")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // A terminal run keeps the replay path of issue #612: it replays, and it
    // gives the final state.
    let completed = seed_run(
        &pool,
        "COMPLETED",
        json!({}),
        &[
            started(),
            snapshot(0, 0),
            signal(4),
            snapshot(4, 1),
            checkpoint_record(),
            WorkflowEvent::SignalReceived {
                signal_name: ENTITY_OP_SIGNAL.to_string(),
                payload: json!(EntityMessage::<i64>::delete()),
            },
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::EntityState,
                name: Some(autumn_harvest::entity::ENTITY_DELETED_SIDE_EFFECT.to_string()),
                value: json!({"state": 4, "stats": {"applied": 1}}),
            },
            WorkflowEvent::WorkflowCompleted { output: json!(4) },
        ],
    )
    .await;
    AtomicUsize::store(&BODY_RUNS, 0, Ordering::SeqCst);
    let (status, state) = send(
        &app,
        "GET",
        &format!("/workflows/{completed}/query/{ENTITY_STATE_QUERY}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{state}");
    assert_eq!(state, json!(4));
    assert!(
        AtomicUsize::load(&BODY_RUNS, Ordering::SeqCst) > 0,
        "a terminal run replays"
    );

    // An erased terminal run is still a 410, before any projection read.
    let erased = seed_run(
        &pool,
        "COMPLETED",
        autumn_harvest::erase::erasure_tombstone(),
        &[started(), snapshot(0, 0)],
    )
    .await;
    let (status, body) = send(
        &app,
        "GET",
        &format!("/workflows/{erased}/query/{ENTITY_STATE_QUERY}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::GONE, "{body}");
}
