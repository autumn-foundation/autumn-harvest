//! Integration tests for `GET /api/harvest/workflows/{id}/history` (issue #529).
//!
//! Tests run against a real Postgres instance (testcontainers) and exercise:
//!   (a) Page boundaries: `limit=10`, follow `next_cursor`, no drops/duplicates.
//!   (b) `event_type` filter: only matching types returned; `total_events` is unfiltered.
//!   (c) Append-during-paging stability: new events after page 1 visible on page 2.
//!   (d) 404 for unknown execution id.
//!   (e) 400 for malformed `after` cursor and non-integer `limit`.
//!   (f) `get_workflow` truncation: `history.len()` <= 100, `history_truncated=true`, `history_endpoint` present.

#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, TimerId};
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{
    StartWorkflowParams, WorkflowEvent, WorkflowIdReusePolicy, start_or_load_workflow_execution,
};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("history-pag-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("x-harvest-admin", "true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("GET request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

async fn seed_execution(conn: &mut AsyncPgConnection, workflow_id: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "history-pag-wf",
            workflow_id,
            exec_id,
            input: json!({"n": 1}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::default(),
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("seed workflow");
    exec_id
}

/// Append N `TimerStarted` events then N `TimerFired` events and return the
/// total appended count (not counting the initial `WorkflowStarted` row).
async fn append_mixed_events(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    n_per_type: usize,
) -> usize {
    let history = store::load_history(conn, exec_id).await.unwrap();
    let mut next_id = history.next_event_id;
    let mut events: Vec<WorkflowEvent> = Vec::new();

    for i in 0..n_per_type {
        events.push(WorkflowEvent::TimerStarted {
            timer_id: TimerId::new(format!("t{i}")),
            duration_secs: 60,
        });
    }
    for i in 0..n_per_type {
        events.push(WorkflowEvent::TimerFired {
            timer_id: TimerId::new(format!("t{i}")),
        });
    }

    store::append_events(conn, exec_id, &events, next_id)
        .await
        .expect("append events");
    next_id += i32::try_from(events.len()).expect("event count fits i32");
    let _ = next_id;
    events.len()
}

/// Append N `ActivityScheduled` events, returning how many were appended.
async fn append_activity_events(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    n: usize,
) -> usize {
    let history = store::load_history(conn, exec_id).await.unwrap();
    let mut events: Vec<WorkflowEvent> = Vec::new();
    for i in 0..n {
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: format!("act{i}"),
            input: json!(null),
            queue: "default".to_string(),
        });
    }
    store::append_events(conn, exec_id, &events, history.next_event_id)
        .await
        .expect("append activity events");
    events.len()
}

// ─── RED phase tests ──────────────────────────────────────────────────────────

/// (d) Unknown execution id → 404.
#[tokio::test]
async fn history_unknown_id_returns_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let unknown = ExecutionId::new_for_shard(ShardId::new(0));
    let (status, _body) = get_json(&app, &format!("/workflows/{unknown}/history")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// (e) Malformed `after` cursor → 400.
#[tokio::test]
async fn history_malformed_cursor_returns_400() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "bad-cursor-wf").await;
    let (status, _body) = get_json(
        &app,
        &format!("/workflows/{exec_id}/history?after=not-a-number"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// (e) Non-integer `limit` → 400.
#[tokio::test]
async fn history_non_integer_limit_returns_400() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "bad-limit-wf").await;
    let (status, _body) = get_json(&app, &format!("/workflows/{exec_id}/history?limit=abc")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// (a) Page boundaries: `limit=5`, follow `next_cursor` to drain all events, no
/// drops/duplicates, last page has `next_cursor=null`.
#[tokio::test]
async fn history_page_boundaries_no_drops_or_duplicates() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "page-bounds-wf").await;
    // Append 15 extra events (on top of the initial WorkflowStarted) = 16 total.
    append_mixed_events(&mut conn, exec_id, 7).await; // 7 TimerStarted + 7 TimerFired = 14
    // Also append 2 more so total is 1 + 14 + 1 = 16; actually let's just use 15 total extra.
    // WorkflowStarted is event 0 → +14 = 15 events total.

    let limit = 5;
    let mut collected_ids: Vec<i64> = Vec::new();
    let mut cursor = String::new();
    let mut page_count = 0;

    loop {
        let uri = if cursor.is_empty() {
            format!("/workflows/{exec_id}/history?limit={limit}")
        } else {
            format!("/workflows/{exec_id}/history?limit={limit}&after={cursor}")
        };
        let (status, body) = get_json(&app, &uri).await;
        assert_eq!(status, StatusCode::OK, "page {page_count}: body={body}");

        let events = body["events"].as_array().expect("events must be array");
        assert!(
            events.len() <= limit,
            "page must not exceed limit, got {}",
            events.len()
        );

        for event in events {
            let id = event["id"].as_i64().expect("each event must have i64 id");
            assert!(
                !collected_ids.contains(&id),
                "duplicate row id {id} on page {page_count}"
            );
            collected_ids.push(id);
        }

        page_count += 1;

        match body["next_cursor"].as_str() {
            Some(nc) if !nc.is_empty() => cursor = nc.to_string(),
            _ => break,
        }
    }

    assert!(page_count > 1, "must have needed more than one page");
    assert!(!collected_ids.is_empty(), "must have collected events");
    // All IDs must be in ascending order across pages.
    let mut sorted = collected_ids.clone();
    sorted.sort_unstable();
    assert_eq!(
        collected_ids, sorted,
        "event row ids must be in ascending order across pages"
    );
    // 15 total events (1 WorkflowStarted + 14 timer events).
    assert_eq!(collected_ids.len(), 15, "must have collected all 15 events");
}

/// (b) `event_type` filter: only matching types returned; `total_events` is the
/// unfiltered count.
#[tokio::test]
async fn history_event_type_filter_returns_only_matching() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "filter-wf").await;
    // 5 TimerStarted + 5 TimerFired = 10, plus 1 WorkflowStarted = 11 total.
    append_mixed_events(&mut conn, exec_id, 5).await;

    // Filter to only TimerStarted.
    let (status, body) = get_json(
        &app,
        &format!("/workflows/{exec_id}/history?event_type=TimerStarted&limit=100"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let events = body["events"].as_array().expect("events array");
    assert_eq!(
        events.len(),
        5,
        "only TimerStarted events should be returned"
    );
    for ev in events {
        assert_eq!(
            ev["type"].as_str(),
            Some("TimerStarted"),
            "all filtered events must be TimerStarted"
        );
    }

    // total_events reflects ALL events (unfiltered).
    let total = body["total_events"]
        .as_i64()
        .expect("total_events must be i64");
    assert_eq!(
        total, 11,
        "total_events must count all events including filtered-out ones"
    );
}

/// (b) Multiple `event_type` values (repeatable param).
#[tokio::test]
async fn history_multiple_event_type_filters() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "multi-filter-wf").await;
    append_mixed_events(&mut conn, exec_id, 3).await; // 3 TimerStarted + 3 TimerFired + 1 WorkflowStarted = 7

    let (status, body) = get_json(
        &app,
        &format!(
            "/workflows/{exec_id}/history?event_type=TimerStarted&event_type=TimerFired&limit=100"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let events = body["events"].as_array().expect("events array");
    assert_eq!(events.len(), 6, "TimerStarted + TimerFired = 6");
    for ev in events {
        let t = ev["type"].as_str().unwrap_or("");
        assert!(
            t == "TimerStarted" || t == "TimerFired",
            "unexpected event type: {t}"
        );
    }
}

/// (c) Append-during-paging stability: already-returned rows are not re-emitted
/// and newly appended rows (higher id) are reachable via cursor.
#[tokio::test]
async fn history_append_during_paging_stability() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "append-paging-wf").await;
    // Seed 5 events (1 WorkflowStarted already). Append 4 more.
    append_activity_events(&mut conn, exec_id, 4).await;
    // Total = 5 events.

    // Page 1: limit=3 → get 3 rows, capture cursor.
    let (status, page1) = get_json(&app, &format!("/workflows/{exec_id}/history?limit=3")).await;
    assert_eq!(status, StatusCode::OK, "page1: {page1}");
    let page1_events = page1["events"].as_array().unwrap();
    assert_eq!(page1_events.len(), 3);
    let cursor = page1["next_cursor"]
        .as_str()
        .expect("must have next_cursor");
    let page1_ids: Vec<i64> = page1_events
        .iter()
        .map(|e| e["id"].as_i64().unwrap())
        .collect();

    // Append 3 more events while "between pages".
    append_activity_events(&mut conn, exec_id, 3).await;

    // Page 2: use cursor — must not see page1's rows; must see remaining old rows + new rows.
    let (status, page2) = get_json(
        &app,
        &format!("/workflows/{exec_id}/history?limit=100&after={cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "page2: {page2}");
    let page2_events = page2["events"].as_array().unwrap();

    // No page-1 ID should appear in page 2.
    for ev in page2_events {
        let id = ev["id"].as_i64().unwrap();
        assert!(
            !page1_ids.contains(&id),
            "row id {id} from page 1 re-appeared on page 2"
        );
    }

    // We started with 5 rows, appended 3 more = 8 total. Page1 took 3, so page2 must have 5.
    assert_eq!(
        page2_events.len(),
        5,
        "page2 must contain remaining 2 old + 3 new = 5 events"
    );
}

/// (f) `get_workflow` truncation: when >100 events, `history.len()` <= 100,
/// `history_truncated=true`, `history_endpoint` is present.
#[tokio::test]
async fn get_workflow_history_is_truncated_when_over_100_events() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "trunc-wf").await;
    // Append 110 events to push over the 100-event page default.
    append_activity_events(&mut conn, exec_id, 110).await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let history = body["history"].as_array().expect("history must be array");
    assert!(
        history.len() <= 100,
        "get_workflow history must be bounded to <= 100 events, got {}",
        history.len()
    );

    assert_eq!(
        body["history_truncated"].as_bool(),
        Some(true),
        "history_truncated must be true when events exceed page size"
    );

    let endpoint = body["history_endpoint"]
        .as_str()
        .expect("history_endpoint must be present");
    assert!(
        endpoint.contains("/history"),
        "history_endpoint must reference the history sub-route, got: {endpoint}"
    );
}

/// Response shape: each event in the history page must have `id`, `event_id`, timestamp, type, data.
#[tokio::test]
async fn history_response_shape_has_required_fields() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "shape-wf").await;

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/history?limit=10")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    // Top-level response fields.
    assert!(
        body["events"].is_array(),
        "response must have 'events' array"
    );
    assert!(
        body["total_events"].is_number(),
        "response must have 'total_events' number"
    );
    assert!(
        body["last_event_id"].is_number(),
        "response must have 'last_event_id' number"
    );
    // next_cursor may be null (last page) or a string.
    assert!(
        body["next_cursor"].is_null() || body["next_cursor"].is_string(),
        "next_cursor must be null or string"
    );

    // Per-event fields.
    let events = body["events"].as_array().unwrap();
    assert!(!events.is_empty(), "must have at least WorkflowStarted");
    let first = &events[0];
    assert!(first["id"].is_number(), "each event must have 'id'");
    assert!(
        first["event_id"].is_number(),
        "each event must have 'event_id'"
    );
    assert!(
        first["timestamp"].is_string(),
        "each event must have 'timestamp'"
    );
    assert!(first["type"].is_string(), "each event must have 'type'");
    assert!(first["data"].is_object(), "each event must have 'data'");
}

/// Unknown `event_type` names yield an empty events page (not a 400).
#[tokio::test]
async fn history_unknown_event_type_yields_empty_page() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "unknown-type-wf").await;

    let (status, body) = get_json(
        &app,
        &format!("/workflows/{exec_id}/history?event_type=NonExistentEventType"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let events = body["events"].as_array().unwrap();
    assert!(
        events.is_empty(),
        "unknown event_type must yield empty page"
    );
    // total_events still reflects the real total.
    let total = body["total_events"].as_i64().unwrap();
    assert_eq!(
        total, 1,
        "total_events counts WorkflowStarted regardless of filter"
    );
}
