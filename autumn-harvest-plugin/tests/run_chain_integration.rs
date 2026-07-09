//! Integration tests for `GET /api/harvest/workflows/{id}/run-chain` (issue #701).
//!
//! Tests run against a real Postgres instance (testcontainers) and exercise:
//!   (1) 404 for an unknown execution id.
//!   (2) 400 for a malformed execution id.
//!   (3) A fully post-feature 4-run continue-as-new chain resolves to the same
//!       ordered chain from the head, a middle run, and the tail.
//!   (4) A standalone never-continued run resolves to a single-entry chain.
//!   (5) A pure legacy chain (rows lack the back-link columns, linked only by
//!       `WorkflowContinuedAsNew` events) resolves best-effort with
//!       `head_unknown = true` and no 500.
//!
//! The pure ordering / `head_unknown` math is unit-tested in
//! `autumn-harvest/src/run_chain.rs`; these tests verify the HTTP wiring plus
//! the DB gather (indexed back-link query + legacy forward-walk).

#![allow(clippy::too_many_lines, clippy::similar_names)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::{harvest_events, harvest_workflow_executions};
use autumn_harvest::shard::ShardRouter;
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
use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260619000000_harvest_task_queue_created_at/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260616000001_harvest_workflow_schedule_id/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260430000000_harvest_workflow_schedules/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508000000_harvest_external_task_updated_at/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508010000_harvest_workers_drain_deadline/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260518000000_harvest_signal_idempotency/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260519000000_harvest_calendar_awareness/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260522000000_harvest_schedule_decisions/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260526000001_harvest_parent_close_policy/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260601000001_harvest_poison_pill_strikes/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260601000002_harvest_ownership_metadata/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260603000000_harvest_completion_triggers/up.sql"
    ),
    include_str!(
        "../../autumn-harvest/migrations/20260708000001_harvest_completion_trigger_condition/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"
    ),
    include_str!(
        "../../autumn-harvest/migrations/20260607000000_harvest_worker_capability_labels/up.sql"
    ),
    include_str!(
        "../../autumn-harvest/migrations/20260607000001_harvest_task_required_capabilities/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260609000001_harvest_workflow_current_details/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260613000001_harvest_schedule_catchup_window/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../autumn-harvest/migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260705000000_harvest_completion_deliveries/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260706000000_harvest_worker_sessions/up.sql"),
    include_str!(
        "../../autumn-harvest/migrations/20260709000000_harvest_workflow_continue_chain/up.sql"
    ),
);

type HarvestApiApp = axum::Router;

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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
        Some("run-chain-test".to_string()),
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

/// Seed one execution row in a chosen state, then overwrite its lifecycle
/// timestamps and the continue-as-new back-link columns directly. Each run is
/// started under a distinct `workflow_id` so `start_or_load_workflow_execution`
/// never trips the active-run uniqueness index; the run-chain gather resolves by
/// `id`/`first_exec_id`, not `workflow_id`, so distinct ids are fine.
async fn seed_run(
    conn: &mut AsyncPgConnection,
    workflow_id: &str,
    state: &str,
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    continued_from: Option<ExecutionId>,
    first: Option<ExecutionId>,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "run-chain-wf",
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
        },
    )
    .await
    .expect("seed workflow");

    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq(state),
            harvest_workflow_executions::started_at.eq(started_at),
            harvest_workflow_executions::completed_at.eq(completed_at),
            harvest_workflow_executions::continued_from_exec_id
                .eq(continued_from.map(|e| e.as_uuid())),
            harvest_workflow_executions::first_exec_id.eq(first.map(|e| e.as_uuid())),
        ))
        .execute(conn)
        .await
        .unwrap();
    exec_id
}

/// Append a `WorkflowContinuedAsNew` event to `exec_id`'s history pointing at
/// `successor` — the only forward link a pre-feature (legacy) chain carries.
async fn insert_continue_event(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    event_id: i32,
    successor: ExecutionId,
) {
    let event = WorkflowEvent::WorkflowContinuedAsNew {
        new_exec_id: successor,
        input: json!({"n": 2}),
    };
    let data = serde_json::to_value(&event).expect("event serializes");
    let event_type = data
        .get("type")
        .and_then(Value::as_str)
        .expect("adjacently-tagged event carries a 'type'")
        .to_string();
    diesel::insert_into(harvest_events::table)
        .values((
            harvest_events::workflow_exec_id.eq(exec_id.as_uuid()),
            harvest_events::event_id.eq(event_id),
            harvest_events::event_type.eq(event_type),
            harvest_events::event_data.eq(data),
            harvest_events::timestamp.eq(Utc::now()),
        ))
        .execute(conn)
        .await
        .unwrap();
}

fn run_ids(body: &Value) -> Vec<String> {
    body["runs"]
        .as_array()
        .expect("runs is an array")
        .iter()
        .map(|r| r["exec_id"].as_str().unwrap().to_string())
        .collect()
}

/// (1) Unknown execution id → 404.
#[tokio::test]
async fn run_chain_unknown_id_returns_404() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let unknown = ExecutionId::new_for_shard(ShardId::new(0));
    let (status, body) = get_json(&app, &format!("/workflows/{unknown}/run-chain")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // A structured problem-details error body, not an empty 404.
    let detail = body["detail"]
        .as_str()
        .unwrap_or_else(|| panic!("404 must carry a JSON problem-details body, got: {body}"));
    assert!(
        detail.contains("not found"),
        "404 detail should name the missing execution, got: {detail}"
    );
}

/// (2) Malformed execution id → 400.
#[tokio::test]
async fn run_chain_malformed_id_returns_400() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = get_json(&app, "/workflows/not-a-uuid/run-chain").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // A structured problem-details error body, not an empty 400.
    assert!(
        body["detail"].as_str().is_some_and(|d| !d.is_empty()),
        "400 must carry a non-empty JSON problem-details body, got: {body}"
    );
}

/// (3) A fully post-feature 4-run chain (3 `CONTINUED_AS_NEW` + 1 `RUNNING` tail,
/// each successor carrying `continued_from_exec_id` + `first_exec_id`) resolves
/// to the SAME ordered chain from the head, a middle run, and the tail.
#[tokio::test]
async fn run_chain_three_can_transitions_from_head_middle_and_tail() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let t0 = Utc::now() - Duration::seconds(400);
    // run0 = origin; run1, run2 = middle; run3 = live tail.
    let run0 = seed_run(
        &mut conn,
        "chain-run-0",
        "CONTINUED_AS_NEW",
        t0,
        Some(t0 + Duration::seconds(10)),
        None,
        None,
    )
    .await;
    let run1 = seed_run(
        &mut conn,
        "chain-run-1",
        "CONTINUED_AS_NEW",
        t0 + Duration::seconds(100),
        Some(t0 + Duration::seconds(110)),
        Some(run0),
        Some(run0),
    )
    .await;
    let run2 = seed_run(
        &mut conn,
        "chain-run-2",
        "CONTINUED_AS_NEW",
        t0 + Duration::seconds(200),
        Some(t0 + Duration::seconds(210)),
        Some(run1),
        Some(run0),
    )
    .await;
    let run3 = seed_run(
        &mut conn,
        "chain-run-3",
        "RUNNING",
        t0 + Duration::seconds(300),
        None,
        Some(run2),
        Some(run0),
    )
    .await;

    // Realistic forward links in history (the gather must not need them for a
    // post-feature chain, but they should be a harmless no-op for the walk).
    insert_continue_event(&mut conn, run0, 100, run1).await;
    insert_continue_event(&mut conn, run1, 100, run2).await;
    insert_continue_event(&mut conn, run2, 100, run3).await;

    let expected = vec![
        run0.to_string(),
        run1.to_string(),
        run2.to_string(),
        run3.to_string(),
    ];

    for (label, member) in [("head", run0), ("middle", run1), ("tail", run3)] {
        let (status, body) = get_json(&app, &format!("/workflows/{member}/run-chain")).await;
        assert_eq!(status, StatusCode::OK, "resolving from {label}");
        assert_eq!(
            body["head_unknown"], false,
            "post-feature chain head is confirmed ({label})"
        );
        assert_eq!(run_ids(&body), expected, "same ordered chain from {label}");

        let runs = body["runs"].as_array().unwrap();
        assert_eq!(runs.len(), 4);
        for (i, run) in runs.iter().enumerate() {
            assert_eq!(run["sequence"], i as u64, "sequence {i} ({label})");
        }
        assert_eq!(runs[0]["outcome"], "continued_as_new");
        assert_eq!(runs[3]["outcome"], "running");
        assert_eq!(
            runs[0]["continued_to_exec_id"].as_str(),
            Some(run1.to_string().as_str()),
            "run0 forward link ({label})"
        );
        assert_eq!(
            runs[2]["continued_to_exec_id"].as_str(),
            Some(run3.to_string().as_str()),
            "run2 forward link ({label})"
        );
        assert!(
            runs[3]["continued_to_exec_id"].is_null(),
            "tail has no forward link ({label})"
        );
    }
}

/// (4) A standalone never-continued run → single-entry chain, head known.
#[tokio::test]
async fn run_chain_standalone_run_is_single_entry() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let t0 = Utc::now() - Duration::seconds(30);
    let run = seed_run(
        &mut conn,
        "standalone",
        "COMPLETED",
        t0,
        Some(t0 + Duration::seconds(5)),
        None,
        None,
    )
    .await;

    let (status, body) = get_json(&app, &format!("/workflows/{run}/run-chain")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["head_unknown"], false);
    let runs = body["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["sequence"], 0);
    assert_eq!(runs[0]["outcome"], "completed");
    assert!(runs[0]["continued_to_exec_id"].is_null());
}

/// (5) A pure legacy chain: rows carry NO back-links (both columns NULL) and are
/// linked only by `WorkflowContinuedAsNew` events; the head is `CONTINUED_AS_NEW`.
/// The gather forward-walks the events and the assembler flags `head_unknown`.
#[tokio::test]
async fn run_chain_legacy_chain_flags_head_unknown() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let t0 = Utc::now() - Duration::seconds(300);
    // Legacy rows: continued_from + first_exec_id are both NULL.
    let run0 = seed_run(
        &mut conn,
        "legacy-run-0",
        "CONTINUED_AS_NEW",
        t0,
        Some(t0 + Duration::seconds(10)),
        None,
        None,
    )
    .await;
    let run1 = seed_run(
        &mut conn,
        "legacy-run-1",
        "CONTINUED_AS_NEW",
        t0 + Duration::seconds(100),
        Some(t0 + Duration::seconds(110)),
        None,
        None,
    )
    .await;
    let run2 = seed_run(
        &mut conn,
        "legacy-run-2",
        "RUNNING",
        t0 + Duration::seconds(200),
        None,
        None,
        None,
    )
    .await;

    // The only forward links a legacy chain has.
    insert_continue_event(&mut conn, run0, 100, run1).await;
    insert_continue_event(&mut conn, run1, 100, run2).await;

    let (status, body) = get_json(&app, &format!("/workflows/{run0}/run-chain")).await;
    assert_eq!(status, StatusCode::OK, "legacy chain must not 500");
    assert_eq!(
        body["head_unknown"], true,
        "legacy chain cannot confirm its origin"
    );
    assert_eq!(
        run_ids(&body),
        vec![run0.to_string(), run1.to_string(), run2.to_string()],
        "legacy chain ordered by started_at via forward-walk"
    );
    let runs = body["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 3);
    assert_eq!(
        runs[0]["continued_to_exec_id"].as_str(),
        Some(run1.to_string().as_str())
    );
    assert!(runs[2]["continued_to_exec_id"].is_null());
    assert_eq!(runs[2]["outcome"], "running");
}

/// (6) A pure legacy chain queried from a MIDDLE run. The gather can only walk
/// FORWARD via `WorkflowContinuedAsNew` events (legacy rows have no back-links to
/// reach the origin), so it resolves the runs from the middle onward and flags
/// `head_unknown = true` — documented graceful degradation, never a 500.
#[tokio::test]
async fn run_chain_legacy_middle_run_flags_head_unknown() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();

    let t0 = Utc::now() - Duration::seconds(300);
    // Legacy rows: continued_from + first_exec_id are both NULL.
    let run0 = seed_run(
        &mut conn,
        "legacy-mid-run-0",
        "CONTINUED_AS_NEW",
        t0,
        Some(t0 + Duration::seconds(10)),
        None,
        None,
    )
    .await;
    let run1 = seed_run(
        &mut conn,
        "legacy-mid-run-1",
        "CONTINUED_AS_NEW",
        t0 + Duration::seconds(100),
        Some(t0 + Duration::seconds(110)),
        None,
        None,
    )
    .await;
    let run2 = seed_run(
        &mut conn,
        "legacy-mid-run-2",
        "RUNNING",
        t0 + Duration::seconds(200),
        None,
        None,
        None,
    )
    .await;

    // Forward links: run0 -> run1 -> run2.
    insert_continue_event(&mut conn, run0, 100, run1).await;
    insert_continue_event(&mut conn, run1, 100, run2).await;

    // Query from the MIDDLE run (run1).
    let (status, body) = get_json(&app, &format!("/workflows/{run1}/run-chain")).await;
    assert_eq!(status, StatusCode::OK, "legacy middle query must not 500");
    assert_eq!(
        body["head_unknown"], true,
        "a legacy chain resolved from the middle cannot confirm its origin"
    );
    // Forward-only from the queried middle run: run0 (the origin) is unreachable
    // because legacy rows carry no back-link to walk up to it.
    assert_eq!(
        run_ids(&body),
        vec![run1.to_string(), run2.to_string()],
        "runs resolvable forward from the middle run"
    );
    let runs = body["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0]["sequence"], 0);
    assert_eq!(
        runs[0]["continued_to_exec_id"].as_str(),
        Some(run2.to_string().as_str())
    );
    assert!(runs[1]["continued_to_exec_id"].is_null());
    assert_eq!(runs[1]["outcome"], "running");
}
