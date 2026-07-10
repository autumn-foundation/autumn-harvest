//! Integration tests for `GET /workflows/count` (issue #544).
//!
//! Seeds workflow executions across states and workflow types and asserts the
//! endpoint groups by `state`/`workflow_name` with a real per-shard SQL
//! `GROUP BY`, applies filters before grouping, caps cardinality with an
//! `other` rollup, merges across shards, and degrades gracefully (never a
//! 500) when a shard is unreachable.

use std::collections::BTreeMap;

use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Duration, Utc};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_set_at TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_until TIMESTAMPTZ NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_reason TEXT NULL;\n",
    "ALTER TABLE harvest_workflow_executions ADD COLUMN IF NOT EXISTS legal_hold_actor TEXT NULL;\n",
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
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508010000_harvest_workers_drain_deadline/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260508000000_harvest_external_task_updated_at/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
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
    include_str!("../../autumn-harvest/migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"
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
        "../../autumn-harvest/migrations/20260601000000_harvest_schedule_auto_pause/up.sql"
    ),
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
    "\n",
    include_str!("../../autumn-harvest/migrations/20260605000000_harvest_admission_gates/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260607000000_harvest_worker_capability_labels/up.sql"
    ),
    "\n",
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
        "../../autumn-harvest/migrations/20260710000002_harvest_workflow_continue_chain/up.sql"
    ),
);

type HarvestApiApp = axum::Router;

async fn setup_single_shard() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, container)
}

async fn setup_two_shards() -> ((String, String), ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let s0 = format!("harvest_shard_{}", Uuid::new_v4().simple());
    let s1 = format!("harvest_shard_{}", Uuid::new_v4().simple());

    let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("admin connect");
    for db in [&s0, &s1] {
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create db");
    }
    let url0 = format!("postgres://postgres:postgres@{host}:{port}/{s0}");
    let url1 = format!("postgres://postgres:postgres@{host}:{port}/{s1}");
    for url in [&url0, &url1] {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
            .await
            .expect("shard connect");
        diesel_async::SimpleAsyncConnection::batch_execute(&mut conn, INIT_SQL)
            .await
            .expect("migrate shard");
    }
    ((url0, url1), container)
}

fn build_pool(url: &str) -> DbPool {
    let manager =
        diesel_async::pooled_connection::AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            url,
        );
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool")
}

fn build_app(storage: HarvestDbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(storage);
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn single_app(url: &str) -> HarvestApiApp {
    build_app(HarvestDbPool::from(build_pool(url)))
}

fn two_shard_storage(url0: &str, url1: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(url0));
    pools.insert(ShardId::new(1), build_pool(url1));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

/// Seed one workflow execution on `url`/`shard` and force its `state`/`started_at`.
async fn seed_execution(
    url: &str,
    shard: i32,
    workflow_name: &str,
    state: &str,
    started_at: DateTime<Utc>,
) -> Uuid {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(shard));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let wf_id = format!("{workflow_name}-{}", Uuid::new_v4().simple());
    let row = NewWorkflowExecution {
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name,
        workflow_id: &wf_id,
        run_id: Uuid::new_v4(),
        shard_id: shard,
        input: serde_json::json!({}),
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
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
        completion_callbacks: None,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert");

    let completed_at = if state == "RUNNING" {
        None
    } else {
        Some(started_at + Duration::seconds(5))
    };
    diesel::update(dsl::harvest_workflow_executions.filter(dsl::id.eq(exec_id.as_uuid())))
        .set((
            dsl::state.eq(state),
            dsl::started_at.eq(started_at),
            dsl::completed_at.eq(completed_at),
        ))
        .execute(&mut conn)
        .await
        .expect("force state");
    exec_id.as_uuid()
}

#[tokio::test]
async fn empty_fleet_returns_complete_zero_total() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let (status, body) = get_json(&app, "/workflows/count").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "complete");
    assert_eq!(body["total"], 0);
    assert_eq!(body["groups"].as_array().unwrap().len(), 0);
    assert!(body["as_of"].is_string());
}

#[tokio::test]
async fn default_group_by_is_state() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();

    seed_execution(&url, 0, "onboarding", "RUNNING", now).await;
    seed_execution(&url, 0, "onboarding", "RUNNING", now).await;
    seed_execution(&url, 0, "billing", "FAILED", now).await;

    let (status, body) = get_json(&app, "/workflows/count").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 3);
    let groups = body["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 2, "grouped by state only: RUNNING + FAILED");
    for g in groups {
        assert!(
            g.get("workflow_name").is_none(),
            "workflow_name omitted when not grouped by it"
        );
    }
    let running = groups.iter().find(|g| g["state"] == "RUNNING").unwrap();
    assert_eq!(running["count"], 2);
}

#[tokio::test]
async fn group_by_state_and_workflow_name() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();

    seed_execution(&url, 0, "onboarding", "RUNNING", now).await;
    seed_execution(&url, 0, "onboarding", "RUNNING", now).await;
    seed_execution(&url, 0, "billing", "FAILED", now).await;

    let (status, body) = get_json(&app, "/workflows/count?group_by=state,workflow_name").await;
    assert_eq!(status, StatusCode::OK);
    let groups = body["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 2);
    let onboarding = groups
        .iter()
        .find(|g| g["workflow_name"] == "onboarding")
        .unwrap();
    assert_eq!(onboarding["state"], "RUNNING");
    assert_eq!(onboarding["count"], 2);
}

#[tokio::test]
async fn filters_apply_before_grouping() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();

    seed_execution(&url, 0, "onboarding", "RUNNING", now).await;
    seed_execution(&url, 0, "billing", "RUNNING", now).await;
    seed_execution(&url, 0, "billing", "FAILED", now).await;

    let (status, body) = get_json(
        &app,
        "/workflows/count?workflow_name=billing&group_by=state",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 2, "only billing executions counted");
    let groups = body["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 2);
}

#[tokio::test]
async fn state_filter_narrows_before_grouping() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();

    seed_execution(&url, 0, "onboarding", "RUNNING", now).await;
    seed_execution(&url, 0, "onboarding", "FAILED", now).await;
    seed_execution(&url, 0, "onboarding", "COMPLETED", now).await;

    let (status, body) = get_json(&app, "/workflows/count?state=RUNNING,FAILED").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 2);
}

#[tokio::test]
async fn started_after_and_before_bound_the_window() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let base = Utc::now() - Duration::hours(3);

    seed_execution(&url, 0, "onboarding", "RUNNING", base).await;
    seed_execution(&url, 0, "onboarding", "RUNNING", base + Duration::hours(1)).await;
    seed_execution(&url, 0, "onboarding", "RUNNING", base + Duration::hours(2)).await;

    let after = base + Duration::minutes(30);
    let before = base + Duration::hours(1) + Duration::minutes(30);
    let (status, body) = get_json(
        &app,
        &format!(
            "/workflows/count?started_after={}&started_before={}",
            after.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            before.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 1, "only the middle execution is in-window");
}

#[tokio::test]
async fn invalid_group_by_is_bad_request() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let (status, _) = get_json(&app, "/workflows/count?group_by=queue_name").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn invalid_state_filter_is_bad_request() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let (status, _) = get_json(&app, "/workflows/count?state=BOGUS").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bounded_cardinality_rolls_long_tail_into_other() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();

    for i in 0..5 {
        seed_execution(&url, 0, &format!("wf_{i}"), "RUNNING", now).await;
    }

    let (status, body) = get_json(
        &app,
        "/workflows/count?group_by=workflow_name&limit_groups=2",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["truncated"], true);
    let groups = body["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 3, "top 2 + one other rollup");
    let other = groups.iter().find(|g| g["other"] == true).unwrap();
    assert_eq!(other["count"], 3, "remaining 3 rolled up");
    let sum: i64 = groups.iter().map(|g| g["count"].as_i64().unwrap()).sum();
    assert_eq!(
        sum,
        body["total"].as_i64().unwrap(),
        "groups reconcile to total"
    );
}

#[tokio::test]
async fn merges_across_shards() {
    let ((url0, url1), _c) = setup_two_shards().await;
    let app = build_app(two_shard_storage(&url0, &url1));
    let now = Utc::now();

    seed_execution(&url0, 0, "onboarding", "RUNNING", now).await;
    seed_execution(&url1, 1, "onboarding", "RUNNING", now).await;
    seed_execution(&url1, 1, "onboarding", "FAILED", now).await;

    let (status, body) = get_json(&app, "/workflows/count").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "complete");
    assert_eq!(body["total"], 3);
    let groups = body["groups"].as_array().unwrap();
    let running = groups.iter().find(|g| g["state"] == "RUNNING").unwrap();
    assert_eq!(running["count"], 2, "summed across both shards");
}

#[tokio::test]
async fn one_shard_down_is_partial_not_500() {
    let ((url0, url1), _c) = setup_two_shards().await;
    seed_execution(&url0, 0, "onboarding", "RUNNING", Utc::now()).await;

    // Point shard 1 at a database that does not exist so its pool fails.
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url0));
    pools.insert(
        ShardId::new(1),
        build_pool(&url1.replace("harvest_shard_", "missing_db_")),
    );
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let app_partial = build_app(storage);

    let (status, body) = get_json(&app_partial, "/workflows/count").await;
    assert_eq!(status, StatusCode::OK, "a down shard must not 500");
    assert_eq!(body["status"], "partial");
    // The healthy shard's data still flows through.
    assert_eq!(body["total"], 1);
    let unavailable = body["unavailable_shards"].as_array().unwrap();
    assert_eq!(
        unavailable.len(),
        1,
        "the down shard is named in the report"
    );
    assert_eq!(unavailable[0]["shard_id"], 1);
    assert!(!unavailable[0]["reason"].as_str().unwrap().is_empty());
}
