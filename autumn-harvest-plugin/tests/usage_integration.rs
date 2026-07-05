//! Integration tests for `GET /admin/usage` (issue #596).
//!
//! Seeds workflow executions and hand-inserted `harvest_events` rows with
//! explicit timestamps, and asserts the endpoint aggregates workflow starts,
//! terminal outcomes, activity executions/failures, and activity
//! compute-seconds correctly, honors both `group_by` modes (including the
//! `"(unattributed)"` bucket), enforces the window ceiling, merges across
//! shards, and degrades gracefully (never a 500) when a shard is
//! unreachable.

use std::collections::BTreeMap;

use autumn_harvest::models::{NewHarvestEvent, NewWorkflowExecution};
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
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260702000000_harvest_usage_report_indexes/up.sql"
    ),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"
    ),
    include_str!("../../autumn-harvest/migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../autumn-harvest/migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260705000000_harvest_completion_deliveries/up.sql"
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

fn build_app_with_max_groups(storage: HarvestDbPool, max_groups: usize) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(storage);
    api_state.set_usage_max_groups(max_groups);
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

/// Seed one workflow execution on `url`/`shard`, forcing `state`,
/// `started_at`/`completed_at`, and (optionally) `search_attrs`.
#[allow(clippy::too_many_arguments)]
async fn seed_execution(
    url: &str,
    shard: i32,
    workflow_name: &str,
    state: &str,
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    search_attrs: Option<Value>,
) -> Uuid {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(shard));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let wf_id = format!("{workflow_name}-{}", Uuid::new_v4().simple());
    let row = NewWorkflowExecution {
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
        search_attrs: search_attrs.clone(),
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

/// Insert an `ActivityStarted`/`ActivityCompleted`/`ActivityFailed`/`ActivityTimedOut`
/// event on `exec_id` at an explicit `timestamp`, mirroring the adjacently-tagged
/// `WorkflowEvent` JSON shape (`{"type": .., "data": {"activity_id": ..}}`).
async fn seed_activity_event(
    url: &str,
    exec_id: Uuid,
    event_id: i32,
    event_type: &str,
    activity_id: Uuid,
    timestamp: DateTime<Utc>,
) {
    use autumn_harvest::schema::harvest_events::dsl;

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let event_data = serde_json::json!({
        "type": event_type,
        "data": { "activity_id": activity_id.to_string() }
    });
    let row = NewHarvestEvent {
        workflow_exec_id: exec_id,
        event_id,
        event_type,
        event_data,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_events::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert event");
    diesel::update(
        dsl::harvest_events
            .filter(dsl::workflow_exec_id.eq(exec_id))
            .filter(dsl::event_id.eq(event_id)),
    )
    .set(dsl::timestamp.eq(timestamp))
    .execute(&mut conn)
    .await
    .expect("force event timestamp");
}

#[tokio::test]
async fn empty_fleet_returns_complete_zero_groups() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();
    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (now - Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "complete");
    assert_eq!(body["groups"].as_array().unwrap().len(), 0);
    assert!(body["unavailable_shards"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn missing_from_is_bad_request() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let (status, _) = get_json(&app, "/admin/usage?to=2026-01-01T00:00:00Z").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn window_wider_than_ceiling_is_bad_request_naming_ceiling() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let (status, body) = get_json(
        &app,
        "/admin/usage?from=2026-01-01T00:00:00Z&to=2026-06-01T00:00:00Z",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let msg = body.get("detail").and_then(Value::as_str).unwrap_or("");
    assert!(msg.contains("90"), "error should name the ceiling: {msg}");
}

/// Insert a `WorkflowCompleted`/`WorkflowFailed`/`WorkflowCancelled`/
/// `WorkflowExecutionTimedOut` event on `exec_id` at an explicit
/// `timestamp`, mirroring the adjacently-tagged `WorkflowEvent` JSON shape.
/// The usage query only inspects `event_type`, so a minimal empty `data`
/// object is sufficient.
async fn seed_terminal_event(
    url: &str,
    exec_id: Uuid,
    event_id: i32,
    event_type: &str,
    timestamp: DateTime<Utc>,
) {
    use autumn_harvest::schema::harvest_events::dsl;

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let event_data = serde_json::json!({
        "type": event_type,
        "data": {}
    });
    let row = NewHarvestEvent {
        workflow_exec_id: exec_id,
        event_id,
        event_type,
        event_data,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_events::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert event");
    diesel::update(
        dsl::harvest_events
            .filter(dsl::workflow_exec_id.eq(exec_id))
            .filter(dsl::event_id.eq(event_id)),
    )
    .set(dsl::timestamp.eq(timestamp))
    .execute(&mut conn)
    .await
    .expect("force event timestamp");
}

#[tokio::test]
async fn default_group_by_is_workflow_name_and_counts_starts() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();

    seed_execution(&url, 0, "onboarding", "RUNNING", now, None, None).await;
    seed_execution(&url, 0, "onboarding", "RUNNING", now, None, None).await;
    seed_execution(&url, 0, "billing", "RUNNING", now, None, None).await;

    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (now - Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let groups = body["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 2);
    let onboarding = groups.iter().find(|g| g["group"] == "onboarding").unwrap();
    assert_eq!(onboarding["workflow_starts"], 2);
}

#[tokio::test]
async fn terminal_outcomes_counted_by_durable_terminal_events() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();
    let started = now - Duration::hours(2);

    let completed_id =
        seed_execution(&url, 0, "onboarding", "COMPLETED", started, Some(now), None).await;
    seed_terminal_event(&url, completed_id, 1, "WorkflowCompleted", now).await;

    let failed_id = seed_execution(&url, 0, "onboarding", "FAILED", started, Some(now), None).await;
    seed_terminal_event(&url, failed_id, 1, "WorkflowFailed", now).await;

    let cancelled_id =
        seed_execution(&url, 0, "onboarding", "CANCELLED", started, Some(now), None).await;
    seed_terminal_event(&url, cancelled_id, 1, "WorkflowCancelled", now).await;

    let timed_out_id =
        seed_execution(&url, 0, "onboarding", "TIMED_OUT", started, Some(now), None).await;
    seed_terminal_event(&url, timed_out_id, 1, "WorkflowExecutionTimedOut", now).await;

    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (now - Duration::minutes(30)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::minutes(30)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let groups = body["groups"].as_array().unwrap();
    let onboarding = groups.iter().find(|g| g["group"] == "onboarding").unwrap();
    assert_eq!(onboarding["completed"], 1);
    assert_eq!(onboarding["failed"], 1);
    assert_eq!(onboarding["cancelled"], 1);
    assert_eq!(onboarding["timed_out"], 1);
    // These executions started outside the query window, so they must not
    // be double-counted as workflow_starts.
    assert_eq!(onboarding["workflow_starts"], 0);
}

#[tokio::test]
async fn redriven_failure_still_counted_in_the_window_it_occurred() {
    // Locks in the PR #895 review fix: usage-report terminal outcomes are
    // immutable historical facts derived from durable events, not mutable
    // row-state snapshots. A DLQ redrive (reactivate_failed_execution)
    // clears a FAILED row's state/completed_at but never rewrites or
    // removes the original WorkflowFailed event, so a report re-run for
    // the window the failure actually occurred in must still show it.
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();
    let started = now - Duration::hours(2);

    let exec_id = seed_execution(&url, 0, "onboarding", "FAILED", started, Some(now), None).await;
    seed_terminal_event(&url, exec_id, 1, "WorkflowFailed", now).await;

    // Simulate a redrive: the row flips back to RUNNING and completed_at is
    // cleared, mirroring exactly what reactivate_failed_execution does to
    // the row, without touching the already-recorded WorkflowFailed event.
    {
        use autumn_harvest::schema::harvest_workflow_executions::dsl;
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("connect");
        diesel::update(dsl::harvest_workflow_executions.filter(dsl::id.eq(exec_id)))
            .set((
                dsl::state.eq("RUNNING"),
                dsl::completed_at.eq(None::<DateTime<Utc>>),
            ))
            .execute(&mut conn)
            .await
            .expect("simulate redrive");
    }

    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (now - Duration::minutes(30)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::minutes(30)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let groups = body["groups"].as_array().unwrap();
    let onboarding = groups.iter().find(|g| g["group"] == "onboarding").unwrap();
    assert_eq!(
        onboarding["failed"], 1,
        "the historical WorkflowFailed event must still be counted after redrive clears the row"
    );
}

#[tokio::test]
async fn terminated_execution_reusing_workflow_cancelled_event_is_not_counted_as_cancelled() {
    // terminate_workflow_execution (issue #504) reuses the same
    // WorkflowCancelled event type as a genuine cancel (no new event
    // variant) but seals the row to TERMINATED, not CANCELLED. The
    // `cancelled` counter must not conflate the two.
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();
    let started = now - Duration::hours(2);

    let exec_id = seed_execution(
        &url,
        0,
        "onboarding",
        "TERMINATED",
        started,
        Some(now),
        None,
    )
    .await;
    seed_terminal_event(&url, exec_id, 1, "WorkflowCancelled", now).await;

    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (now - Duration::minutes(30)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::minutes(30)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let groups = body["groups"].as_array().unwrap();
    if let Some(onboarding) = groups.iter().find(|g| g["group"] == "onboarding") {
        assert_eq!(onboarding["cancelled"], 0);
    }
}

#[tokio::test]
async fn reset_terminated_cancelled_execution_still_counted_as_cancelled() {
    // PR #895 review (chatgpt-codex-connector): a DAG retry/reset with
    // allow_terminal_source can seal a CANCELLED source execution to
    // TERMINATED (reset.rs's sealable_states). Because that reset also
    // appends WorkflowResetTerminated on the source execution -- unlike a
    // genuine terminate, which never does -- the cancelled counter must
    // keep counting the original WorkflowCancelled event instead of losing
    // it once the row moves off CANCELLED.
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();
    let started = now - Duration::hours(2);

    let exec_id = seed_execution(
        &url,
        0,
        "onboarding",
        "TERMINATED",
        started,
        Some(now),
        None,
    )
    .await;
    seed_terminal_event(&url, exec_id, 1, "WorkflowCancelled", now).await;
    seed_terminal_event(
        &url,
        exec_id,
        2,
        "WorkflowResetTerminated",
        now + Duration::minutes(5),
    )
    .await;

    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (now - Duration::minutes(30)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::minutes(30)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let groups = body["groups"].as_array().unwrap();
    let onboarding = groups.iter().find(|g| g["group"] == "onboarding").unwrap();
    assert_eq!(
        onboarding["cancelled"], 1,
        "the historical cancellation must survive the later reset seal"
    );
}

#[tokio::test]
async fn external_activity_timeout_without_a_start_is_not_counted_as_failed() {
    // PR #895 review (chatgpt-codex-connector): enforce_external_task_timeouts
    // appends ActivityTimedOut for external activities that never emit
    // ActivityStarted (they're dispatched via ActivityAwaitingExternal, never
    // claimed by a worker). activity_executions_failed must require a
    // matching start, same as activity_compute_seconds already does.
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();
    let started = now - Duration::hours(1);

    let exec_id = seed_execution(&url, 0, "onboarding", "RUNNING", started, None, None).await;
    let activity_id = Uuid::new_v4();
    seed_activity_event(
        &url,
        exec_id,
        1,
        "ActivityTimedOut",
        activity_id,
        started + Duration::minutes(10),
    )
    .await;

    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (started - Duration::minutes(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::minutes(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let groups = body["groups"].as_array().unwrap();
    let onboarding = groups.iter().find(|g| g["group"] == "onboarding").unwrap();
    assert_eq!(
        onboarding["activity_executions_failed"], 0,
        "an ActivityTimedOut with no matching ActivityStarted must not be counted as a failure"
    );
    assert_eq!(onboarding["activity_compute_seconds"], 0.0);
}

#[tokio::test]
async fn search_attr_group_by_buckets_missing_key_as_unattributed() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();

    seed_execution(
        &url,
        0,
        "onboarding",
        "RUNNING",
        now,
        None,
        Some(serde_json::json!({"tenant_id": "acme"})),
    )
    .await;
    seed_execution(
        &url,
        0,
        "onboarding",
        "RUNNING",
        now,
        None,
        Some(serde_json::json!({"tenant_id": "acme"})),
    )
    .await;
    seed_execution(&url, 0, "onboarding", "RUNNING", now, None, None).await;

    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}&group_by=search_attr:tenant_id",
            (now - Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let groups = body["groups"].as_array().unwrap();
    let acme = groups.iter().find(|g| g["group"] == "acme").unwrap();
    assert_eq!(acme["workflow_starts"], 2);
    let unattributed = groups
        .iter()
        .find(|g| g["group"] == "(unattributed)")
        .unwrap();
    assert_eq!(unattributed["workflow_starts"], 1);
}

#[tokio::test]
async fn retried_activity_counts_two_executions_one_failure_final_attempt_compute() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();
    let started = now - Duration::hours(1);

    let exec_id = seed_execution(&url, 0, "onboarding", "RUNNING", started, None, None).await;
    let activity_id = Uuid::new_v4();

    // Attempt 1: starts, then fails without a terminal event (mid-retry —
    // the engine does not append ActivityFailed for a non-final attempt).
    let attempt1_start = started + Duration::minutes(1);
    seed_activity_event(
        &url,
        exec_id,
        1,
        "ActivityStarted",
        activity_id,
        attempt1_start,
    )
    .await;

    // Attempt 2 (final, retries exhausted): starts, then a terminal
    // ActivityFailed 10 seconds later.
    let attempt2_start = started + Duration::minutes(5);
    let attempt2_terminal = attempt2_start + Duration::seconds(10);
    seed_activity_event(
        &url,
        exec_id,
        2,
        "ActivityStarted",
        activity_id,
        attempt2_start,
    )
    .await;
    seed_activity_event(
        &url,
        exec_id,
        3,
        "ActivityFailed",
        activity_id,
        attempt2_terminal,
    )
    .await;

    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (started - Duration::minutes(5)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let groups = body["groups"].as_array().unwrap();
    let onboarding = groups.iter().find(|g| g["group"] == "onboarding").unwrap();
    assert_eq!(
        onboarding["activity_executions"], 2,
        "two ActivityStarted events (one per attempt)"
    );
    assert_eq!(
        onboarding["activity_executions_failed"], 1,
        "only the terminal ActivityFailed counts"
    );
    let compute_seconds = onboarding["activity_compute_seconds"].as_f64().unwrap();
    assert!(
        (compute_seconds - 10.0).abs() < 0.01,
        "compute-seconds should reflect only the final attempt's 10s span, got {compute_seconds}"
    );
}

#[tokio::test]
async fn completed_activity_contributes_compute_seconds_but_not_failure_count() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();
    let started = now - Duration::hours(1);

    let exec_id = seed_execution(&url, 0, "onboarding", "RUNNING", started, None, None).await;
    let activity_id = Uuid::new_v4();
    let start_ts = started + Duration::minutes(1);
    let terminal_ts = start_ts + Duration::seconds(5);
    seed_activity_event(&url, exec_id, 1, "ActivityStarted", activity_id, start_ts).await;
    seed_activity_event(
        &url,
        exec_id,
        2,
        "ActivityCompleted",
        activity_id,
        terminal_ts,
    )
    .await;

    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (started - Duration::minutes(5)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let groups = body["groups"].as_array().unwrap();
    let onboarding = groups.iter().find(|g| g["group"] == "onboarding").unwrap();
    assert_eq!(onboarding["activity_executions"], 1);
    assert_eq!(onboarding["activity_executions_failed"], 0);
    let compute_seconds = onboarding["activity_compute_seconds"].as_f64().unwrap();
    assert!((compute_seconds - 5.0).abs() < 0.01);
}

#[tokio::test]
async fn merges_across_shards() {
    let ((url0, url1), _c) = setup_two_shards().await;
    let app = build_app(two_shard_storage(&url0, &url1));
    let now = Utc::now();

    seed_execution(&url0, 0, "onboarding", "RUNNING", now, None, None).await;
    seed_execution(&url1, 1, "onboarding", "RUNNING", now, None, None).await;

    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (now - Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "complete");
    let groups = body["groups"].as_array().unwrap();
    let onboarding = groups.iter().find(|g| g["group"] == "onboarding").unwrap();
    assert_eq!(
        onboarding["workflow_starts"], 2,
        "summed across both shards"
    );
}

#[tokio::test]
async fn one_shard_down_is_partial_not_500() {
    let ((url0, url1), _c) = setup_two_shards().await;
    let now = Utc::now();
    seed_execution(&url0, 0, "onboarding", "RUNNING", now, None, None).await;

    // Point shard 1 at a database that does not exist so its pool fails.
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url0));
    pools.insert(
        ShardId::new(1),
        build_pool(&url1.replace("harvest_shard_", "missing_db_")),
    );
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let app_partial = build_app(storage);

    let (status, body) = get_json(
        &app_partial,
        &format!(
            "/admin/usage?from={}&to={}",
            (now - Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "a down shard must not 500");
    assert_eq!(body["status"], "partial");
    let groups = body["groups"].as_array().unwrap();
    let onboarding = groups.iter().find(|g| g["group"] == "onboarding").unwrap();
    assert_eq!(
        onboarding["workflow_starts"], 1,
        "healthy shard still flows through"
    );
    let unavailable = body["unavailable_shards"].as_array().unwrap();
    assert_eq!(unavailable.len(), 1);
    assert_eq!(unavailable[0]["shard_id"], 1);
    assert!(!unavailable[0]["reason"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn search_attr_value_literally_unattributed_merges_with_missing_key() {
    // Locks in the documented limitation (issue #596 F1): an execution whose
    // search_attrs value for the requested key literally equals the
    // "(unattributed)" sentinel is indistinguishable from an execution that
    // lacks the key entirely — both merge into the same group. This is an
    // accepted, documented limitation, not a defect (see the module doc in
    // autumn-harvest/src/usage.rs).
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let now = Utc::now();

    seed_execution(&url, 0, "onboarding", "RUNNING", now, None, None).await;
    seed_execution(
        &url,
        0,
        "onboarding",
        "RUNNING",
        now,
        None,
        Some(serde_json::json!({"tenant_id": "(unattributed)"})),
    )
    .await;

    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}&group_by=search_attr:tenant_id",
            (now - Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let groups = body["groups"].as_array().unwrap();
    assert_eq!(
        groups.len(),
        1,
        "the missing-key execution and the literal-'(unattributed)'-value execution merge into one group"
    );
    let unattributed = groups
        .iter()
        .find(|g| g["group"] == "(unattributed)")
        .unwrap();
    assert_eq!(unattributed["workflow_starts"], 2);
}

#[tokio::test]
async fn group_count_over_cap_is_413_naming_the_cap() {
    let (url, _c) = setup_single_shard().await;
    let now = Utc::now();

    for i in 0..3 {
        seed_execution(&url, 0, &format!("wf_{i}"), "RUNNING", now, None, None).await;
    }

    let app = build_app_with_max_groups(HarvestDbPool::from(build_pool(&url)), 2);
    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (now - Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let detail = body.get("detail").and_then(Value::as_str).unwrap_or("");
    assert!(detail.contains('2'), "error should name the cap: {detail}");
    assert!(
        detail.contains('3'),
        "error should name the group count: {detail}"
    );
}

#[tokio::test]
async fn group_count_far_over_cap_is_still_413_via_bounded_shard_query() {
    // PR #895 review (chatgpt-codex-connector): the 413 cap must bound the
    // shard query itself (a LIMIT), not just the merged response -- a
    // single shard with far more distinct groups than the cap must not
    // materialize them all before the check runs. Seeds well more than
    // cap+1 groups on one shard and confirms the report still fails loudly
    // rather than silently succeeding or hanging.
    let (url, _c) = setup_single_shard().await;
    let now = Utc::now();

    for i in 0..10 {
        seed_execution(&url, 0, &format!("wf_{i}"), "RUNNING", now, None, None).await;
    }

    let app = build_app_with_max_groups(HarvestDbPool::from(build_pool(&url)), 2);
    let (status, body) = get_json(
        &app,
        &format!(
            "/admin/usage?from={}&to={}",
            (now - Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            (now + Duration::hours(1)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let detail = body.get("detail").and_then(Value::as_str).unwrap_or("");
    assert!(detail.contains('2'), "error should name the cap: {detail}");
}
