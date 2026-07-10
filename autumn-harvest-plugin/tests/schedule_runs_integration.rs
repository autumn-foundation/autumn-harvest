//! Integration tests for `GET /admin/schedules/{id}/runs` (issue #534).
//!
//! Seeds workflow executions attributed to a schedule with the three dispatch
//! origins (`scheduled`/`backfill`/`manual_trigger`) and asserts the endpoint
//! lists them newest-first, filters by state, paginates by keyset cursor, merges
//! across shards (flagging an unavailable shard as `partial`, never 500), and
//! reports a cadence summary that counts only `scheduled`-origin runs — the
//! "zero conflation" guarantee from the issue's success metric.

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
    // issue #534: origin column + per-schedule run-history index.
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

/// Seed one schedule-attributed run on `url`/`shard` and return its exec id.
#[allow(clippy::too_many_arguments)]
async fn seed_run(
    url: &str,
    shard: i32,
    schedule_id: Uuid,
    origin: Option<&str>,
    scheduled_for: Option<DateTime<Utc>>,
    state: &str,
    started_at: DateTime<Utc>,
) -> Uuid {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(shard));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let wf_id = format!("nightly-{}", Uuid::new_v4().simple());
    let row = NewWorkflowExecution {
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: "nightly_etl",
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
        schedule_id: Some(schedule_id),
        scheduled_for,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin,
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

/// Insert a minimal `harvest_schedules` row so the runs endpoint can resolve the
/// schedule (existence gate + `next_run_at` echo, issue #762). All other columns
/// rely on their migration-level DB defaults.
async fn seed_schedule(url: &str, schedule_id: Uuid, next_run_at: Option<DateTime<Utc>>) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    diesel::sql_query(
        "INSERT INTO harvest_schedules (id, workflow_name, next_run_at) VALUES ($1, $2, $3)",
    )
    .bind::<diesel::sql_types::Uuid, _>(schedule_id)
    .bind::<diesel::sql_types::Text, _>("nightly_etl")
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(next_run_at)
    .execute(&mut conn)
    .await
    .expect("insert schedule");
}

#[tokio::test]
async fn unknown_schedule_id_returns_404() {
    // issue #762: a real-but-unknown schedule id is a 404, never a silent empty list.
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let sid = Uuid::new_v4();
    let (status, _) = get_json(&app, &format!("/admin/schedules/{sid}/runs")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn next_run_at_is_echoed() {
    // issue #762: the response echoes the schedule's next expected fire.
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let sid = Uuid::new_v4();
    let next = Utc::now() + Duration::hours(3);
    seed_schedule(&url, sid, Some(next)).await;
    let (status, body) = get_json(&app, &format!("/admin/schedules/{sid}/runs")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["next_run_at"].is_string(),
        "next_run_at echoed, got {:?}",
        body["next_run_at"]
    );
    // No runs seeded, but the schedule exists → OK with an empty list (not 404).
    assert_eq!(body["runs"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn outcome_and_error_are_surfaced_per_run() {
    // issue #762: each run carries a collapsed `outcome`; `error` is the first line,
    // only for terminally-failed runs.
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let sid = Uuid::new_v4();
    seed_schedule(&url, sid, None).await;
    let base = Utc::now() - Duration::hours(2);

    let ok = seed_run(
        &url,
        0,
        sid,
        Some("scheduled"),
        Some(base),
        "COMPLETED",
        base,
    )
    .await;
    let failed = seed_run(
        &url,
        0,
        sid,
        Some("scheduled"),
        Some(base + Duration::hours(1)),
        "FAILED",
        base + Duration::hours(1),
    )
    .await;
    // Give the failed run a multi-line error to prove first-line truncation.
    {
        use autumn_harvest::schema::harvest_workflow_executions::dsl;
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
            .await
            .expect("connect");
        diesel::update(dsl::harvest_workflow_executions.filter(dsl::id.eq(failed)))
            .set(dsl::error.eq("card declined: insufficient funds\nstack frame 2"))
            .execute(&mut conn)
            .await
            .expect("set error");
    }

    let (status, body) = get_json(&app, &format!("/admin/schedules/{sid}/runs")).await;
    assert_eq!(status, StatusCode::OK);
    let runs = body["runs"].as_array().unwrap();

    let find = |id: Uuid| {
        runs.iter()
            .find(|r| r["execution_id"] == id.to_string())
            .unwrap()
    };
    let ok_run = find(ok);
    assert_eq!(ok_run["outcome"], "completed");
    assert!(ok_run["error"].is_null(), "completed run has no error");

    let failed_run = find(failed);
    assert_eq!(failed_run["outcome"], "failed");
    assert_eq!(
        failed_run["error"], "card declined: insufficient funds",
        "first line only"
    );
}

#[tokio::test]
async fn runs_ordered_by_slot_across_page_boundary() {
    // issue #762: newest-slot-first ordering (not started_at) survives keyset paging.
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let sid = Uuid::new_v4();
    seed_schedule(&url, sid, None).await;
    let base = Utc::now() - Duration::hours(12);

    // Slots strictly decrease; start times deliberately shuffled so slot != start.
    let starts = [4, 1, 7, 2, 9];
    let mut expected_by_slot: Vec<(i64, Uuid)> = Vec::new();
    for (i, s) in starts.iter().enumerate() {
        let i = i64::try_from(i).unwrap();
        let slot = base + Duration::hours(10 - i);
        let id = seed_run(
            &url,
            0,
            sid,
            Some("scheduled"),
            Some(slot),
            "COMPLETED",
            base + Duration::hours(*s),
        )
        .await;
        expected_by_slot.push((10 - i, id));
    }
    // expected_by_slot already in slot-DESC order (10,9,8,7,6).

    // Page through with limit=2 and stitch the pages together.
    let mut collected: Vec<String> = Vec::new();
    let mut uri = format!("/admin/schedules/{sid}/runs?limit=2");
    loop {
        let (status, body) = get_json(&app, &uri).await;
        assert_eq!(status, StatusCode::OK);
        for r in body["runs"].as_array().unwrap() {
            collected.push(r["execution_id"].as_str().unwrap().to_string());
        }
        match body["next_cursor"].as_str() {
            Some(cur) => uri = format!("/admin/schedules/{sid}/runs?limit=2&cursor={cur}"),
            None => break,
        }
    }

    let expected: Vec<String> = expected_by_slot
        .iter()
        .map(|(_, id)| id.to_string())
        .collect();
    assert_eq!(
        collected, expected,
        "slot-DESC order preserved across pages"
    );
}

#[tokio::test]
async fn malformed_id_is_bad_request_not_500() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let (status, _) = get_json(&app, "/admin/schedules/not-a-uuid/runs").await;
    assert!(
        status == StatusCode::BAD_REQUEST || status == StatusCode::NOT_FOUND,
        "got {status}"
    );
}

#[tokio::test]
async fn invalid_state_filter_is_bad_request() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let sid = Uuid::new_v4();
    let (status, _) = get_json(&app, &format!("/admin/schedules/{sid}/runs?state=BOGUS")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// The success-metric test: same workflow type started via schedule, backfill,
/// and manual trigger. Each must appear under the correct `origin`, and only the
/// schedule-origin runs may count toward the cadence summary (zero conflation).
#[tokio::test]
async fn origins_are_separated_and_summary_is_cadence_only() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let sid = Uuid::new_v4();
    seed_schedule(&url, sid, None).await;
    let base = Utc::now() - Duration::hours(4);

    // Scheduled cadence: 2 succeeded, 1 failed.
    seed_run(
        &url,
        0,
        sid,
        Some("scheduled"),
        Some(base),
        "COMPLETED",
        base,
    )
    .await;
    seed_run(
        &url,
        0,
        sid,
        Some("scheduled"),
        Some(base + Duration::hours(1)),
        "COMPLETED",
        base + Duration::hours(1),
    )
    .await;
    seed_run(
        &url,
        0,
        sid,
        Some("scheduled"),
        Some(base + Duration::hours(2)),
        "FAILED",
        base + Duration::hours(2),
    )
    .await;
    // A backfill FAILED and a manual FAILED — attributed, but NOT cadence.
    seed_run(
        &url,
        0,
        sid,
        Some("backfill"),
        Some(base - Duration::hours(1)),
        "FAILED",
        base + Duration::hours(3),
    )
    .await;
    seed_run(
        &url,
        0,
        sid,
        Some("manual_trigger"),
        None,
        "FAILED",
        base + Duration::hours(3) + Duration::minutes(30),
    )
    .await;

    let (status, body) = get_json(&app, &format!("/admin/schedules/{sid}/runs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "complete");

    // All five attributed runs appear in the list.
    let runs = body["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 5);
    let origins: Vec<&str> = runs.iter().map(|r| r["origin"].as_str().unwrap()).collect();
    assert!(origins.contains(&"scheduled"));
    assert!(origins.contains(&"backfill"));
    assert!(origins.contains(&"manual_trigger"));

    // Cadence summary counts scheduled-origin only: 2 succeeded, 1 failed, total 3.
    assert_eq!(body["summary"]["succeeded"], 2);
    assert_eq!(
        body["summary"]["failed"], 1,
        "backfill and manual failures excluded from cadence"
    );
    assert_eq!(body["summary"]["total"], 3);
}

#[tokio::test]
async fn state_filter_and_newest_first_ordering() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let sid = Uuid::new_v4();
    seed_schedule(&url, sid, None).await;
    let base = Utc::now() - Duration::hours(6);

    seed_run(
        &url,
        0,
        sid,
        Some("scheduled"),
        Some(base),
        "COMPLETED",
        base,
    )
    .await;
    seed_run(
        &url,
        0,
        sid,
        Some("scheduled"),
        Some(base + Duration::hours(1)),
        "FAILED",
        base + Duration::hours(1),
    )
    .await;
    seed_run(
        &url,
        0,
        sid,
        Some("scheduled"),
        Some(base + Duration::hours(2)),
        "TIMED_OUT",
        base + Duration::hours(2),
    )
    .await;

    // Newest-first overall.
    let (_, all) = get_json(&app, &format!("/admin/schedules/{sid}/runs")).await;
    let times: Vec<&str> = all["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["started_at"].as_str().unwrap())
        .collect();
    let mut sorted = times.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(times, sorted, "runs are newest-first");

    // Filter to FAILED + TIMED_OUT.
    let (_, filtered) = get_json(
        &app,
        &format!("/admin/schedules/{sid}/runs?state=FAILED&state=TIMED_OUT"),
    )
    .await;
    let states: Vec<&str> = filtered["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["state"].as_str().unwrap())
        .collect();
    assert_eq!(states.len(), 2);
    assert!(states.iter().all(|s| *s == "FAILED" || *s == "TIMED_OUT"));
}

#[tokio::test]
async fn limit_reported_and_cursor_paginates() {
    let (url, _c) = setup_single_shard().await;
    let app = single_app(&url);
    let sid = Uuid::new_v4();
    seed_schedule(&url, sid, None).await;
    let base = Utc::now() - Duration::hours(8);

    for i in 0..5 {
        seed_run(
            &url,
            0,
            sid,
            Some("scheduled"),
            Some(base + Duration::minutes(i)),
            "COMPLETED",
            base + Duration::minutes(i),
        )
        .await;
    }

    let (_, page1) = get_json(&app, &format!("/admin/schedules/{sid}/runs?limit=2")).await;
    assert_eq!(page1["limit"], 2);
    assert_eq!(page1["runs"].as_array().unwrap().len(), 2);
    let cursor = page1["next_cursor"].as_str().expect("more rows -> cursor");

    let (_, page2) = get_json(
        &app,
        &format!("/admin/schedules/{sid}/runs?limit=2&cursor={cursor}"),
    )
    .await;
    let p1_ids: Vec<&str> = page1["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["execution_id"].as_str().unwrap())
        .collect();
    let p2_ids: Vec<&str> = page2["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["execution_id"].as_str().unwrap())
        .collect();
    assert!(
        p1_ids.iter().all(|id| !p2_ids.contains(id)),
        "no overlap across pages"
    );
}

#[tokio::test]
async fn merges_across_shards() {
    let ((url0, url1), _c) = setup_two_shards().await;
    let app = build_app(two_shard_storage(&url0, &url1));
    let sid = Uuid::new_v4();
    // The schedule row lives on one shard (shard 0); runs may spread across both.
    seed_schedule(&url0, sid, None).await;
    let base = Utc::now() - Duration::hours(2);

    seed_run(
        &url0,
        0,
        sid,
        Some("scheduled"),
        Some(base),
        "COMPLETED",
        base,
    )
    .await;
    seed_run(
        &url1,
        1,
        sid,
        Some("scheduled"),
        Some(base + Duration::minutes(30)),
        "FAILED",
        base + Duration::minutes(30),
    )
    .await;

    let (status, body) = get_json(&app, &format!("/admin/schedules/{sid}/runs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "complete");
    assert_eq!(
        body["runs"].as_array().unwrap().len(),
        2,
        "merged both shards"
    );
    assert_eq!(body["summary"]["succeeded"], 1);
    assert_eq!(body["summary"]["failed"], 1);
}

#[tokio::test]
async fn one_shard_down_is_partial_not_500() {
    let ((url0, url1), _c) = setup_two_shards().await;
    let app = build_app(two_shard_storage(&url0, &url1));
    let sid = Uuid::new_v4();
    // Schedule + healthy run on shard 0, which is queried before the down shard 1.
    seed_schedule(&url0, sid, None).await;
    let base = Utc::now() - Duration::hours(1);
    seed_run(
        &url0,
        0,
        sid,
        Some("scheduled"),
        Some(base),
        "COMPLETED",
        base,
    )
    .await;

    // Point shard 1 at a database that does not exist so its pool fails.
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url0));
    pools.insert(
        ShardId::new(1),
        build_pool(&url1.replace("harvest_shard_", "missing_db_")),
    );
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let app_partial = build_app(storage);
    let _ = app; // the healthy app is exercised by other tests

    let (status, body) = get_json(&app_partial, &format!("/admin/schedules/{sid}/runs")).await;
    assert_eq!(status, StatusCode::OK, "a down shard must not 500");
    assert_eq!(body["status"], "partial");
    // The healthy shard's data still flows through.
    assert_eq!(body["runs"].as_array().unwrap().len(), 1);
    let unavailable = body["shards"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["status"] == "unavailable");
    assert!(unavailable, "the down shard is named in the report");
}

#[tokio::test]
async fn schedule_on_down_shard_is_indeterminate_not_404() {
    // issue #762 review: a schedule that genuinely EXISTS but whose owning shard is
    // unreachable must be reported as INDETERMINATE (503), never as a definitive 404
    // (which would lie about existence). Mirrors `one_shard_down_is_partial_not_500`
    // but seeds the schedule on the DOWN shard so the existence lookup can't resolve
    // it while a shard is unreachable.
    let ((url0, url1), _c) = setup_two_shards().await;
    let sid = Uuid::new_v4();
    // Schedule lives on shard 1, which we will make unreachable.
    seed_schedule(&url1, sid, None).await;

    // Point shard 1 at a database that does not exist so its pool fails; shard 0 is
    // healthy but does not own the schedule.
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url0));
    pools.insert(
        ShardId::new(1),
        build_pool(&url1.replace("harvest_shard_", "missing_db_")),
    );
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let app = build_app(storage);

    let (status, body) = get_json(&app, &format!("/admin/schedules/{sid}/runs")).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "existence is indeterminate while a shard is down; must be 503, not 404, got body {body:?}"
    );
}

#[tokio::test]
async fn lookup_error_on_earlier_shard_still_resolves_on_later_shard() {
    // issue #762 review (finding 2): a per-shard schedule-lookup ERROR (as opposed
    // to an unreachable connection) on an earlier-queried shard must NOT
    // short-circuit the request with a blanket 503. The existence scan continues,
    // so a schedule living on a *later* healthy shard is still resolved (200).
    //
    // Shard 0's database is created but never migrated, so its `harvest_schedules`
    // SELECT errors ("relation does not exist") — the connection acquires fine, it
    // is the query that fails, exercising the new `Err(_) => continue` arm rather
    // than the acquire-failure arm. Shard 1 is healthy and owns the schedule.
    //
    // NB: the router-known-but-poolless shard case (finding 1) funnels into the
    // same `any_shard_unreachable` flag + `resolve_not_found_outcome` decision
    // (existence gate) AND, since the fan-out now iterates the same
    // `expected_shards` set, into an unavailable run observation (fan-out) — but
    // neither can be modelled here: `build_app` installs no runtime, so
    // `api_state` has no router and `expected_shards` reduces to the live pool
    // keys. Those paths are covered by the pure unit tests
    // `resolve_not_found_outcome_*` and
    // `observe_schedule_runs_shard_poolless_shard_is_unavailable_not_complete`
    // in `api.rs`. The fan-out's query-error → `partial` path is covered
    // end-to-end by `one_shard_down_is_partial_not_500`.
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let empty = format!("harvest_shard_{}", Uuid::new_v4().simple());
    let healthy = format!("harvest_shard_{}", Uuid::new_v4().simple());

    let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("admin connect");
    for db in [&empty, &healthy] {
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create db");
    }
    let url0 = format!("postgres://postgres:postgres@{host}:{port}/{empty}");
    let url1 = format!("postgres://postgres:postgres@{host}:{port}/{healthy}");
    // Migrate only shard 1; shard 0 stays schema-less so its lookup errors.
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url1)
            .await
            .expect("shard connect");
        diesel_async::SimpleAsyncConnection::batch_execute(&mut conn, INIT_SQL)
            .await
            .expect("migrate shard 1");
    }
    let sid = Uuid::new_v4();
    // Schedule lives on the healthy later shard (queried after the erroring one).
    seed_schedule(&url1, sid, None).await;

    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url0));
    pools.insert(ShardId::new(1), build_pool(&url1));
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let app = build_app(storage);

    let (status, body) = get_json(&app, &format!("/admin/schedules/{sid}/runs")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a lookup error on shard 0 must not blanket-503 when shard 1 owns the \
         schedule; got body {body:?}"
    );
    // The schedule resolved, so next_run_at is present (null here — none seeded)
    // and the row was found rather than 404/503.
    assert!(
        body.get("runs").is_some(),
        "resolved schedule yields a runs response, got {body:?}"
    );
    let _container = container;
}
