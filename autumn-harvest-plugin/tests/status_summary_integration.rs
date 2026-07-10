//! Integration tests for `GET /admin/status` (issue #679).
//!
//! Exercises the rolled-up health summary end-to-end: a healthy fleet reports
//! `healthy` with five subsystems; seeded dead-letters drive the `dead_letters`
//! subsystem (and the overall verdict) non-healthy with a drill-down link; the
//! route is admin-gated; and a down shard degrades (never 500s) with the shard
//! named in `unavailable_shards`.
//!
//! These tests require Docker/testcontainers and are compile-checked in
//! sandboxes without a Docker daemon (matching the #543/#544/#601 precedent).

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

/// Admin-gated router with the outer auth boundary declared, so `/admin/status`
/// reaches the handler.
fn build_app(storage: HarvestDbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(storage);
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Router with NO auth boundary — used to prove the admin guard rejects.
fn build_unauthenticated_app(storage: HarvestDbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(storage);
    harvest_api_router(api_state).with_state(AppState::for_test())
}

fn single_app(url: &str) -> HarvestApiApp {
    build_app(HarvestDbPool::from(build_pool(url)))
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

async fn seed_execution(url: &str, shard: i32, workflow_name: &str, state: &str) -> Uuid {
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
        continued_from_exec_id: None,
        first_exec_id: None,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert");
    diesel::update(dsl::harvest_workflow_executions.filter(dsl::id.eq(exec_id.as_uuid())))
        .set(dsl::state.eq(state))
        .execute(&mut conn)
        .await
        .expect("force state");
    exec_id.as_uuid()
}

/// Insert a fresh (`NOW()`) `harvest_events` row for `exec_id` so the execution
/// is not counted as stalled — the stalled predicate is: active state with no
/// `harvest_events` row newer than the no-progress window. Without this, a
/// seeded RUNNING execution with an empty history reads as stalled, so the
/// healthy path would never actually be healthy against a real DB.
async fn seed_recent_event(url: &str, exec_id: Uuid) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    diesel::sql_query(
        "INSERT INTO harvest_events \
             (workflow_exec_id, event_id, event_type, event_data, timestamp) \
         VALUES ($1, 0, 'WorkflowStarted', '{}'::jsonb, NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(&mut conn)
    .await
    .expect("seed recent event");
}

/// Record every migration's version in `__diesel_schema_migrations`, mirroring
/// what `diesel migration run` does. The raw-`up.sql` `with_init_sql` path never
/// populates this table, so the shard-health readiness probe (which reads it,
/// `shard_health.rs`) would otherwise report `schema_unreadable` and degrade the
/// shards subsystem — making the healthy path impossible.
///
/// Versions are the numeric prefix of each directory under
/// `autumn-harvest/migrations`, resolved at compile time via `CARGO_MANIFEST_DIR`
/// so it is independent of the test's runtime CWD (matches the local-PG helper).
async fn populate_migration_ledger(url: &str) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let migrations_dir =
        concat!(env!("CARGO_MANIFEST_DIR"), "/../autumn-harvest/migrations").to_string();
    let mut versions: Vec<String> = std::fs::read_dir(&migrations_dir)
        .expect("read migrations dir")
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().into_string().ok()?;
            let prefix = name.split('_').next()?;
            (prefix.len() >= 8 && prefix.chars().all(|c| c.is_ascii_digit()))
                .then(|| prefix.to_string())
        })
        .collect();
    versions.sort();
    versions.dedup();

    diesel_async::SimpleAsyncConnection::batch_execute(
        &mut conn,
        "CREATE TABLE IF NOT EXISTS __diesel_schema_migrations ( \
             version VARCHAR(50) PRIMARY KEY NOT NULL, \
             run_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    )
    .await
    .expect("create migration ledger");
    for version in &versions {
        diesel::sql_query(
            "INSERT INTO __diesel_schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING",
        )
        .bind::<diesel::sql_types::Text, _>(version)
        .execute(&mut conn)
        .await
        .expect("insert migration version");
    }
}

/// Insert a recent dead-letter row on `url`.
async fn seed_dead_letter(url: &str) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    diesel::sql_query(
        "INSERT INTO harvest_dead_letters \
             (id, original_task_id, queue_name, task_type, input, error, attempts, failed_at) \
         VALUES (gen_random_uuid(), gen_random_uuid(), 'default', 'activity', \
                 '{}'::jsonb, 'boom', 3, NOW())",
    )
    .execute(&mut conn)
    .await
    .expect("seed dead letter");
}

fn subsystem<'a>(body: &'a Value, name: &str) -> &'a Value {
    body["subsystems"]
        .as_array()
        .expect("subsystems array")
        .iter()
        .find(|s| s["name"] == name)
        .unwrap_or_else(|| panic!("{name} subsystem present"))
}

// ── (a) healthy path ─────────────────────────────────────────────────────────

#[tokio::test]
async fn healthy_fleet_reports_healthy_with_five_subsystems() {
    let (url, _c) = setup_single_shard().await;
    // A fully-migrated schema (ledger populated) with an actively-progressing
    // run (fresh event ⇒ not stalled) is the only genuinely healthy shape.
    populate_migration_ledger(&url).await;
    let exec = seed_execution(&url, 0, "onboarding", "RUNNING").await;
    seed_recent_event(&url, exec).await;
    let app = single_app(&url);

    let (status, body) = get_json(&app, "/admin/status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "healthy");
    let names: Vec<&str> = body["subsystems"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    for expected in [
        "workers",
        "shards",
        "dead_letters",
        "queues",
        "stalled_workflows",
    ] {
        assert!(names.contains(&expected), "{expected} subsystem present");
    }
    assert!(body["as_of"].is_string());
    assert!(body["unavailable_shards"].as_array().unwrap().is_empty());
    // Every healthy subsystem has a null drill_down.
    assert!(
        body["subsystems"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["drill_down"].is_null())
    );
}

// ── (b) dead-letters drive the verdict non-healthy ──────────────────────────

#[tokio::test]
async fn seeded_dead_letters_make_dead_letters_subsystem_non_healthy() {
    let (url, _c) = setup_single_shard().await;
    seed_dead_letter(&url).await;
    let app = single_app(&url);

    let (status, body) = get_json(&app, "/admin/status").await;
    assert_eq!(status, StatusCode::OK);

    let dl = subsystem(&body, "dead_letters");
    assert_ne!(dl["status"], "healthy", "a dead-letter must not be healthy");
    assert_eq!(dl["drill_down"], "/dead-letters/aggregate");
    assert!(dl["total"].as_i64().unwrap() >= 1);
    // A fresh dead-letter is an active failure ⇒ critical, which dominates the
    // overall verdict.
    assert_eq!(dl["status"], "critical");
    assert_eq!(body["status"], "critical");
}

// ── (c) admin guard ──────────────────────────────────────────────────────────

#[tokio::test]
async fn admin_status_requires_admin() {
    let (url, _c) = setup_single_shard().await;
    let app = build_unauthenticated_app(HarvestDbPool::from(build_pool(&url)));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/admin/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// ── (d) one shard down ⇒ degraded, not 500 ──────────────────────────────────

#[tokio::test]
async fn one_shard_down_degrades_and_names_the_shard() {
    let ((url0, url1), _c) = setup_two_shards().await;
    seed_execution(&url0, 0, "onboarding", "RUNNING").await;

    // Point shard 1 at a database that does not exist so its pool fails.
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url0));
    pools.insert(
        ShardId::new(1),
        build_pool(&url1.replace("harvest_shard_", "missing_db_")),
    );
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)));
    let app = build_app(storage);

    let (status, body) = get_json(&app, "/admin/status").await;
    assert_eq!(status, StatusCode::OK, "a down shard must not 500");
    assert_ne!(
        body["status"], "healthy",
        "a down shard degrades the verdict"
    );
    let unavailable = body["unavailable_shards"].as_array().unwrap();
    assert!(
        unavailable.iter().any(|u| u["shard_id"] == 1),
        "the down shard is named in unavailable_shards"
    );
    assert!(
        !unavailable.iter().find(|u| u["shard_id"] == 1).unwrap()["reason"]
            .as_str()
            .unwrap()
            .is_empty()
    );
}
