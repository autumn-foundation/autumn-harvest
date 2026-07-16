//! Local-Postgres end-to-end evidence for `GET /admin/status` (issue #679).
//!
//! The primary coverage for this route lives in `status_summary_integration.rs`,
//! which spins a real Postgres via testcontainers. In sandboxes that have a
//! running Postgres but **no Docker daemon**, testcontainers can't start a
//! container — so this file drives the identical app router (built with
//! `harvest_api_router` over `HarvestApiState`) against an operator-supplied
//! `DATABASE_URL` instead, exercising the real HTTP route through the axum stack
//! (not just the pure classifiers). It mirrors the #597 M9 precedent of
//! verifying routes end-to-end against a local Postgres by applying migrations
//! directly.
//!
//! **Opt-in / CI-safe**: the test is a no-op unless `DATABASE_URL` is set, so it
//! never runs (and never fails) in CI without an explicitly provided database.
//! Run it locally with e.g.:
//!
//! ```sh
//! DATABASE_URL='postgres://harvest:harvest@localhost:5432/harvest_test' \
//!   cargo test -p autumn-harvest-plugin --test status_summary_localpg -- --nocapture
//! ```
//!
//! All phases run in one `#[tokio::test]` sequentially, resetting the schema
//! between phases, so the result is deterministic regardless of `--test-threads`.
//!
//! **Circuit-breaker-tracked backlog (PR #988 review, issue #679):** the
//! per-shard queue-backlog scan now threads the runtime's circuit-breaker
//! tracked-activity names into `claimable_pending_demand_by_queue` (so a
//! breaker-tracked activity's rows count as claimable even with an empty
//! rate-limit bucket — matching `claim_task` and the shard-health coverage
//! gate). That non-empty-names path is intentionally **not** exercised here: this
//! harness builds `HarvestApiState` with `install_storage_pool` only and never
//! calls `install(runtime)`, so `api_state.runtime()` is `Err` and the resolved
//! tracked-names set is always empty (the safe under-count-free fallback the plumbing
//! degrades to at early boot). The non-empty branch is the byte-identical mirror
//! of `shard_health.rs`'s already-tested coverage-gate call, so its correctness
//! is covered there and by the core-crate tests for
//! `queue::claimable_pending_demand_by_queue`; the compiler verifies the
//! parameter threading.

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
use diesel_async::SimpleAsyncConnection;
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

type HarvestApiApp = axum::Router;

/// Drop and recreate the `public` schema, then apply the full migration set —
/// giving each phase a clean, deterministic slate on the shared local database.
///
/// Also creates and populates `__diesel_schema_migrations` from the on-disk
/// migration directories so the shard-health readiness probe (which reads that
/// table, `shard_health.rs`) sees a fully-migrated schema — exactly what a real
/// `diesel migration run` deployment produces, and what raw `up.sql` application
/// alone does not.
async fn reset_and_migrate(url: &str) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect for reset");
    conn.batch_execute("DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;")
        .await
        .expect("reset schema");
    conn.batch_execute(autumn_harvest::full_migrations_sql())
        .await
        .expect("apply migrations");
    populate_migration_ledger(&mut conn).await;
}

/// Record every migration's version in `__diesel_schema_migrations`, mirroring
/// what `diesel migration run` does. Versions are the numeric prefix of each
/// directory under `autumn-harvest/migrations`, resolved at compile time via
/// `CARGO_MANIFEST_DIR` so it is independent of the test's runtime CWD.
async fn populate_migration_ledger(conn: &mut AsyncPgConnection) {
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

    conn.batch_execute(
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
        .execute(conn)
        .await
        .expect("insert migration version");
    }
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

fn build_unauthenticated_app(storage: HarvestDbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(storage);
    harvest_api_router(api_state).with_state(AppState::for_test())
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

async fn seed_running_execution(url: &str, shard: i32) -> Uuid {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(shard));
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    let wf_id = format!("onboarding-{}", Uuid::new_v4().simple());
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: "onboarding",
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
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(autumn_harvest::schema::harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert running execution");
    diesel::update(dsl::harvest_workflow_executions.filter(dsl::id.eq(exec_id.as_uuid())))
        .set(dsl::state.eq("RUNNING"))
        .execute(&mut conn)
        .await
        .expect("force RUNNING");
    exec_id.as_uuid()
}

/// Insert a fresh (`NOW()`) `harvest_events` row for `exec_id` so the execution
/// is not counted as stalled (the stalled predicate is: active state with no
/// `harvest_events` row newer than the no-progress window). This is the piece
/// that distinguishes a *healthy, progressing* run from a stalled one.
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

/// Insert a future-dated, unfired durable timer for `exec_id` — a workflow that
/// is legitimately parked on a long `ctx.timer`/`receive_signal_timeout` and is
/// therefore "correctly sleeping", NOT stalled (issue #679 review, FIX 1).
async fn seed_future_timer(url: &str, exec_id: Uuid) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect");
    diesel::sql_query(
        "INSERT INTO harvest_timers (id, workflow_exec_id, timer_id, fires_at, fired) \
         VALUES (gen_random_uuid(), $1, 'sleep', NOW() + INTERVAL '1 hour', false)",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id)
    .execute(&mut conn)
    .await
    .expect("seed future timer");
}

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

/// Drives the real `GET /admin/status` route against a local Postgres for all
/// four issue-#679 scenarios. No-op unless `DATABASE_URL` is set.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn admin_status_localpg_end_to_end() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("SKIP status_summary_localpg: DATABASE_URL not set");
        return;
    };
    eprintln!("status_summary_localpg: using DATABASE_URL={url}");

    // ── (a) healthy path: a migrated schema with an actively-progressing run ─
    reset_and_migrate(&url).await;
    let exec = seed_running_execution(&url, 0).await;
    seed_recent_event(&url, exec).await; // fresh event ⇒ not stalled
    let app = build_app(HarvestDbPool::from(build_pool(&url)));
    let (status, body) = get_json(&app, "/admin/status").await;
    assert_eq!(status, StatusCode::OK, "healthy path returns 200");
    assert_eq!(
        body["status"], "healthy",
        "a fully-migrated schema with a progressing run is healthy"
    );
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
    assert_eq!(names.len(), 5, "exactly five subsystems");
    assert!(body["as_of"].is_string(), "as_of timestamp present");
    assert!(
        body["unavailable_shards"].as_array().unwrap().is_empty(),
        "no unavailable shards on the healthy path"
    );
    assert!(
        body["subsystems"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["drill_down"].is_null()),
        "every healthy subsystem has a null drill_down"
    );
    eprintln!(
        "PASS (a) healthy: {}",
        serde_json::to_string(&body).unwrap()
    );

    // ── (b) dead-letters drive the verdict non-healthy ──────────────────────
    reset_and_migrate(&url).await;
    seed_dead_letter(&url).await;
    let app = build_app(HarvestDbPool::from(build_pool(&url)));
    let (status, body) = get_json(&app, "/admin/status").await;
    assert_eq!(status, StatusCode::OK);
    let dl = subsystem(&body, "dead_letters");
    assert_ne!(dl["status"], "healthy", "a dead-letter must not be healthy");
    assert_eq!(
        dl["drill_down"], "/dead-letters/aggregate",
        "drill-down link"
    );
    assert!(dl["total"].as_i64().unwrap() >= 1, "dead-letter counted");
    assert_eq!(dl["status"], "critical", "a fresh dead-letter is critical");
    assert_eq!(body["status"], "critical", "overall = worst subsystem");
    eprintln!(
        "PASS (b) dead_letters: overall={}, dead_letters={}",
        body["status"], dl["status"]
    );

    // ── (c) admin guard rejects an unauthenticated request ──────────────────
    reset_and_migrate(&url).await;
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
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "admin guard rejects an unauthenticated caller"
    );
    eprintln!("PASS (c) admin guard: {} UNAUTHORIZED", response.status());

    // ── (d) one shard down ⇒ degraded, not 500, shard named ─────────────────
    reset_and_migrate(&url).await;
    seed_running_execution(&url, 0).await;
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url));
    // Point shard 1 at a database that does not exist so its pool fails.
    let broken_url = format!("{url}_missing_db_does_not_exist");
    pools.insert(ShardId::new(1), build_pool(&broken_url));
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
        "the down shard is named in unavailable_shards: {unavailable:?}"
    );
    let down = unavailable
        .iter()
        .find(|u| u["shard_id"] == 1)
        .expect("shard 1 entry");
    assert!(
        !down["reason"].as_str().unwrap().is_empty(),
        "the down shard carries a non-empty reason"
    );
    eprintln!(
        "PASS (d) shard down: overall={}, unavailable_shards={}",
        body["status"],
        serde_json::to_string(unavailable).unwrap()
    );

    // ── (e) a workflow correctly sleeping on a future timer is NOT stalled ───
    // (issue #679 review, FIX 1) The exec has no recent event (so it passes the
    // event-age stalled predicate) but its sole pending work is a future-dated
    // timer, so the future-timer exclusion — copied verbatim from
    // `load_stalled_workflows` — must keep it out of the stalled count.
    reset_and_migrate(&url).await; // also populates the migration ledger
    let sleeper = seed_running_execution(&url, 0).await;
    seed_future_timer(&url, sleeper).await; // sole pending work is a future timer
    let app = build_app(HarvestDbPool::from(build_pool(&url)));
    let (status, body) = get_json(&app, "/admin/status").await;
    assert_eq!(status, StatusCode::OK);
    let stalled = subsystem(&body, "stalled_workflows");
    assert_eq!(
        stalled["count"].as_i64().unwrap(),
        0,
        "a workflow sleeping on a future timer must not be counted as stalled: {stalled}"
    );
    assert_eq!(
        stalled["status"], "healthy",
        "stalled_workflows subsystem is healthy when the only 'stall' is a sleeper"
    );
    eprintln!(
        "PASS (e) sleeper-not-stalled: stalled_count={}",
        stalled["count"]
    );

    eprintln!("ALL PHASES PASSED against local Postgres");
}
