//! Integration tests for the batch operations API (issue #102).
//!
//! Covers the durable wiring end to end: submit → list → get → executor
//! drains, mid-batch crash resume, Terminate-on-non-running, and the issue's
//! 1k-workflow / 30s success metric.

#![allow(clippy::similar_names, clippy::redundant_clone)]

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::batch::{BatchExecutorConfig, run_executor_once};
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
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
    include_str!("../../autumn-harvest/migrations/20260501010000_harvest_batch_jobs/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260501020000_harvest_batch_processed_ids/up.sql"
    ),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
);

type HarvestApiApp = axum::Router;

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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

fn test_app_state() -> AppState {
    AppState::for_test().with_profile("test")
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("batch-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(test_app_state())
}

async fn post_json(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response is JSON")
    };
    (status, json)
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response is JSON")
    };
    (status, json)
}

async fn seed_workflows(database_url: &str, workflow_name: &str, count: usize) -> Vec<ExecutionId> {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for seed");
    let mut ids = Vec::with_capacity(count);
    for index in 0..count {
        let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        start_or_load_workflow_execution(
            &mut conn,
            StartWorkflowParams {
                workflow_name,
                workflow_id: &format!("wf-{index}"),
                exec_id,
                input: json!({"i": index}),
                parent_id: None,
                queue_name: "default",
                execution_timeout: None,
                memo: None,
                search_attrs: Some(json!({"tenant": "acme"})),
                reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
                trace_context: None,
            },
        )
        .await
        .expect("seed workflow");
        ids.push(exec_id);
    }
    ids
}

#[tokio::test]
async fn batch_cancel_drains_filtered_workflows() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    seed_workflows(&url, "onboarding", 50).await;

    // Submit a batch-cancel job filtered to onboarding workflows.
    let (status, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Cancel",
            "filter": { "workflow_name": "onboarding", "states": ["RUNNING"] }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "submit response: {body}");
    let job_id = body["batch_job_id"]
        .as_str()
        .expect("batch_job_id present")
        .to_string();

    // Initially Pending, no progress yet.
    let (status, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "Pending");
    assert_eq!(body["completed"], 0);
    assert_eq!(body["failed"], 0);

    // Drive the executor synchronously.
    let sharded_pool = HarvestDbPool::from(pool.clone());
    run_executor_once(sharded_pool.sharded_pool(), &BatchExecutorConfig::default())
        .await
        .expect("executor tick succeeds");

    // Job has reached terminal Completed and counters reflect the cancelled fleet.
    let (status, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "Completed", "after executor tick: {body}");
    assert_eq!(body["total"], 50);
    assert_eq!(body["completed"], 50);
    assert_eq!(body["failed"], 0);

    // Every workflow in the fleet is now CANCELLED.
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .unwrap();
    let states: Vec<String> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq("onboarding"))
        .select(harvest_workflow_executions::state)
        .load(&mut conn)
        .await
        .unwrap();
    assert_eq!(states.len(), 50);
    assert!(
        states.iter().all(|s| s == "CANCELLED"),
        "expected all CANCELLED, got {states:?}"
    );
}

#[tokio::test]
async fn batch_submission_is_idempotent_under_retry() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let payload = json!({
        "action": "Cancel",
        "filter": { "workflow_name": "billing" },
        "idempotency_key": "incident-2026-04-28-billing-rollback"
    });

    let (s1, b1) = post_json(&app, "/batch-operations", payload.clone()).await;
    let (s2, b2) = post_json(&app, "/batch-operations", payload).await;

    assert_eq!(s1, StatusCode::ACCEPTED);
    assert_eq!(s2, StatusCode::ACCEPTED);
    assert_eq!(
        b1["batch_job_id"], b2["batch_job_id"],
        "duplicate submission must return same job id"
    );
}

#[tokio::test]
async fn batch_signal_action_requires_signal_name() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Signal",
            "filter": { "workflow_name": "onboarding" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("signal_name"),
        "expected signal_name validation error, got {body}"
    );
}

#[tokio::test]
async fn batch_filter_must_have_at_least_one_criterion() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let (status, body) = post_json(
        &app,
        "/batch-operations",
        json!({ "action": "Cancel", "filter": {} }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("filter"),
        "expected filter validation error, got {body}"
    );
}

#[tokio::test]
async fn batch_list_filters_by_status_and_action() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    // Two distinct submissions so list has something to slice.
    let (_, _) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Cancel",
            "filter": { "workflow_name": "onboarding" },
            "idempotency_key": "k-cancel"
        }),
    )
    .await;
    let (_, _) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Signal",
            "filter": { "workflow_name": "onboarding" },
            "signal_name": "rollback",
            "idempotency_key": "k-signal"
        }),
    )
    .await;

    let (status, body) = get_json(&app, "/batch-operations").await;
    assert_eq!(status, StatusCode::OK);
    let arr = body.as_array().expect("list response is array");
    assert_eq!(arr.len(), 2);

    let (_, body) = get_json(&app, "/batch-operations?action=Signal").await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["action"], "Signal");

    let (_, body) = get_json(&app, "/batch-operations?status=Pending").await;
    assert_eq!(body.as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn batch_per_target_failures_dont_abort_run() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let ids = seed_workflows(&url, "onboarding", 4).await;
    // Pre-cancel one workflow so the cancel path returns idempotent on it.
    // Then mark another COMPLETED so the cancel path *errors* on it. The
    // batch should still drain the remaining two and report failed=1.
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .unwrap();
    diesel::update(harvest_workflow_executions::table.find(ids[0].as_uuid()))
        .set(harvest_workflow_executions::state.eq("CANCELLED"))
        .execute(&mut conn)
        .await
        .unwrap();
    diesel::update(harvest_workflow_executions::table.find(ids[1].as_uuid()))
        .set(harvest_workflow_executions::state.eq("COMPLETED"))
        .execute(&mut conn)
        .await
        .unwrap();

    let (_, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Cancel",
            "filter": {
                "workflow_name": "onboarding",
                "states": ["RUNNING", "CANCELLED", "COMPLETED"]
            }
        }),
    )
    .await;
    let job_id = body["batch_job_id"].as_str().unwrap().to_string();

    let sharded_pool = HarvestDbPool::from(pool);
    run_executor_once(sharded_pool.sharded_pool(), &BatchExecutorConfig::default())
        .await
        .unwrap();

    let (_, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(body["status"], "Completed");
    assert_eq!(body["total"], 4);
    assert_eq!(
        body["failed"], 1,
        "completed workflow must surface as failed: {body}"
    );
    assert_eq!(body["completed"], 3);
    let errors = body["errors"].as_array().expect("errors array");
    assert_eq!(errors.len(), 1);
}

// Issue #102 success metric: 1k-workflow batch-cancel drains within 30 seconds
// at default concurrency on a laptop-class Postgres. The 60s outer timeout is
// the CI escape hatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_drains_thousand_workflows_within_time_budget() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    seed_workflows(&url, "onboarding", 1000).await;
    let (_, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Cancel",
            "filter": { "workflow_name": "onboarding" }
        }),
    )
    .await;
    let job_id = body["batch_job_id"].as_str().unwrap().to_string();

    let sharded_pool = HarvestDbPool::from(pool);
    let started = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(60),
        run_executor_once(sharded_pool.sharded_pool(), &BatchExecutorConfig::default()),
    )
    .await
    .expect("executor must finish under 60s")
    .expect("executor result");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "1k-workflow batch should drain in under 30s, took {elapsed:?}"
    );

    let (_, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(body["status"], "Completed");
    assert_eq!(body["total"], 1000);
    assert_eq!(body["completed"], 1000);
    assert_eq!(body["failed"], 0);
}

// Issue #102: "A job survives plugin restart and resumes from
// `completed + failed` cursor."
//
// Direct-process-kill is awkward in a single test process, so this simulates
// the post-crash state: the executor wrote `Running, completed=K` against the
// row and cancelled K of the matched workflows before the host went away. A
// fresh executor tick should pick up the remaining N-K and end the job at
// `total=N, completed=N, failed=0`.
#[tokio::test]
async fn batch_resumes_from_partial_progress_cursor() {
    use autumn_harvest::cancel_workflow_execution;

    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let ids = seed_workflows(&url, "onboarding", 20).await;

    // Submit the batch through the API so the row is shaped exactly as the
    // production path produces it.
    let (_, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Cancel",
            "filter": { "workflow_name": "onboarding" }
        }),
    )
    .await;
    let job_id = body["batch_job_id"].as_str().unwrap().to_string();

    // Simulate a crash mid-tick: 8 of the 20 targets already cancelled and
    // the row already in Running with the partial counters in place.
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .unwrap();
    for exec_id in ids.iter().take(8).copied() {
        cancel_workflow_execution(&mut conn, exec_id, "simulated mid-batch progress")
            .await
            .unwrap();
    }
    let job_uuid: uuid::Uuid = job_id.parse().unwrap();
    // Record the 8 already-cancelled exec ids in `processed_ids` so the next
    // tick excludes them by identity (not by counter offset). Backdate
    // updated_at past the lease window so the surviving worker can claim the
    // abandoned row (simulating a worker that crashed long ago).
    let processed_json = serde_json::to_value(
        ids.iter()
            .take(8)
            .map(|id| id.as_uuid().to_string())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    diesel::sql_query(
        "UPDATE harvest_batch_jobs \
         SET status='Running', total=20, completed=8, \
             processed_ids = $2, \
             updated_at = now() - INTERVAL '5 minutes' \
         WHERE id=$1",
    )
    .bind::<diesel::sql_types::Uuid, _>(job_uuid)
    .bind::<diesel::sql_types::Jsonb, _>(processed_json)
    .execute(&mut conn)
    .await
    .unwrap();

    // Fresh tick from the surviving plugin process.
    let sharded_pool = HarvestDbPool::from(pool.clone());
    run_executor_once(sharded_pool.sharded_pool(), &BatchExecutorConfig::default())
        .await
        .unwrap();

    // Job ends terminal with the full count, not double-counted.
    let (_, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(body["status"], "Completed", "after resume tick: {body}");
    assert_eq!(body["total"], 20);
    assert_eq!(body["completed"], 20);
    assert_eq!(body["failed"], 0);

    // And every workflow ended up CANCELLED.
    let states: Vec<String> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq("onboarding"))
        .select(harvest_workflow_executions::state)
        .load(&mut conn)
        .await
        .unwrap();
    assert!(
        states.iter().all(|s| s == "CANCELLED"),
        "every workflow must be CANCELLED after resume, got {states:?}"
    );
}

// Issue #102 distinguishes Cancel from Terminate: Terminate is a hard
// finalize that operates on workflows Cancel rejects (FAILED, TIMED_OUT, ...).
#[tokio::test]
async fn batch_terminate_finalizes_non_running_workflows() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let ids = seed_workflows(&url, "onboarding", 4).await;

    // Two workflows in non-RUNNING states that Cancel cannot touch.
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .unwrap();
    diesel::update(harvest_workflow_executions::table.find(ids[1].as_uuid()))
        .set(harvest_workflow_executions::state.eq("FAILED"))
        .execute(&mut conn)
        .await
        .unwrap();
    diesel::update(harvest_workflow_executions::table.find(ids[2].as_uuid()))
        .set(harvest_workflow_executions::state.eq("TIMED_OUT"))
        .execute(&mut conn)
        .await
        .unwrap();

    // Terminate without an explicit state filter — the executor's default for
    // Terminate widens to "every non-CANCELLED state".
    let (_, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Terminate",
            "filter": { "workflow_name": "onboarding" }
        }),
    )
    .await;
    let job_id = body["batch_job_id"].as_str().unwrap().to_string();

    let sharded_pool = HarvestDbPool::from(pool);
    run_executor_once(sharded_pool.sharded_pool(), &BatchExecutorConfig::default())
        .await
        .unwrap();

    let (_, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(body["status"], "Completed");
    assert_eq!(body["total"], 4);
    // All four — including FAILED and TIMED_OUT — must be force-finalized.
    assert_eq!(
        body["completed"], 4,
        "Terminate must accept any non-cancelled state: {body}"
    );
    assert_eq!(body["failed"], 0);

    let states: Vec<String> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq("onboarding"))
        .select(harvest_workflow_executions::state)
        .load(&mut conn)
        .await
        .unwrap();
    assert!(
        states.iter().all(|s| s == "CANCELLED"),
        "Terminate must coerce every match to CANCELLED, got {states:?}"
    );
}

// Issue #102 success metric: "p99 task-queue claim latency for unrelated
// workflows must not regress by more than 10% during a 10k-target batch run."
// This is a scaled-down smoke test: while a 200-target batch tick is in
// flight, an unrelated GET /workflows must complete within a tight bound.
// Detects accidental table-locking or pool-exhaustion regressions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_does_not_block_unrelated_workflow_reads() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    seed_workflows(&url, "onboarding", 200).await;
    seed_workflows(&url, "billing", 5).await; // distinct, not in batch filter

    let (_, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Cancel",
            "filter": { "workflow_name": "onboarding" }
        }),
    )
    .await;
    let _job_id = body["batch_job_id"].as_str().unwrap().to_string();

    let sharded_pool = HarvestDbPool::from(pool);
    let executor_pool = sharded_pool.clone();
    let executor = tokio::spawn(async move {
        run_executor_once(
            executor_pool.sharded_pool(),
            &BatchExecutorConfig::default(),
        )
        .await
    });

    // Probe an unrelated read while the batch is draining. The probe must
    // complete in well under 5s — the executor holds no long-lived locks
    // and uses bounded fan-out, so per-call latency should be DB-roundtrip
    // bounded.
    let probe_started = std::time::Instant::now();
    let (status, body) = get_json(&app, "/workflows?workflow_name=billing").await;
    let probe_elapsed = probe_started.elapsed();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.as_array().expect("array").len(),
        5,
        "unrelated billing workflows must remain visible during a batch run"
    );
    assert!(
        probe_elapsed < Duration::from_secs(5),
        "unrelated GET /workflows must not be blocked by the batch executor; took {probe_elapsed:?}"
    );

    executor.await.unwrap().unwrap();
}

// Two executor ticks running in parallel must not both claim the same job.
// The lease-based claim guarantees exactly-once dispatch per target across
// concurrent worker processes (issue #102, P1 review feedback).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_executor_lease_prevents_double_dispatch() {
    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let _ids = seed_workflows(&url, "onboarding", 30).await;

    let (_, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Cancel",
            "filter": { "workflow_name": "onboarding" }
        }),
    )
    .await;
    let job_id = body["batch_job_id"].as_str().unwrap().to_string();

    // Race two executor ticks on the same shard pool. With the atomic lease
    // claim, exactly one tick processes the job; the other no-ops silently.
    let sharded_pool = HarvestDbPool::from(pool.clone());
    let pool_a = sharded_pool.sharded_pool().clone();
    let pool_b = sharded_pool.sharded_pool().clone();
    let tick_a =
        tokio::spawn(
            async move { run_executor_once(&pool_a, &BatchExecutorConfig::default()).await },
        );
    let tick_b =
        tokio::spawn(
            async move { run_executor_once(&pool_b, &BatchExecutorConfig::default()).await },
        );
    tick_a.await.unwrap().unwrap();
    tick_b.await.unwrap().unwrap();

    // The job ends terminal with completed == total (no double-counting).
    let (_, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(body["status"], "Completed", "after racing ticks: {body}");
    assert_eq!(body["total"], 30);
    assert_eq!(
        body["completed"], 30,
        "completed must equal total; double-dispatch would inflate this"
    );
    assert_eq!(body["failed"], 0);
}

// Issue #102 Signal-resume correctness: Signal targets stay RUNNING after
// dispatch, so the resume cursor cannot be a counter offset over a re-queried
// list. If workflows that were already signaled naturally complete during the
// recovery window, the new RUNNING set shrinks and an offset cursor would
// silently skip workflows that were never signaled. This test seeds that
// exact race: it backs the executor row with `processed_ids = first 4 UUIDs`
// (those were "signaled and later naturally completed"), then deletes those 4
// rows from the workflow table (so they no longer match the RUNNING filter),
// and confirms the resume tick dispatches the remaining 6 by identity.
#[tokio::test]
async fn batch_signal_resume_excludes_processed_ids_not_offset() {
    use autumn_harvest::schema::harvest_signals;

    let (url, _container) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool);

    let ids = seed_workflows(&url, "onboarding", 10).await;

    let (_, body) = post_json(
        &app,
        "/batch-operations",
        json!({
            "action": "Signal",
            "filter": { "workflow_name": "onboarding" },
            "signal_name": "switch_to_fallback"
        }),
    )
    .await;
    let job_id = body["batch_job_id"].as_str().unwrap().to_string();
    let job_uuid: uuid::Uuid = job_id.parse().unwrap();

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .unwrap();

    // Simulate "signaled before crash, naturally completed during recovery":
    // mark the first 4 rows COMPLETED so they leave the RUNNING filter.
    for exec_id in ids.iter().take(4) {
        diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
            .set(harvest_workflow_executions::state.eq("COMPLETED"))
            .execute(&mut conn)
            .await
            .unwrap();
    }

    // Persist those 4 ids as already-processed and bump completed=4.
    let processed_json = serde_json::to_value(
        ids.iter()
            .take(4)
            .map(|id| id.as_uuid().to_string())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    diesel::sql_query(
        "UPDATE harvest_batch_jobs \
         SET status='Running', total=10, completed=4, \
             processed_ids = $2, \
             updated_at = now() - INTERVAL '5 minutes' \
         WHERE id=$1",
    )
    .bind::<diesel::sql_types::Uuid, _>(job_uuid)
    .bind::<diesel::sql_types::Jsonb, _>(processed_json)
    .execute(&mut conn)
    .await
    .unwrap();

    let sharded_pool = HarvestDbPool::from(pool.clone());
    run_executor_once(sharded_pool.sharded_pool(), &BatchExecutorConfig::default())
        .await
        .unwrap();

    // The remaining 6 RUNNING workflows must each have received a signal,
    // and total/completed must reflect identity-based exclusion: 4 from the
    // prior tick + 6 dispatched here = 10. An offset cursor would have
    // skipped 4 workflows from the now-shrunken list of 6 and dispatched 2.
    let (_, body) = get_json(&app, &format!("/batch-operations/{job_id}")).await;
    assert_eq!(body["status"], "Completed", "after resume tick: {body}");
    assert_eq!(body["total"], 10);
    assert_eq!(
        body["completed"], 10,
        "completed must equal total; signal-resume by identity"
    );

    // Verify each of the 6 surviving workflows actually received a signal.
    for exec_id in ids.iter().skip(4) {
        let signal_count: i64 = harvest_signals::table
            .filter(harvest_signals::workflow_exec_id.eq(exec_id.as_uuid()))
            .count()
            .get_result(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            signal_count, 1,
            "exec {exec_id} must have exactly one signal queued"
        );
    }
}
